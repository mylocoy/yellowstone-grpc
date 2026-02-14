use {
    anyhow::{bail, Context},
    crc32fast::Hasher as Crc32Hasher,
    memmap2::{Mmap, MmapOptions},
    std::{
        fs::OpenOptions,
        mem::align_of,
        path::Path,
        sync::atomic::{AtomicU64, Ordering},
    },
};

pub const DEFAULT_POSIX_SHM_NAME: &str = "/yellowstone_accounts";

// ── Ring buffer wire format constants ──────────────────────────────────────
// IMPORTANT: These values must match the writer side in
// yellowstone-grpc-geyser/src/shm.rs. Any change there MUST be mirrored here.
// ───────────────────────────────────────────────────────────────────────────
const HEADER_BYTES: usize = 4096;
const HEADER_MAGIC: [u8; 8] = *b"YGRING01";
const HEADER_VERSION: u32 = 1;

const OFFSET_MAGIC: usize = 0;
const OFFSET_VERSION: usize = 8;
const OFFSET_HEADER_BYTES: usize = 12;
const OFFSET_CAPACITY: usize = 16;
const OFFSET_WRITE_POS: usize = 24;
const OFFSET_TAIL_POS: usize = 32;
const OFFSET_DROPPED_RECORDS: usize = 40;
const OFFSET_WRITTEN_RECORDS: usize = 48;

pub const ACCOUNT_FRAME_KIND: u16 = 1;
/// Fixed-size portion of an account frame (before variable-length `data`):
///   kind(2) + flags(2) + sequence(8) + nanos(8) + slot(8) + write_version(8)
///   + lamports(8) + rent_epoch(8) + pubkey(32) + owner(32) + txn_sig(64) + data_len(4)
const ACCOUNT_FRAME_FIXED_BYTES: usize = 2 + 2 + 8 + 8 + 8 + 8 + 8 + 8 + 32 + 32 + 64 + 4;
const _: () = assert!(ACCOUNT_FRAME_FIXED_BYTES == 184);

const FLAG_IS_STARTUP: u16 = 1 << 0;
const FLAG_EXECUTABLE: u16 = 1 << 1;
const FLAG_HAS_TXN_SIGNATURE: u16 = 1 << 2;

#[derive(Debug, Clone, Copy)]
pub struct ReaderStats {
    pub write_pos: u64,
    pub tail_pos: u64,
    pub dropped_records: u64,
    pub written_records: u64,
    pub skipped_records: u64,
}

#[derive(Debug, Clone)]
pub struct AccountFrame {
    pub sequence: u64,
    pub created_at_unix_nanos: u64,
    pub slot: u64,
    pub write_version: u64,
    pub lamports: u64,
    pub rent_epoch: u64,
    pub pubkey: [u8; 32],
    pub owner: [u8; 32],
    pub executable: bool,
    pub is_startup: bool,
    pub txn_signature: Option<[u8; 64]>,
    pub data: Vec<u8>,
}

pub struct SharedRingReader {
    mmap: Mmap,
    capacity: usize,
    cursor: u64,
    skipped_records: u64,
}

impl SharedRingReader {
    pub fn open(path: impl AsRef<Path>, from_latest: bool) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .with_context(|| format!("failed to open ring file {path:?}"))?;
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)
                .with_context(|| format!("failed to mmap ring file {path:?}"))?
        };

        let capacity =
            verify_header(&mmap).with_context(|| format!("invalid ring header in {path:?}"))?;
        let cursor = if from_latest {
            atomic_u64(mmap.as_ref(), OFFSET_WRITE_POS).load(Ordering::Acquire)
        } else {
            atomic_u64(mmap.as_ref(), OFFSET_TAIL_POS).load(Ordering::Acquire)
        };

        Ok(Self {
            mmap,
            capacity,
            cursor,
            skipped_records: 0,
        })
    }

    pub fn next_payload(&mut self) -> anyhow::Result<Option<Vec<u8>>> {
        // Cross-process memory fence: ensure all prior writes from the writer
        // process are visible to this reader before we inspect positions.
        // On x86_64 (TSO) this is essentially a no-op, but on ARM (aarch64)
        // it emits the necessary barrier instruction.
        std::sync::atomic::fence(Ordering::Acquire);

        let tail_pos = self.load_tail_pos();
        if self.cursor < tail_pos {
            self.cursor = tail_pos;
            self.skipped_records += 1;
        }

        let write_pos = self.load_write_pos();
        if self.cursor >= write_pos {
            return Ok(None);
        }

        let payload_len = self.read_u32_at(self.cursor)? as usize;
        let record_len = payload_len.saturating_add(std::mem::size_of::<u32>());
        if payload_len == 0 || record_len > self.capacity {
            self.cursor = self.load_tail_pos();
            self.skipped_records += 1;
            return Ok(None);
        }

        let record_end = self.cursor + record_len as u64;
        if record_end > write_pos {
            return Ok(None);
        }

        let mut payload = vec![0u8; payload_len];
        copy_from_ring(
            self.mmap.as_ref(),
            self.capacity,
            self.cursor + std::mem::size_of::<u32>() as u64,
            &mut payload,
        )?;
        self.cursor = record_end;
        Ok(Some(payload))
    }

    pub fn next_account_frame(&mut self) -> anyhow::Result<Option<AccountFrame>> {
        let Some(payload) = self.next_payload()? else {
            return Ok(None);
        };
        decode_account_frame(&payload).map(Some)
    }

    pub fn stats(&self) -> ReaderStats {
        ReaderStats {
            write_pos: self.load_write_pos(),
            tail_pos: self.load_tail_pos(),
            dropped_records: self.load_dropped_records(),
            written_records: self.load_written_records(),
            skipped_records: self.skipped_records,
        }
    }

    fn load_write_pos(&self) -> u64 {
        atomic_u64(self.mmap.as_ref(), OFFSET_WRITE_POS).load(Ordering::Acquire)
    }

    fn load_tail_pos(&self) -> u64 {
        atomic_u64(self.mmap.as_ref(), OFFSET_TAIL_POS).load(Ordering::Acquire)
    }

    fn load_dropped_records(&self) -> u64 {
        atomic_u64(self.mmap.as_ref(), OFFSET_DROPPED_RECORDS).load(Ordering::Acquire)
    }

    fn load_written_records(&self) -> u64 {
        atomic_u64(self.mmap.as_ref(), OFFSET_WRITTEN_RECORDS).load(Ordering::Acquire)
    }

    fn read_u32_at(&self, absolute_offset: u64) -> anyhow::Result<u32> {
        let mut bytes = [0u8; 4];
        copy_from_ring(
            self.mmap.as_ref(),
            self.capacity,
            absolute_offset,
            &mut bytes,
        )?;
        Ok(u32::from_le_bytes(bytes))
    }
}

/// CRC32 tail size in bytes (appended by the writer after the frame body).
const CRC32_TAIL_BYTES: usize = 4;

pub fn decode_account_frame(payload: &[u8]) -> anyhow::Result<AccountFrame> {
    // Minimum: fixed header + CRC32 tail (data may be zero-length).
    if payload.len() < ACCOUNT_FRAME_FIXED_BYTES + CRC32_TAIL_BYTES {
        bail!(
            "invalid account frame: len {} < minimum {}",
            payload.len(),
            ACCOUNT_FRAME_FIXED_BYTES + CRC32_TAIL_BYTES
        );
    }

    // Verify CRC32: the last 4 bytes are the checksum over everything before them.
    let (body, crc_bytes) = payload.split_at(payload.len() - CRC32_TAIL_BYTES);
    let stored_crc = u32::from_le_bytes(
        crc_bytes
            .try_into()
            .context("failed to read CRC32 from payload tail")?,
    );
    let mut hasher = Crc32Hasher::new();
    hasher.update(body);
    let computed_crc = hasher.finalize();
    if stored_crc != computed_crc {
        bail!(
            "CRC32 mismatch: stored={stored_crc:#010x} computed={computed_crc:#010x} (payload corrupt or partially written)"
        );
    }

    // Decode the body (everything except the trailing CRC32).
    let mut offset = 0usize;
    let kind = decode_u16(body, &mut offset)?;
    if kind != ACCOUNT_FRAME_KIND {
        bail!("unexpected frame kind: {kind}");
    }
    let flags = decode_u16(body, &mut offset)?;
    let sequence = decode_u64(body, &mut offset)?;
    let created_at_unix_nanos = decode_u64(body, &mut offset)?;
    let slot = decode_u64(body, &mut offset)?;
    let write_version = decode_u64(body, &mut offset)?;
    let lamports = decode_u64(body, &mut offset)?;
    let rent_epoch = decode_u64(body, &mut offset)?;
    let pubkey = decode_fixed::<32>(body, &mut offset)?;
    let owner = decode_fixed::<32>(body, &mut offset)?;
    let txn_signature_raw = decode_fixed::<64>(body, &mut offset)?;
    let data_len = decode_u32(body, &mut offset)? as usize;
    let expected_body_len = ACCOUNT_FRAME_FIXED_BYTES + data_len;
    if body.len() != expected_body_len {
        bail!(
            "invalid account frame size: actual={} expected={expected_body_len}",
            body.len()
        );
    }

    let data = body[offset..].to_vec();
    let txn_signature = if (flags & FLAG_HAS_TXN_SIGNATURE) == 0 {
        None
    } else {
        Some(txn_signature_raw)
    };

    Ok(AccountFrame {
        sequence,
        created_at_unix_nanos,
        slot,
        write_version,
        lamports,
        rent_epoch,
        pubkey,
        owner,
        executable: (flags & FLAG_EXECUTABLE) != 0,
        is_startup: (flags & FLAG_IS_STARTUP) != 0,
        txn_signature,
        data,
    })
}

/// Convert a POSIX SHM name to its filesystem path.
/// Only valid on Linux where `/dev/shm` is the backing tmpfs.
pub fn posix_shm_name_to_path(name: &str) -> anyhow::Result<String> {
    if name.is_empty() {
        bail!("shm name must not be empty");
    }
    if name.contains('\0') {
        bail!("shm name must not contain NUL");
    }
    let normalized = name.strip_prefix('/').unwrap_or(name);
    if normalized.is_empty() {
        bail!("shm name must include non-slash characters");
    }
    if cfg!(not(target_os = "linux")) {
        log::warn!("posix_shm_name_to_path assumes Linux /dev/shm; path may be incorrect on this platform");
    }
    Ok(format!("/dev/shm/{normalized}"))
}

fn decode_u16(payload: &[u8], offset: &mut usize) -> anyhow::Result<u16> {
    let end = offset.saturating_add(2);
    let value = payload
        .get(*offset..end)
        .context("failed to decode u16")?
        .try_into()
        .map(u16::from_le_bytes)
        .context("failed to decode u16 bytes")?;
    *offset = end;
    Ok(value)
}

fn decode_u32(payload: &[u8], offset: &mut usize) -> anyhow::Result<u32> {
    let end = offset.saturating_add(4);
    let value = payload
        .get(*offset..end)
        .context("failed to decode u32")?
        .try_into()
        .map(u32::from_le_bytes)
        .context("failed to decode u32 bytes")?;
    *offset = end;
    Ok(value)
}

fn decode_u64(payload: &[u8], offset: &mut usize) -> anyhow::Result<u64> {
    let end = offset.saturating_add(8);
    let value = payload
        .get(*offset..end)
        .context("failed to decode u64")?
        .try_into()
        .map(u64::from_le_bytes)
        .context("failed to decode u64 bytes")?;
    *offset = end;
    Ok(value)
}

fn decode_fixed<const N: usize>(payload: &[u8], offset: &mut usize) -> anyhow::Result<[u8; N]> {
    let end = offset.saturating_add(N);
    let value = payload
        .get(*offset..end)
        .with_context(|| format!("failed to decode fixed bytes of size {N}"))?
        .try_into()
        .with_context(|| format!("failed to decode fixed byte array of size {N}"))?;
    *offset = end;
    Ok(value)
}

fn verify_header(mmap: &[u8]) -> anyhow::Result<usize> {
    if mmap.len() < HEADER_BYTES {
        bail!("ring file is smaller than header");
    }

    let magic = mmap
        .get(OFFSET_MAGIC..OFFSET_MAGIC + HEADER_MAGIC.len())
        .context("failed to read ring magic")?;
    if magic != HEADER_MAGIC {
        bail!("ring magic mismatch");
    }

    let version = load_u32(mmap, OFFSET_VERSION)?;
    if version != HEADER_VERSION {
        bail!("ring version mismatch: {version} != {HEADER_VERSION}");
    }

    let header_bytes = load_u32(mmap, OFFSET_HEADER_BYTES)? as usize;
    if header_bytes != HEADER_BYTES {
        bail!("ring header size mismatch: {header_bytes} != {HEADER_BYTES}");
    }

    let capacity = load_u64(mmap, OFFSET_CAPACITY)? as usize;
    if mmap.len() != HEADER_BYTES + capacity {
        bail!(
            "ring file size mismatch: file={} header+capacity={}",
            mmap.len(),
            HEADER_BYTES + capacity
        );
    }

    Ok(capacity)
}

fn copy_from_ring(
    mmap: &[u8],
    capacity: usize,
    absolute_offset: u64,
    target: &mut [u8],
) -> anyhow::Result<()> {
    if target.is_empty() {
        return Ok(());
    }
    if target.len() > capacity {
        bail!(
            "cannot read {} bytes from ring with capacity {capacity}",
            target.len()
        );
    }
    if mmap.len() < HEADER_BYTES + capacity {
        bail!("ring mmap is smaller than expected");
    }

    let mut ring_offset = (absolute_offset as usize) % capacity;
    let mut copied = 0usize;
    while copied < target.len() {
        let src_base = HEADER_BYTES + ring_offset;
        let segment_len = (capacity - ring_offset).min(target.len() - copied);
        let src_end = src_base + segment_len;
        target[copied..copied + segment_len].copy_from_slice(
            mmap.get(src_base..src_end)
                .context("failed to read ring data segment")?,
        );
        copied += segment_len;
        ring_offset = 0;
    }
    Ok(())
}

fn load_u32(bytes: &[u8], offset: usize) -> anyhow::Result<u32> {
    let end = offset + std::mem::size_of::<u32>();
    Ok(u32::from_le_bytes(
        bytes
            .get(offset..end)
            .context("failed to read u32")?
            .try_into()
            .context("failed to decode u32")?,
    ))
}

fn load_u64(bytes: &[u8], offset: usize) -> anyhow::Result<u64> {
    let end = offset + std::mem::size_of::<u64>();
    Ok(u64::from_le_bytes(
        bytes
            .get(offset..end)
            .context("failed to read u64")?
            .try_into()
            .context("failed to decode u64")?,
    ))
}

fn atomic_u64(bytes: &[u8], offset: usize) -> &AtomicU64 {
    debug_assert_eq!(offset % align_of::<AtomicU64>(), 0);
    debug_assert!(bytes.len() >= offset + std::mem::size_of::<AtomicU64>());
    unsafe { &*(bytes.as_ptr().add(offset) as *const AtomicU64) }
}

#[cfg(test)]
mod tests {
    use super::{decode_account_frame, posix_shm_name_to_path, ACCOUNT_FRAME_KIND};

    #[test]
    fn test_account_frame_roundtrip() {
        let payload = encode_test_account_frame();
        let frame = decode_account_frame(&payload).expect("decode should succeed");
        assert_eq!(frame.sequence, 7);
        assert_eq!(frame.slot, 42);
        assert_eq!(frame.write_version, 99);
        assert_eq!(frame.lamports, 123_456);
        assert_eq!(frame.pubkey, [1u8; 32]);
        assert_eq!(frame.owner, [2u8; 32]);
        assert!(frame.executable);
        assert!(!frame.is_startup);
        assert_eq!(frame.txn_signature, Some([3u8; 64]));
        assert_eq!(frame.data, vec![7, 8, 9]);
    }

    #[test]
    fn test_posix_name_to_path() {
        assert_eq!(
            posix_shm_name_to_path("/yellowstone_accounts").unwrap(),
            "/dev/shm/yellowstone_accounts"
        );
        assert_eq!(
            posix_shm_name_to_path("yellowstone_accounts").unwrap(),
            "/dev/shm/yellowstone_accounts"
        );
    }

    #[test]
    fn test_crc32_corruption_detected() {
        let mut payload = encode_test_account_frame();
        // Corrupt one byte in the middle
        let mid = payload.len() / 2;
        payload[mid] ^= 0xFF;
        let result = decode_account_frame(&payload);
        assert!(result.is_err(), "corrupted payload should fail CRC check");
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("CRC32 mismatch"),
            "error should mention CRC32: {err_msg}"
        );
    }

    fn encode_test_account_frame() -> Vec<u8> {
        let mut payload = Vec::new();
        let flags = super::FLAG_EXECUTABLE | super::FLAG_HAS_TXN_SIGNATURE;
        payload.extend_from_slice(&ACCOUNT_FRAME_KIND.to_le_bytes());
        payload.extend_from_slice(&flags.to_le_bytes());
        payload.extend_from_slice(&7u64.to_le_bytes());
        payload.extend_from_slice(&1_700_000_000_123_456_789u64.to_le_bytes());
        payload.extend_from_slice(&42u64.to_le_bytes());
        payload.extend_from_slice(&99u64.to_le_bytes());
        payload.extend_from_slice(&123_456u64.to_le_bytes());
        payload.extend_from_slice(&12u64.to_le_bytes());
        payload.extend_from_slice(&[1u8; 32]);
        payload.extend_from_slice(&[2u8; 32]);
        payload.extend_from_slice(&[3u8; 64]);
        payload.extend_from_slice(&(3u32).to_le_bytes());
        payload.extend_from_slice(&[7u8, 8u8, 9u8]);
        // Append CRC32 to match writer format
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload);
        payload.extend_from_slice(&hasher.finalize().to_le_bytes());
        payload
    }
}

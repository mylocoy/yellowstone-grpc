use {
    crate::config::ConfigGrpcShm,
    anyhow::{anyhow, bail, Context},
    crc32fast::Hasher as Crc32Hasher,
    libc::{
        c_int, c_void, close, fstat, ftruncate, mmap, munmap, off_t, shm_open, MAP_FAILED,
        MAP_SHARED, O_CREAT, O_RDWR, PROT_READ, PROT_WRITE,
    },
    std::{
        ffi::CString,
        mem::align_of,
        ptr, slice,
        sync::atomic::{AtomicU64, Ordering},
    },
};

// ── Ring buffer wire format constants ──────────────────────────────────────
// IMPORTANT: These values define the cross-process ABI between the geyser
// plugin (writer) and external readers (e.g. examples/rust/src/shm_ring.rs).
// Any change here MUST be mirrored in the reader side.
// ───────────────────────────────────────────────────────────────────────────
const HEADER_BYTES: usize = 4096;
const HEADER_MAGIC: [u8; 8] = *b"YGRING01";
const HEADER_VERSION: u32 = 2;

const OFFSET_MAGIC: usize = 0;
const OFFSET_VERSION: usize = 8;
const OFFSET_HEADER_BYTES: usize = 12;
const OFFSET_CAPACITY: usize = 16;
const OFFSET_WRITE_POS: usize = 24;
const OFFSET_TAIL_POS: usize = 32;
const OFFSET_DROPPED_RECORDS: usize = 40;
const OFFSET_WRITTEN_RECORDS: usize = 48;

const ACCOUNT_FRAME_KIND: u16 = 1;
/// Fixed-size portion of an account frame (before variable-length `data`):
///   kind(2) + flags(2) + sequence(8) + nanos(8) + slot(8) + write_version(8)
///   + lamports(8) + rent_epoch(8) + pubkey(32) + owner(32) + txn_sig(64) + data_len(4)
const ACCOUNT_FRAME_FIXED_BYTES: usize = 2 + 2 + 8 + 8 + 8 + 8 + 8 + 8 + 32 + 32 + 64 + 4;
// Static assert: ensure the computed value matches the expected layout.
const _: () = assert!(ACCOUNT_FRAME_FIXED_BYTES == 184);

const FLAG_IS_STARTUP: u16 = 1 << 0;
const FLAG_EXECUTABLE: u16 = 1 << 1;
const FLAG_HAS_TXN_SIGNATURE: u16 = 1 << 2;

#[derive(Debug, Clone, Copy)]
struct RingHeaderMeta {
    capacity: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct AccountFrameInput<'a> {
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
    pub data: &'a [u8],
}

#[derive(Debug)]
pub struct PosixShmRingWriter {
    name: String,
    fd: c_int,
    mmap_ptr: *mut u8,
    mmap_len: usize,
    capacity: usize,
}

// SAFETY: PosixShmRingWriter owns the fd and mmap_ptr exclusively.
// Transferring ownership to another thread (Send) is safe because:
// 1. The raw pointer `mmap_ptr` is only accessed through `&mut self` methods.
// 2. The struct is always wrapped in `Mutex` in the plugin, ensuring exclusive access.
// Note: PosixShmRingWriter must NOT implement Sync — concurrent `&self` access
// to the mmap region would be a data race.
unsafe impl Send for PosixShmRingWriter {}

impl PosixShmRingWriter {
    pub fn create(config: &ConfigGrpcShm) -> anyhow::Result<Self> {
        let name = normalize_name(&config.name)?;
        let capacity = config.ring_bytes;
        if capacity <= 8 {
            bail!("ring_bytes must be larger than 8 bytes");
        }

        let fd = open_shm(&name, config.mode)?;
        let expected_mmap_len = HEADER_BYTES + capacity;

        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { fstat(fd, stat.as_mut_ptr()) } != 0 {
            unsafe {
                close(fd);
            }
            return Err(last_os_error_with(format!("fstat failed for shm {name}")));
        }
        let current_size = unsafe { stat.assume_init() }.st_size.max(0) as usize;

        let should_reset =
            config.reset_on_start || current_size == 0 || current_size != expected_mmap_len;
        if should_reset {
            if unsafe { ftruncate(fd, expected_mmap_len as off_t) } != 0 {
                unsafe {
                    close(fd);
                }
                return Err(last_os_error_with(format!(
                    "ftruncate failed for shm {name}"
                )));
            }
        }

        let mmap_ptr = unsafe {
            mmap(
                ptr::null_mut(),
                expected_mmap_len,
                PROT_READ | PROT_WRITE,
                MAP_SHARED,
                fd,
                0,
            )
        };
        if mmap_ptr == MAP_FAILED {
            unsafe {
                close(fd);
            }
            return Err(last_os_error_with(format!("mmap failed for shm {name}")));
        }

        let mut this = Self {
            name,
            fd,
            mmap_ptr: mmap_ptr.cast::<u8>(),
            mmap_len: expected_mmap_len,
            capacity,
        };

        if should_reset {
            this.initialize_header();
        } else {
            let header = verify_header(this.mmap_slice())?;
            if header.capacity != capacity {
                bail!(
                    "ring capacity mismatch for {}: mapped={} requested={capacity}",
                    this.name,
                    header.capacity
                );
            }
        }

        Ok(this)
    }

    pub fn shm_name(&self) -> &str {
        &self.name
    }

    /// Returns the filesystem path for this SHM object.
    /// Only valid on Linux where POSIX SHM is backed by `/dev/shm`.
    #[cfg(target_os = "linux")]
    pub fn shm_path(&self) -> String {
        format!("/dev/shm/{}", self.name.trim_start_matches('/'))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn shm_path(&self) -> String {
        format!("<platform-specific>:{}", self.name)
    }

    pub fn push_account_frame(&mut self, frame: AccountFrameInput<'_>) -> anyhow::Result<()> {
        let payload = encode_account_frame(frame)?;
        self.push_payload(&payload)
    }

    fn push_payload(&mut self, payload: &[u8]) -> anyhow::Result<()> {
        let payload_len = u32::try_from(payload.len()).context("payload is larger than 4GiB")?;
        let record_len = payload_len as usize + std::mem::size_of::<u32>();
        if record_len > self.capacity {
            bail!(
                "record size {record_len} exceeds ring capacity {}",
                self.capacity
            );
        }

        let capacity_u64 = self.capacity as u64;
        let write_pos = self.load_write_pos();
        let mut tail_pos = self.load_tail_pos();
        let next_write = write_pos
            .checked_add(record_len as u64)
            .context("write_pos overflow: ring buffer absolute offset exhausted")?;

        // Guard against corrupted tail_pos (e.g. after crash or external tampering).
        // If tail_pos is ahead of write_pos, reset it to make forward progress.
        if tail_pos > write_pos {
            tail_pos = write_pos;
        }

        let mut dropped_records = 0u64;
        while next_write.saturating_sub(tail_pos) > capacity_u64 {
            if tail_pos >= write_pos {
                tail_pos = next_write - capacity_u64;
                dropped_records += 1;
                break;
            }

            let dropped_payload_len = self.read_u32_at(tail_pos)? as usize;
            let dropped_record_len = dropped_payload_len.saturating_add(std::mem::size_of::<u32>());
            if dropped_payload_len == 0 || dropped_record_len > self.capacity {
                tail_pos = next_write - capacity_u64;
                dropped_records += 1;
                break;
            }

            tail_pos += dropped_record_len as u64;
            dropped_records += 1;
        }

        if tail_pos != self.load_tail_pos() {
            self.store_tail_pos(tail_pos);
        }
        if dropped_records > 0 {
            self.fetch_add_dropped_records(dropped_records);
        }

        self.write_ring_bytes(write_pos, &payload_len.to_le_bytes());
        self.write_ring_bytes(write_pos + 4, payload);
        // Release-store on write_pos acts as the publish barrier: the reader
        // performs an Acquire-load on the same location (plus an explicit fence
        // on non-TSO architectures) to ensure all ring data written above is
        // visible before the updated write_pos.
        self.store_write_pos(next_write);
        self.fetch_add_written_records(1);

        Ok(())
    }

    fn initialize_header(&mut self) {
        let capacity = self.capacity as u64;
        self.mmap_slice_mut()[..HEADER_BYTES].fill(0);
        self.mmap_slice_mut()[OFFSET_MAGIC..OFFSET_MAGIC + HEADER_MAGIC.len()]
            .copy_from_slice(&HEADER_MAGIC);
        store_u32(self.mmap_slice_mut(), OFFSET_VERSION, HEADER_VERSION);
        store_u32(
            self.mmap_slice_mut(),
            OFFSET_HEADER_BYTES,
            HEADER_BYTES as u32,
        );
        store_u64(self.mmap_slice_mut(), OFFSET_CAPACITY, capacity);
        atomic_u64_mut(self.mmap_slice_mut(), OFFSET_WRITE_POS).store(0, Ordering::Relaxed);
        atomic_u64_mut(self.mmap_slice_mut(), OFFSET_TAIL_POS).store(0, Ordering::Relaxed);
        atomic_u64_mut(self.mmap_slice_mut(), OFFSET_DROPPED_RECORDS).store(0, Ordering::Relaxed);
        atomic_u64_mut(self.mmap_slice_mut(), OFFSET_WRITTEN_RECORDS).store(0, Ordering::Relaxed);
    }

    fn mmap_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.mmap_ptr, self.mmap_len) }
    }

    fn mmap_slice_mut(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.mmap_ptr, self.mmap_len) }
    }

    fn load_write_pos(&self) -> u64 {
        atomic_u64(self.mmap_slice(), OFFSET_WRITE_POS).load(Ordering::Acquire)
    }

    fn store_write_pos(&mut self, value: u64) {
        atomic_u64_mut(self.mmap_slice_mut(), OFFSET_WRITE_POS).store(value, Ordering::Release);
    }

    fn load_tail_pos(&self) -> u64 {
        atomic_u64(self.mmap_slice(), OFFSET_TAIL_POS).load(Ordering::Acquire)
    }

    fn store_tail_pos(&mut self, value: u64) {
        atomic_u64_mut(self.mmap_slice_mut(), OFFSET_TAIL_POS).store(value, Ordering::Release);
    }

    fn fetch_add_dropped_records(&mut self, value: u64) {
        atomic_u64_mut(self.mmap_slice_mut(), OFFSET_DROPPED_RECORDS)
            .fetch_add(value, Ordering::Relaxed);
    }

    fn fetch_add_written_records(&mut self, value: u64) {
        atomic_u64_mut(self.mmap_slice_mut(), OFFSET_WRITTEN_RECORDS)
            .fetch_add(value, Ordering::Relaxed);
    }

    fn write_ring_bytes(&mut self, absolute_offset: u64, data: &[u8]) {
        let capacity = self.capacity;
        copy_into_ring(self.mmap_slice_mut(), capacity, absolute_offset, data);
    }

    fn read_u32_at(&self, absolute_offset: u64) -> anyhow::Result<u32> {
        let mut bytes = [0u8; 4];
        copy_from_ring(
            self.mmap_slice(),
            self.capacity,
            absolute_offset,
            &mut bytes,
        )?;
        Ok(u32::from_le_bytes(bytes))
    }
}

impl Drop for PosixShmRingWriter {
    fn drop(&mut self) {
        if !self.mmap_ptr.is_null() && self.mmap_len > 0 {
            unsafe {
                munmap(self.mmap_ptr.cast::<c_void>(), self.mmap_len);
            }
        }
        if self.fd >= 0 {
            unsafe {
                close(self.fd);
            }
        }
    }
}

fn normalize_name(name: &str) -> anyhow::Result<String> {
    if name.is_empty() {
        bail!("shm name must not be empty");
    }
    if name.contains('\0') {
        bail!("shm name must not contain NUL");
    }
    let name = if name.starts_with('/') {
        name.to_owned()
    } else {
        format!("/{name}")
    };
    if name.len() <= 1 {
        bail!("shm name must contain non-slash characters");
    }
    Ok(name)
}

fn open_shm(name: &str, mode: u32) -> anyhow::Result<c_int> {
    let c_name = CString::new(name).context("failed to convert shm name to CString")?;
    let fd = unsafe {
        #[cfg(target_vendor = "apple")]
        {
            shm_open(c_name.as_ptr(), O_CREAT | O_RDWR, mode as libc::c_uint)
        }
        #[cfg(not(target_vendor = "apple"))]
        {
            shm_open(c_name.as_ptr(), O_CREAT | O_RDWR, mode as libc::mode_t)
        }
    };
    if fd < 0 {
        Err(last_os_error_with(format!(
            "shm_open failed for {name} with mode {mode:o}"
        )))
    } else {
        Ok(fd)
    }
}

fn verify_header(mmap: &[u8]) -> anyhow::Result<RingHeaderMeta> {
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

    Ok(RingHeaderMeta { capacity })
}

fn encode_account_frame(frame: AccountFrameInput<'_>) -> anyhow::Result<Vec<u8>> {
    let data_len = u32::try_from(frame.data.len()).context("account data is larger than 4GiB")?;
    // +4 for trailing CRC32
    let mut payload = Vec::with_capacity(ACCOUNT_FRAME_FIXED_BYTES + frame.data.len() + 4);
    payload.extend_from_slice(&ACCOUNT_FRAME_KIND.to_le_bytes());

    let mut flags = 0u16;
    if frame.is_startup {
        flags |= FLAG_IS_STARTUP;
    }
    if frame.executable {
        flags |= FLAG_EXECUTABLE;
    }
    if frame.txn_signature.is_some() {
        flags |= FLAG_HAS_TXN_SIGNATURE;
    }
    payload.extend_from_slice(&flags.to_le_bytes());
    payload.extend_from_slice(&frame.sequence.to_le_bytes());
    payload.extend_from_slice(&frame.created_at_unix_nanos.to_le_bytes());
    payload.extend_from_slice(&frame.slot.to_le_bytes());
    payload.extend_from_slice(&frame.write_version.to_le_bytes());
    payload.extend_from_slice(&frame.lamports.to_le_bytes());
    payload.extend_from_slice(&frame.rent_epoch.to_le_bytes());
    payload.extend_from_slice(&frame.pubkey);
    payload.extend_from_slice(&frame.owner);
    payload.extend_from_slice(&frame.txn_signature.unwrap_or([0u8; 64]));
    payload.extend_from_slice(&data_len.to_le_bytes());
    payload.extend_from_slice(frame.data);

    // Append CRC32 checksum over the entire payload for integrity verification.
    let mut hasher = Crc32Hasher::new();
    hasher.update(&payload);
    payload.extend_from_slice(&hasher.finalize().to_le_bytes());

    Ok(payload)
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

fn copy_into_ring(mmap: &mut [u8], capacity: usize, absolute_offset: u64, source: &[u8]) {
    if source.is_empty() {
        return;
    }

    let mut ring_offset = (absolute_offset as usize) % capacity;
    let mut copied = 0usize;
    while copied < source.len() {
        let dst_base = HEADER_BYTES + ring_offset;
        let segment_len = (capacity - ring_offset).min(source.len() - copied);
        let dst_end = dst_base + segment_len;
        mmap[dst_base..dst_end].copy_from_slice(&source[copied..copied + segment_len]);
        copied += segment_len;
        ring_offset = 0;
    }
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

fn store_u32(bytes: &mut [u8], offset: usize, value: u32) {
    let end = offset + std::mem::size_of::<u32>();
    bytes[offset..end].copy_from_slice(&value.to_le_bytes());
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

fn store_u64(bytes: &mut [u8], offset: usize, value: u64) {
    let end = offset + std::mem::size_of::<u64>();
    bytes[offset..end].copy_from_slice(&value.to_le_bytes());
}

fn atomic_u64(bytes: &[u8], offset: usize) -> &AtomicU64 {
    debug_assert_eq!(offset % align_of::<AtomicU64>(), 0);
    debug_assert!(bytes.len() >= offset + std::mem::size_of::<AtomicU64>());
    unsafe { &*(bytes.as_ptr().add(offset) as *const AtomicU64) }
}

fn atomic_u64_mut(bytes: &mut [u8], offset: usize) -> &AtomicU64 {
    debug_assert_eq!(offset % align_of::<AtomicU64>(), 0);
    debug_assert!(bytes.len() >= offset + std::mem::size_of::<AtomicU64>());
    unsafe { &*(bytes.as_mut_ptr().add(offset) as *const AtomicU64) }
}

fn last_os_error_with(message: String) -> anyhow::Error {
    anyhow!("{message}: {}", std::io::Error::last_os_error())
}

use {
    anyhow::bail,
    clap::Parser,
    log::info,
    solana_pubkey::Pubkey,
    std::{
        collections::HashMap,
        path::Path,
        time::{Duration, Instant},
    },
    yellowstone_grpc_client_simple::shm_ring::{
        posix_shm_name_to_path, SharedRingReader, DEFAULT_POSIX_SHM_NAME,
    },
};

#[derive(Debug, Clone, Parser)]
#[clap(
    author,
    version,
    about = "Read account frames from Yellowstone /dev/shm ring"
)]
struct Args {
    /// Ring file path; ignored when --shm-name is set
    #[clap(long)]
    shm_path: Option<String>,

    /// POSIX shm object name from plugin config, e.g. /yellowstone_accounts
    #[clap(long)]
    shm_name: Option<String>,

    /// Start consuming from oldest available message instead of latest
    #[clap(long, default_value_t = false)]
    from_start: bool,

    /// Poll sleep in microseconds when ring has no new message
    #[clap(long, default_value_t = 1_000)]
    poll_interval_us: u64,

    /// Print each account update line
    #[clap(long, default_value_t = false)]
    print_updates: bool,

    /// Max bytes to preview when printing updates
    #[clap(long, default_value_t = 16)]
    data_preview_bytes: usize,

    /// Log stats interval in milliseconds
    #[clap(long, default_value_t = 1_000)]
    stats_interval_ms: u64,

    /// Disable per-account dedup by (slot, write_version)
    #[clap(long, default_value_t = false)]
    disable_dedup: bool,
}

/// Maximum number of pubkeys tracked in the dedup map.
/// When exceeded, the map is cleared to reclaim memory. On Solana mainnet
/// there are ~300M accounts, so this caps memory at ~40 bytes * 2M ≈ 80 MB.
const DEDUP_MAX_ENTRIES: usize = 2_000_000;

fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();
    let shm_path = resolve_shm_path(&args)?;
    let mut reader = SharedRingReader::open(Path::new(&shm_path), !args.from_start)?;
    info!(
        "reader started: path={} from_start={} dedup={}",
        shm_path, args.from_start, !args.disable_dedup
    );

    let poll_interval = Duration::from_micros(args.poll_interval_us.max(50));
    let stats_interval = Duration::from_millis(args.stats_interval_ms.max(100));
    let mut last_report = Instant::now();
    let mut interval_messages = 0u64;
    let mut interval_data_bytes = 0u64;
    let mut interval_seen = 0u64;
    let mut interval_dedup_dropped = 0u64;
    let mut dedup_state: HashMap<[u8; 32], (u64, u64)> = HashMap::new();

    loop {
        match reader.next_account_frame()? {
            Some(frame) => {
                interval_seen += 1;

                if !args.disable_dedup {
                    let keep = match dedup_state.get(&frame.pubkey) {
                        Some((slot, write_version)) => {
                            frame.slot > *slot
                                || (frame.slot == *slot && frame.write_version > *write_version)
                        }
                        None => true,
                    };
                    if !keep {
                        interval_dedup_dropped += 1;
                        continue;
                    }
                    dedup_state.insert(frame.pubkey, (frame.slot, frame.write_version));
                    if dedup_state.len() > DEDUP_MAX_ENTRIES {
                        info!(
                            "dedup map exceeded {} entries, clearing",
                            DEDUP_MAX_ENTRIES
                        );
                        dedup_state.clear();
                    }
                }

                interval_messages += 1;
                interval_data_bytes += frame.data.len() as u64;

                if args.print_updates {
                    let pubkey = Pubkey::new_from_array(frame.pubkey);
                    let owner = Pubkey::new_from_array(frame.owner);
                    let preview_size = args.data_preview_bytes.min(frame.data.len());
                    let preview = &frame.data[..preview_size];
                    info!(
                        "seq={} slot={} wv={} owner={} pubkey={} data_len={} preview={}{}",
                        frame.sequence,
                        frame.slot,
                        frame.write_version,
                        owner,
                        pubkey,
                        frame.data.len(),
                        hex::encode(preview),
                        if frame.data.len() > preview_size {
                            "..."
                        } else {
                            ""
                        }
                    );
                }
                continue;
            }
            None => {
                std::thread::sleep(poll_interval);
            }
        }

        if last_report.elapsed() >= stats_interval {
            let stats = reader.stats();
            info!(
                "reader stats: seen={} delivered={} dedup_dropped={} tracked_accounts={} data_bytes={} skipped_local={} dropped_global={} write_pos={} tail_pos={} total_written={}",
                interval_seen,
                interval_messages,
                interval_dedup_dropped,
                dedup_state.len(),
                interval_data_bytes,
                stats.skipped_records,
                stats.dropped_records,
                stats.write_pos,
                stats.tail_pos,
                stats.written_records
            );
            interval_seen = 0;
            interval_messages = 0;
            interval_dedup_dropped = 0;
            interval_data_bytes = 0;
            last_report = Instant::now();
        }
    }
}

fn resolve_shm_path(args: &Args) -> anyhow::Result<String> {
    match (&args.shm_name, &args.shm_path) {
        (Some(_), Some(_)) => bail!("use either --shm-name or --shm-path, not both"),
        (Some(name), None) => posix_shm_name_to_path(name),
        (None, Some(path)) => Ok(path.clone()),
        (None, None) => posix_shm_name_to_path(DEFAULT_POSIX_SHM_NAME),
    }
}

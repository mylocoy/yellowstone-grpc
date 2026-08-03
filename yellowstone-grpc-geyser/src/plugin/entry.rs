use {
    crate::{
        config::Config,
        file_watcher::FileWatcher,
        grpc::{BlockReconstructionMessage, BroadcastedMessage, GrpcService},
        metrics::{self, incr_geyser_event_dropped, PrometheusService},
        plugin::{
            filter::limits::FilterLimits,
            message::{
                CommitmentLevel, Message, MessageAccount, MessageBlockMeta,
                MessageDeshredTransaction, MessageEntry, MessageSlot, MessageTransaction,
            },
        },
        shm::{AccountFrameInput, PosixShmRingWriter},
        stream::tokio::BatchStreamUnboundedReceiver,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoV3, ReplicaAccountInfoVersions,
        ReplicaBlockInfoVersions, ReplicaDeshredTransactionInfoVersions, ReplicaEntryInfoVersions,
        ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
    },
    solana_pubkey::Pubkey,
    std::{
        collections::HashSet,
        concat, env,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex, Once,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        runtime::{Builder, Runtime},
        sync::{broadcast, mpsc},
    },
    tokio_rustls::rustls,
    tokio_util::{sync::CancellationToken, task::TaskTracker},
};

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    snapshot_channel: Mutex<Option<crossbeam_channel::Sender<Box<Message>>>>,
    snapshot_channel_closed: AtomicBool,
    filter_limits: FilterLimits,
    grpc_channel: mpsc::UnboundedSender<Message>, // geyser_loop
    deshred_channel: broadcast::Sender<Message>,  // deshred_client_loop
    block_reconstruction_channel: mpsc::UnboundedSender<BlockReconstructionMessage>, // block_reconstruction_loop
    broadcast_channel: broadcast::Sender<BroadcastedMessage>,                        // client_loop
    blocks_meta_tx: Option<mpsc::UnboundedSender<Message>>,
    static_owner_allowlist: Option<HashSet<[u8; 32]>>,
    shm_writer: Option<Mutex<PosixShmRingWriter>>,
    shm_sequence: AtomicU64,
    /// Consecutive SHM write error count. Reset to 0 on success.
    shm_consecutive_errors: AtomicU64,
    /// Unix timestamp in nanos; SHM writes are skipped while now < retry_after.
    shm_retry_after_unix_nanos: AtomicU64,
    shm_disable_grpc_accounts: bool,
    plugin_cancellation_token: CancellationToken,
    plugin_task_tracker: TaskTracker,
    file_watcher: Arc<FileWatcher>,
}

impl PluginInner {
    // Sends messages to the geyser_loop
    fn send_message(&self, message: Message) {
        if self.grpc_channel.send(message).is_ok() {
            metrics::message_queue_size_inc();
        }
    }

    // Sends messages to the deshred subscribers if their filter allows it.
    fn send_deshred_message(&self, message: Message) {
        if let Ok(count) = self.deshred_channel.send(message) {
            metrics::deshred_queue_size_inc(count as i64);
        }
    }

    // Sends messages to the block reconstruction loop
    fn send_block_reconstruction_message(&self, message: BlockReconstructionMessage) {
        if self.block_reconstruction_channel.send(message).is_ok() {
            metrics::block_reconstruction_queue_size_inc();
        }
    }

    // Sends messages to all subscribed clients if their filter matches the message.
    fn send_broadcast_message(&self, message: BroadcastedMessage) {
        let _ = self.broadcast_channel.send(message);
    }

    // Sends messages to block meta storage
    fn send_blocks_meta_message(&self, message: Message) {
        if let Some(blocks_meta_tx) = &self.blocks_meta_tx {
            let _ = blocks_meta_tx.send(message);
        }
    }

    fn is_account_owner_allowed(&self, owner: &[u8]) -> bool {
        match &self.static_owner_allowlist {
            Some(allowlist) => <&[u8; 32]>::try_from(owner)
                .map(|owner| allowlist.contains(owner))
                .unwrap_or(false),
            None => true,
        }
    }

    /// Maximum consecutive SHM write errors before pausing writes temporarily.
    const SHM_MAX_CONSECUTIVE_ERRORS: u64 = 64;
    /// Backoff duration after reaching `SHM_MAX_CONSECUTIVE_ERRORS`.
    const SHM_ERROR_BACKOFF: Duration = Duration::from_secs(5);

    fn write_account_to_shm(
        &self,
        account: &ReplicaAccountInfoV3<'_>,
        slot: u64,
        is_startup: bool,
    ) {
        let Some(shm_writer) = &self.shm_writer else {
            return;
        };

        let retry_after = self.shm_retry_after_unix_nanos.load(Ordering::Relaxed);
        if retry_after != 0 {
            let now_unix_nanos = Self::unix_now_nanos();
            if now_unix_nanos < retry_after {
                return;
            }
            if self
                .shm_retry_after_unix_nanos
                .compare_exchange(retry_after, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                // Reset error counter here so the second guard below cannot
                // falsely re-block the retry if record_shm_error races in
                // from another thread between CAS and the guard check.
                self.shm_consecutive_errors.store(0, Ordering::Relaxed);
                log::warn!("shm write backoff elapsed, attempting writes again");
            }
        }

        if self.shm_consecutive_errors.load(Ordering::Relaxed) >= Self::SHM_MAX_CONSECUTIVE_ERRORS
            && self.shm_retry_after_unix_nanos.load(Ordering::Relaxed) != 0
        {
            return;
        }

        let Ok(pubkey) = <[u8; 32]>::try_from(account.pubkey) else {
            self.record_shm_error("invalid pubkey length in account update");
            return;
        };
        let Ok(owner) = <[u8; 32]>::try_from(account.owner) else {
            self.record_shm_error("invalid owner length in account update");
            return;
        };
        let txn_signature = account
            .txn
            .and_then(|txn| <[u8; 64]>::try_from(txn.signature().as_ref()).ok());

        let created_at_unix_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        let sequence = self.shm_sequence.fetch_add(1, Ordering::Relaxed) + 1;

        let Ok(mut writer) = shm_writer.lock() else {
            self.record_shm_error("failed to lock shm writer (poisoned)");
            return;
        };

        match writer.push_account_frame(AccountFrameInput {
            sequence,
            created_at_unix_nanos,
            slot,
            write_version: account.write_version,
            lamports: account.lamports,
            rent_epoch: account.rent_epoch,
            pubkey,
            owner,
            executable: account.executable,
            is_startup,
            txn_signature,
            data: account.data,
        }) {
            Ok(()) => {
                // Reset error counter on success.
                self.shm_consecutive_errors.store(0, Ordering::Relaxed);
                self.shm_retry_after_unix_nanos.store(0, Ordering::Relaxed);
            }
            Err(error) => {
                self.record_shm_error(&format!("shm write failed: {error:?}"));
            }
        }
    }

    fn record_shm_error(&self, message: &str) {
        let prev = self.shm_consecutive_errors.load(Ordering::Relaxed);
        if prev >= Self::SHM_MAX_CONSECUTIVE_ERRORS {
            // Already at threshold — skip increment to avoid unbounded growth.
            return;
        }
        let consecutive_errors = self.shm_consecutive_errors.fetch_add(1, Ordering::Relaxed) + 1;
        if consecutive_errors == 1 {
            // Log only the first error in a streak to avoid log spam.
            log::error!("{message}");
        }
        if consecutive_errors >= Self::SHM_MAX_CONSECUTIVE_ERRORS {
            let now_unix_nanos = Self::unix_now_nanos();
            let backoff_nanos = Self::SHM_ERROR_BACKOFF.as_nanos().min(u64::MAX as u128) as u64;
            let retry_after = now_unix_nanos.saturating_add(backoff_nanos);
            self.shm_retry_after_unix_nanos
                .store(retry_after, Ordering::Relaxed);
            if consecutive_errors == Self::SHM_MAX_CONSECUTIVE_ERRORS {
                log::error!(
                    "shm write paused after {} consecutive errors, retrying after {:?}",
                    Self::SHM_MAX_CONSECUTIVE_ERRORS,
                    Self::SHM_ERROR_BACKOFF
                );
            }
        }
    }

    fn unix_now_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or(0)
    }
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl Plugin {
    fn with_inner<F>(&self, f: F) -> PluginResult<()>
    where
        F: FnOnce(&PluginInner) -> PluginResult<()>,
    {
        let inner = self.inner.as_ref().expect("initialized");
        f(inner)
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(
            env!("CARGO_PKG_NAME"),
            "-",
            env!("CARGO_PKG_VERSION"),
            "+",
            env!("GIT_VERSION")
        )
    }

    fn on_load(&mut self, config_file: &str, is_reload: bool) -> PluginResult<()> {
        let config = Config::load_from_file(config_file)?;
        let filter_limits = config.grpc.filter_limits.clone();

        // Setup logger
        solana_logger::setup_with_default(&config.log.level);

        log::info!("loading plugin: {}", self.name());

        let static_owner_allowlist = (!config.grpc.static_owner_allowlist.is_empty()).then(|| {
            config
                .grpc
                .static_owner_allowlist
                .iter()
                .map(|owner| owner.to_bytes())
                .collect::<HashSet<_>>()
        });
        let shm_disable_grpc_accounts = config
            .grpc
            .shm
            .as_ref()
            .map(|shm| shm.disable_grpc_accounts)
            .unwrap_or(false);
        let shm_writer = match &config.grpc.shm {
            Some(shm_config) => {
                let shm_writer = PosixShmRingWriter::create(shm_config).map_err(|error| {
                    GeyserPluginError::Custom(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("{error:?}"),
                    )))
                })?;
                log::info!(
                    "shm output enabled: name={} path={} ring_bytes={} mode={:o} reset_on_start={} disable_grpc_accounts={}",
                    shm_writer.shm_name(),
                    shm_writer.shm_path(),
                    shm_config.ring_bytes,
                    shm_config.mode,
                    shm_config.reset_on_start,
                    shm_config.disable_grpc_accounts
                );
                Some(Mutex::new(shm_writer))
            }
            None => None,
        };
        if let Some(allowlist) = &static_owner_allowlist {
            log::info!(
                "static owner allowlist enabled with {} owners",
                allowlist.len()
            );
        }
        if shm_disable_grpc_accounts {
            log::warn!("gRPC account forwarding disabled; account updates are available only via shm output");
        }

        // Reset metrics to prevent accumulation across plugin reload cycles
        metrics::reset_metrics();

        // Create inner
        let mut builder = Builder::new_multi_thread();
        if let Some(worker_threads) = config.tokio.worker_threads {
            builder.worker_threads(worker_threads);
        }
        if let Some(tokio_cpus) = config.tokio.affinity.clone() {
            builder.on_thread_start(move || {
                crate::util::cpu_core_affinity::set_thread_affinity(&tokio_cpus)
                    .expect("failed to set affinity")
            });
        }
        let plugin_cancellation_token = CancellationToken::new();
        let plugin_task_tracker = TaskTracker::new();
        let prometheus_cancellation_token = plugin_cancellation_token.child_token();
        let prometheus_task_tracker = plugin_task_tracker.clone();
        let grpc_cancellation_token = plugin_cancellation_token.child_token();
        let grpc_task_tracker = plugin_task_tracker.clone();

        let runtime = builder
            .thread_name_fn(crate::get_thread_name)
            .enable_all()
            .build()
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

        let file_watcher = crate::file_watcher::FileWatcher::new().map_err(|error| {
            GeyserPluginError::Custom(format!("failed to create file watcher: {error:?}").into())
        })?;
        let file_watcher = Arc::new(file_watcher);
        let geyser_svc_file_watcher = Arc::clone(&file_watcher);
        let (grpc_channel_tx, grpc_channel_receiver) = mpsc::unbounded_channel();
        let result = runtime.block_on(async move {
            static CRYPTO_PROVIDER_INIT: Once = Once::new();

            CRYPTO_PROVIDER_INIT.call_once(|| {
                let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
            });
            // Create prometheus service First so if it fails the plugin doesn't spawn geyser tasks unnecessarily.
            PrometheusService::spawn(
                config.prometheus,
                prometheus_cancellation_token,
                prometheus_task_tracker,
            )
            .await
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

            let grpc_channel_rx = BatchStreamUnboundedReceiver::new(grpc_channel_receiver);
            let grpc_service_result = GrpcService::create(
                config.grpc,
                is_reload,
                grpc_cancellation_token,
                grpc_task_tracker,
                geyser_svc_file_watcher,
                grpc_channel_rx,
            )
            .await
            .map_err(|error| GeyserPluginError::Custom(format!("{error:?}").into()))?;
            Ok::<_, GeyserPluginError>(grpc_service_result)
        });

        let grpc_service_result = match result {
            Ok(val) => val,
            Err(e) => {
                log::error!("failed to start plugin services: {e}");
                plugin_cancellation_token.cancel();
                // join before returning because encoder already dropped, channel closed
                return Err(GeyserPluginError::Custom(format!("{e:?}").into()));
            }
        };

        self.inner = Some(PluginInner {
            runtime,
            snapshot_channel: Mutex::new(grpc_service_result.snapshot_tx),
            snapshot_channel_closed: AtomicBool::new(false),
            filter_limits,
            grpc_channel: grpc_channel_tx,
            deshred_channel: grpc_service_result.deshred_broadcast_tx,
            block_reconstruction_channel: grpc_service_result.block_reconstruction_tx,
            broadcast_channel: grpc_service_result.broadcast_tx,
            blocks_meta_tx: grpc_service_result.blocks_meta_tx,
            static_owner_allowlist,
            shm_writer,
            shm_sequence: AtomicU64::new(0),
            shm_consecutive_errors: AtomicU64::new(0),
            shm_retry_after_unix_nanos: AtomicU64::new(0),
            shm_disable_grpc_accounts,
            plugin_cancellation_token,
            plugin_task_tracker,
            file_watcher,
        });

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            let number_of_tasks = inner.plugin_task_tracker.len();
            log::info!("shutting down plugin: {number_of_tasks} tasks to cancel.");
            inner.plugin_cancellation_token.cancel();
            inner.plugin_task_tracker.close();
            drop(inner.file_watcher);
            drop(inner.grpc_channel);
            const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
            let now = std::time::Instant::now();
            log::info!(
                "waiting up to {:?} for plugin tasks to shut down",
                SHUTDOWN_TIMEOUT
            );
            inner.runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);
            log::info!("tokio runtime shut down in {:?}", now.elapsed());
            log::info!("plugin shutdown complete");
        }
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let account = match account {
                ReplicaAccountInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_3(info) => info,
            };
            if !inner.is_account_owner_allowed(account.owner) {
                return Ok(());
            }
            inner.write_account_to_shm(account, slot, is_startup);
            if inner.shm_disable_grpc_accounts {
                return Ok(());
            }

            if let Ok(owner) = Pubkey::try_from(account.owner) {
                // Drop accounts from owners in the drop list, even during startup.
                if inner.filter_limits.accounts.owner_reject.contains(&owner) {
                    incr_geyser_event_dropped("account");
                    return Ok(());
                }
            }

            if is_startup {
                if let Some(channel) = inner.snapshot_channel.lock().unwrap().as_ref() {
                    let message = Message::Account(Arc::new(MessageAccount::from_geyser(
                        account, slot, is_startup,
                    )));
                    match channel.send(Box::new(message)) {
                        Ok(()) => metrics::message_queue_size_inc(),
                        Err(_) => {
                            if !inner.snapshot_channel_closed.swap(true, Ordering::Relaxed) {
                                log::error!(
                                    "failed to send message to startup queue: channel closed"
                                )
                            }
                        }
                    }
                }
            } else {
                let message = Message::Account(Arc::new(MessageAccount::from_geyser(
                    account, slot, is_startup,
                )));
                inner.send_message(message);
            }

            Ok(())
        })
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        self.with_inner(|inner| {
            let _snapshot_channel = inner.snapshot_channel.lock().unwrap().take();
            Ok(())
        })
    }

    fn update_slot_status(
        &self,
        slot: u64,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let message = Message::Slot(Arc::new(MessageSlot::from_geyser(slot, parent, status)));
            if matches!(
                status,
                SlotStatus::Processed | SlotStatus::Confirmed | SlotStatus::Rooted
            ) {
                // Processed/Confirmed/Finalized slot status updates are handled by the block reconstruction loop and are never directly exposed by the processed message loop (geyser_loop.)
                // If they were to be sent through geyser_loop, the block_reconstruction_loop would also send them, resulting in duplicate slot life-cycle messages.
                // By sending them directly to block_reconstruction_loop, we avoid this issue while ensuring block reconstruction happens as normal.
                inner.send_block_reconstruction_message(BlockReconstructionMessage::Single(
                    message.clone(),
                ));
            } else {
                // The only remaining states are FirstShredReceived/Completed/CreatedBank/Dead.
                // These states are used by both block reconstruction and the geyser_loop (processed message loop).
                // When sending to geyser_loop, the loop will batch all messages (whether it's slot updates, account updates, or transaction updates) and send them to the block_reconstruction_loop in a single batch.
                // CreatedBank in particular is critical to the life-cycle of a block reconstruction, but it is not forwarded to the subscribed client from block_reconstruction_loop, so it must be sent to geyser_loop to ensure subscribers receive it.
                inner.send_message(message.clone());

                // Note: The following if statement remains here in case of any future additions to a slot's life-cycle state.
                // It could be removed as it is right now, since we've already filtered out all other states above, but it is left here for clarity and future-proofing.
                if matches!(
                    status,
                    SlotStatus::FirstShredReceived
                        | SlotStatus::Completed
                        | SlotStatus::CreatedBank
                        | SlotStatus::Dead(_)
                ) {
                    // FirstShredReceived/Completed/CreatedBank/Dead slot status updates for Confirmed/Finalized commitment subscribers are not explicitly sent by the block reconstruction loop.
                    // Therefore we explicitly need to forward these updates to the subscribers for all commitment levels, the geyser_loop will take care of forwarding them to the Processed commitment level.
                    let messages = Arc::new(vec![message.clone()]);
                    inner.send_broadcast_message((
                        CommitmentLevel::Confirmed,
                        Arc::clone(&messages),
                    ));
                    inner.send_broadcast_message((CommitmentLevel::Finalized, messages));
                }
            }

            // Deshred subscribers need to receive all slot status updates.
            inner.send_deshred_message(message.clone());

            // Blocks meta subscribers need to receive all slot status updates.
            inner.send_blocks_meta_message(message);

            metrics::update_slot_status(status, slot);
            Ok(())
        })
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let transaction = match transaction {
                ReplicaTransactionInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaTransactionInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
                }
                ReplicaTransactionInfoVersions::V0_0_3(info) => info,
            };

            let message =
                Message::Transaction(Arc::new(MessageTransaction::from_geyser(transaction, slot)));
            inner.send_message(message);

            Ok(())
        })
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        self.with_inner(|inner| {
            #[allow(clippy::infallible_destructuring_match)]
            let entry = match entry {
                ReplicaEntryInfoVersions::V0_0_1(_entry) => {
                    unreachable!("ReplicaEntryInfoVersions::V0_0_1 is not supported")
                }
                ReplicaEntryInfoVersions::V0_0_2(entry) => entry,
            };

            let message = Message::Entry(Arc::new(MessageEntry::from_geyser(entry)));
            inner.send_message(message);

            Ok(())
        })
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        self.with_inner(|inner| {
            let blockinfo = match blockinfo {
                ReplicaBlockInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_1 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_2 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_3(_info) => {
                    unreachable!("ReplicaBlockInfoVersions::V0_0_3 is not supported")
                }
                ReplicaBlockInfoVersions::V0_0_4(info) => info,
            };

            let message = Message::BlockMeta(Arc::new(MessageBlockMeta::from_geyser(blockinfo)));

            // It's super important that block-meta message goes to the geyser loop message channel,
            // and not straight to block-reconstruction.
            // The reason why is during block freeze, in agave, some account update are emitted really late, but before block-meta.
            // In order to guarantee those account update are seen by the block-reconstruction state machine and all downstream customer,
            // we must send the block-meta message to the geyser loop, so that it be emitted to the block-reconstruction loop after all account update messages have been processed.
            inner.send_message(message.clone());
            inner.send_blocks_meta_message(message);

            Ok(())
        })
    }

    fn notify_deshred_transaction(
        &self,
        transaction: ReplicaDeshredTransactionInfoVersions,
        slot: u64,
    ) -> PluginResult<()> {
        self.with_inner(|inner| {
            let message = Message::DeshredTransaction(Arc::new(
                MessageDeshredTransaction::from_geyser_versioned(transaction, slot),
            ));
            inner.send_deshred_message(message);

            Ok(())
        })
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        false
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }

    fn deshred_transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn deshred_transaction_alt_resolution_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}

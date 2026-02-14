use {
    crate::{
        config::Config,
        grpc::GrpcService,
        metrics::{self, PrometheusService},
        parallel::ParallelEncoder,
        shm::{AccountFrameInput, PosixShmRingWriter},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoV3, ReplicaAccountInfoVersions,
        ReplicaBlockInfoVersions, ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions,
        Result as PluginResult, SlotStatus,
    },
    std::{
        collections::HashSet,
        concat, env,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        runtime::{Builder, Runtime},
        sync::mpsc,
    },
    tokio_util::{sync::CancellationToken, task::TaskTracker},
    yellowstone_grpc_proto::plugin::message::{
        Message, MessageAccount, MessageBlockMeta, MessageEntry, MessageSlot, MessageTransaction,
    },
};

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    snapshot_channel: Mutex<Option<crossbeam_channel::Sender<Box<Message>>>>,
    snapshot_channel_closed: AtomicBool,
    grpc_channel: mpsc::UnboundedSender<Message>,
    static_owner_allowlist: Option<HashSet<[u8; 32]>>,
    shm_writer: Option<Mutex<PosixShmRingWriter>>,
    shm_sequence: AtomicU64,
    /// Consecutive SHM write error count. Reset to 0 on success.
    shm_consecutive_errors: AtomicU64,
    shm_disable_grpc_accounts: bool,
    plugin_cancellation_token: CancellationToken,
    plugin_task_tracker: TaskTracker,
}

impl PluginInner {
    fn send_message(&self, message: Message) {
        if self.grpc_channel.send(message).is_ok() {
            metrics::message_queue_size_inc();
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

    /// Maximum consecutive SHM write errors before disabling writes for good.
    const SHM_MAX_CONSECUTIVE_ERRORS: u64 = 64;

    fn write_account_to_shm(
        &self,
        account: &ReplicaAccountInfoV3<'_>,
        slot: u64,
        is_startup: bool,
    ) {
        let Some(shm_writer) = &self.shm_writer else {
            return;
        };
        // Skip if too many consecutive errors have accumulated.
        if self.shm_consecutive_errors.load(Ordering::Relaxed) >= Self::SHM_MAX_CONSECUTIVE_ERRORS {
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
            }
            Err(error) => {
                self.record_shm_error(&format!("shm write failed: {error:?}"));
            }
        }
    }

    fn record_shm_error(&self, message: &str) {
        let prev = self.shm_consecutive_errors.fetch_add(1, Ordering::Relaxed);
        if prev == 0 {
            // Log only the first error in a streak to avoid log spam.
            log::error!("{message}");
        }
        if prev + 1 == Self::SHM_MAX_CONSECUTIVE_ERRORS {
            log::error!(
                "shm write disabled after {} consecutive errors",
                Self::SHM_MAX_CONSECUTIVE_ERRORS
            );
        }
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
                affinity::set_thread_affinity(&tokio_cpus).expect("failed to set affinity")
            });
        }
        let plugin_cancellation_token = CancellationToken::new();
        let plugin_task_tracker = TaskTracker::new();
        let prometheus_cancellation_token = plugin_cancellation_token.child_token();
        let prometheus_task_tracker = plugin_task_tracker.clone();
        let grpc_cancellation_token = plugin_cancellation_token.child_token();
        let grpc_task_tracker = plugin_task_tracker.clone();
        let parallel_encoder_threads = config
            .tokio
            .worker_threads
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|value| value.get())
                    .unwrap_or(1)
            })
            .max(1);
        let (parallel_encoder, _parallel_encoder_handle) =
            ParallelEncoder::new(parallel_encoder_threads);
        log::info!("parallel encoder enabled with {parallel_encoder_threads} threads");

        let runtime = builder
            .thread_name_fn(crate::get_thread_name)
            .enable_all()
            .build()
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

        let result = runtime.block_on(async move {
            let (debug_client_tx, debug_client_rx) = mpsc::unbounded_channel();
            // Create prometheus service First so if it fails the plugin doesn't spawn geyser tasks unnecessarily.
            PrometheusService::spawn(
                config.prometheus,
                config.debug_clients_http.then_some(debug_client_rx),
                prometheus_cancellation_token,
                prometheus_task_tracker,
            )
            .await
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

            let (snapshot_channel, grpc_channel) = GrpcService::create(
                config.grpc,
                config.debug_clients_http.then_some(debug_client_tx),
                is_reload,
                grpc_cancellation_token,
                grpc_task_tracker,
                parallel_encoder,
            )
            .await
            .map_err(|error| {
                GeyserPluginError::Custom(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("{error:?}"),
                )))
            })?;
            Ok::<_, GeyserPluginError>((snapshot_channel, grpc_channel))
        });

        let (snapshot_channel, grpc_channel) = result.inspect_err(|e| {
            log::error!("failed to start plugin services: {e}");
            plugin_cancellation_token.cancel();
        })?;

        self.inner = Some(PluginInner {
            runtime,
            snapshot_channel: Mutex::new(snapshot_channel),
            snapshot_channel_closed: AtomicBool::new(false),
            grpc_channel,
            static_owner_allowlist,
            shm_writer,
            shm_sequence: AtomicU64::new(0),
            shm_consecutive_errors: AtomicU64::new(0),
            shm_disable_grpc_accounts,
            plugin_cancellation_token,
            plugin_task_tracker,
        });

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            let number_of_tasks = inner.plugin_task_tracker.len();
            log::info!("shutting down plugin: {number_of_tasks} tasks to cancel.");
            inner.plugin_cancellation_token.cancel();
            inner.plugin_task_tracker.close();
            drop(inner.grpc_channel);
            const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
            let now = std::time::Instant::now();
            log::info!(
                "waiting up to {:?} for plugin tasks to shut down",
                SHUTDOWN_TIMEOUT
            );
            inner.runtime.shutdown_timeout(SHUTDOWN_TIMEOUT);
            log::info!("tokio runtime shut down in {:?}", now.elapsed());
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

            if is_startup {
                if let Some(channel) = inner.snapshot_channel.lock().unwrap().as_ref() {
                    let message =
                        Message::Account(MessageAccount::from_geyser(account, slot, is_startup));
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
                let message =
                    Message::Account(MessageAccount::from_geyser(account, slot, is_startup));
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
            let message = Message::Slot(MessageSlot::from_geyser(slot, parent, status));
            inner.send_message(message);
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

            let message = Message::Transaction(MessageTransaction::from_geyser(transaction, slot));
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
            inner.send_message(message);

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

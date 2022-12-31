use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_channel::Receiver as QueueReceiver;
use async_channel::Sender as QueueSender;
use hyper::Client;
use hyper_rustls::HttpsConnectorBuilder;
use tokio::sync::watch::{Receiver as WatchReceiver, Sender as WatchSender};
use tokio::task::JoinHandle;
use tokio::{sync::Semaphore, task::JoinSet};

use crate::client::HttpClient;
use crate::config::ForkliftConfig;
use crate::config::HTTPConfig;
use crate::config::OutputConfig;
use crate::scripting::ScriptManager;
use crate::warc::WarcRecord;
use crate::writer::RecordProcessor;
use crate::writer::WARCFileOutput;
use crate::ForkliftResult;
use crate::HttpJob;
use crate::TransferResponse;
use crate::UrlSource;

pub struct Pair<L, R> {
    pub tx: L,
    pub rx: R,
}

pub type QueuePair<T> = Pair<QueueSender<T>, QueueReceiver<T>>;

impl<T> QueuePair<T> {
    pub fn new(size: usize) -> QueuePair<T> {
        let (tx, rx) = async_channel::bounded(size);
        Pair { tx, rx }
    }
}

impl Pair<WatchSender<bool>, WatchReceiver<bool>> {
    pub fn new() -> Pair<WatchSender<bool>, WatchReceiver<bool>> {
        let (tx, rx) = tokio::sync::watch::channel(false);
        Pair { tx, rx }
    }
}

pub struct ChannelManager {
    pub url: QueuePair<UrlSource>,
    pub http_job: QueuePair<HttpJob>,
    pub response: QueuePair<TransferResponse>,
    pub record: QueuePair<TransferResponse>,
    pub script: QueuePair<TransferResponse>,
    pub script_close: Pair<WatchSender<bool>, WatchReceiver<bool>>,
}

impl ChannelManager {
    pub fn new(config: &ForkliftConfig) -> ChannelManager {
        let channel_size = config.http.workers * config.http.tasks_per_worker * 4;
        ChannelManager {
            url: QueuePair::new(1024 * 8),
            http_job: QueuePair::new(channel_size),
            response: QueuePair::new(channel_size),
            record: QueuePair::new(channel_size),
            script: QueuePair::new(channel_size),
            script_close: Pair::<WatchSender<bool>, WatchReceiver<bool>>::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.url.rx.is_empty()
            && self.response.rx.is_empty()
            && self.record.rx.is_empty()
            && self.script.rx.is_empty()
            && self.http_job.rx.is_empty()
    }
}

pub struct HttpRunner {
    total_permits: usize,
    pub tasks: JoinSet<ForkliftResult<()>>,
    pub semaphore: Arc<Semaphore>,
}

impl HttpRunner {
    pub fn spawn(
        config: &HTTPConfig,
        channels: &ChannelManager,
        seen_url_db: &sled::Tree,
    ) -> HttpRunner {
        let semaphore = Arc::new(Semaphore::new(0));
        let mut tasks = JoinSet::new();
        log::info!(
            "Starting {} http workers ({} tasks each)",
            config.workers,
            config.tasks_per_worker
        );

        for _ in 0..config.workers {
            let https_builder = HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1();

            let connector = if config.enable_http2 {
                https_builder.enable_http2().build()
            } else {
                https_builder.build()
            };

            let hyper_client = Client::builder().build::<_, hyper::Body>(connector);
            let (url_rx, http_job_rx, response_tx, record_tx) = (
                channels.url.rx.clone(),
                channels.http_job.rx.clone(),
                channels.response.tx.clone(),
                channels.record.tx.clone(),
            );

            let mut client = HttpClient::new(
                seen_url_db.clone(),
                hyper_client,
                channels.url.tx.clone(),
                config.headers.clone(),
                Arc::clone(&semaphore),
                config.tasks_per_worker,
                config.request_timeout,
            );

            tasks.spawn(async move {
                #[allow(unused_must_use)]
                loop {
                    tokio::select! {
                        Ok(job) = http_job_rx.recv() => {
                            let record_tx = record_tx.clone();
                            client.add_task(job.url, |resp| async move {
                                job.sender.send(resp.clone());
                                record_tx.send(resp).await;
                                Ok(())
                            }).await?
                        },
                        Ok(url) = url_rx.recv() => {
                            let response_tx = response_tx.clone();
                            client.add_task(url, |resp| async move { response_tx.send(resp).await; Ok(()) }).await?;
                        },
                        else => break,
                    }
                }
                ForkliftResult::Ok(())
            });
        }

        HttpRunner {
            total_permits: config.workers * config.tasks_per_worker,
            tasks,
            semaphore,
        }
    }

    pub fn is_idle(&self) -> bool {
        self.semaphore.available_permits() == self.total_permits
    }

    pub async fn join_all(&mut self) -> ForkliftResult<()> {
        while self.tasks.join_next().await.is_some() {}
        Ok(())
    }
}

#[repr(transparent)]
pub struct WriterRunner {
    tasks: Vec<JoinHandle<ForkliftResult<()>>>,
}

impl WriterRunner {
    pub fn spawn(
        config: &OutputConfig,
        channels: &ChannelManager,
        db: &sled::Db,
        output: Arc<std::sync::Mutex<WARCFileOutput>>,
    ) -> ForkliftResult<WriterRunner> {
        log::info!("Starting {} output workers", config.workers);

        let mut writer_tasks = Vec::with_capacity(config.workers);
        let mut writer = RecordProcessor::new(6, db.clone(), Arc::clone(&output))?;
        let record_rx = channels.record.rx.clone();

        writer_tasks.push(tokio::task::spawn_blocking(move || {
            while let Ok(response) = record_rx.recv_blocking() {
                writer.add_record(WarcRecord::from_response(
                    &response.parts,
                    response.body,
                    response.target_url.current_url.as_str(),
                )?)?;
            }
            ForkliftResult::Ok(())
        }));

        Ok(WriterRunner {
            tasks: writer_tasks,
        })
    }

    pub async fn join_all(&mut self) -> ForkliftResult<()> {
        futures::future::try_join_all(&mut self.tasks).await?;
        Ok(())
    }
}

pub struct ScriptRunner {
    total_permits: usize,
    semaphore: Arc<Semaphore>,
    rpc_counter: Arc<AtomicUsize>,
    tasks: JoinSet<ForkliftResult<()>>,
}

impl ScriptRunner {
    pub fn spawn(
        config: &ForkliftConfig,
        channels: &ChannelManager,
        db: &sled::Tree,
    ) -> ForkliftResult<ScriptRunner> {
        log::info!("Starting {} script workers", config.script_manager.workers);
        log::info!(
            "Selected scripts: {}",
            config
                .scripts
                .keys()
                .map(|v| v.as_str())
                .collect::<Vec<&str>>()
                .join(",")
        );

        let semaphore = Arc::new(Semaphore::new(config.script_manager.workers));
        let rpc_counter = Arc::new(AtomicUsize::new(0));
        let mut tasks = JoinSet::new();

        for _ in 0..config.script_manager.workers {
            let script_rx = channels.script.rx.clone();
            let script_semaphore = Arc::clone(&semaphore);

            let mut script_close_rx = channels.script_close.rx.clone();

            let mut script_manager = ScriptManager::new(
                1,
                channels.url.tx.clone(),
                channels.http_job.tx.clone(),
                db.clone(),
                config.crawl.base_url.clone(),
                Arc::clone(&rpc_counter),
            );

            for (_, script) in config.scripts.iter() {
                script_manager.script(script)?;
            }

            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        Ok(response) = script_rx.recv() => {
                            let permit = script_semaphore.acquire().await.unwrap();
                            script_manager.run(&response).await?;
                            drop(permit);
                        }
                        _ = script_close_rx.changed() => {
                            script_manager.close().await?;
                            break;
                        },
                        else => break
                    }
                }

                ForkliftResult::Ok(())
            });
        }

        Ok(ScriptRunner {
            total_permits: config.script_manager.workers,
            rpc_counter,
            semaphore,
            tasks,
        })
    }

    pub fn is_idle(&self) -> bool {
        self.semaphore.available_permits() == self.total_permits
            && self.rpc_counter.load(Ordering::Acquire) == 0
    }

    pub async fn join_all(&mut self) -> ForkliftResult<()> {
        while self.tasks.join_next().await.is_some() {}
        Ok(())
    }
}

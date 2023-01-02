use std::sync::Arc;

use async_channel::{Receiver as QueueReceiver, Sender as QueueSender};
use hyper::Client;
use hyper_rustls::HttpsConnectorBuilder;
use hyper_trust_dns::TrustDnsResolver;
use prodash::tree::Item as DashboardItem;

use tokio::sync::watch::{Receiver as WatchReceiver, Sender as WatchSender};
use tokio::task::JoinHandle;
use tokio::{sync::Semaphore, task::JoinSet};

use crate::client::HttpClient;
use crate::config::ForkliftConfig;
use crate::config::HTTPConfig;
use crate::config::OutputConfig;

use crate::warc::WarcRecord;
use crate::writer::RecordProcessor;
use crate::writer::WARCFileOutput;
use crate::ForkliftResult;
use crate::HttpJob;
use crate::HttpRateLimiter;
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
        let channel_size = config.http.workers * config.http.tasks_per_worker * 8;
        ChannelManager {
            url: QueuePair::new(1024 * 8),
            http_job: QueuePair::new(channel_size),
            response: QueuePair::new(channel_size),
            record: QueuePair::new(channel_size),
            script: QueuePair::new(1024 * 24),
            script_close: Pair::<WatchSender<bool>, WatchReceiver<bool>>::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.url.rx.is_empty()
            && self.response.rx.is_empty()
            && self.record.rx.is_empty()
            && self.script.tx.is_empty()
            && self.http_job.rx.is_empty()
    }
}

pub struct HttpRunner {
    total_permits: usize,
    pub tasks: JoinSet<ForkliftResult<()>>,
    pub semaphore: Arc<Semaphore>,
    _dashboard: DashboardItem,
}

impl HttpRunner {
    pub fn spawn(
        config: &HTTPConfig,
        channels: &ChannelManager,
        seen_url_db: &sled::Tree,
        mut dashboard: DashboardItem,
        rate_limiter: &Arc<HttpRateLimiter>,
    ) -> HttpRunner {
        let semaphore = Arc::new(Semaphore::new(0));
        let mut tasks = JoinSet::new();
        log::info!(
            "Starting {} http workers ({} tasks each)",
            config.workers,
            config.tasks_per_worker
        );

        for idx in 0..config.workers {
            let idx_u16 = idx.to_le_bytes();
            let id = [b'H', b'T', idx_u16[0], idx_u16[1]];
            let mut dash = dashboard.add_child_with_id(format!("HTTP {idx}"), id);
            dash.info("Setting up..");

            let (dns_config, mut dns_options) =
                trust_dns_resolver::system_conf::read_system_conf().unwrap_or_default();
            dns_options.cache_size = config.dns_cache_size;
            let mut resolver = TrustDnsResolver::with_config_and_options(dns_config, dns_options)
                .into_http_connector();
            resolver.enforce_http(false);

            let https_builder = HttpsConnectorBuilder::new()
                .with_native_roots()
                .https_or_http()
                .enable_http1();

            let connector = if config.enable_http2 {
                https_builder.enable_http2().wrap_connector(resolver)
            } else {
                https_builder.wrap_connector(resolver)
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
                dash,
                Arc::clone(&rate_limiter),
                config.rate_limiter.jitter,
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
            _dashboard: dashboard,
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

pub struct WriterRunner {
    tasks: Vec<JoinHandle<ForkliftResult<()>>>,
    _dashboard: DashboardItem,
}

impl WriterRunner {
    pub fn spawn(
        config: &OutputConfig,
        channels: &ChannelManager,
        db: &sled::Db,
        output: Arc<std::sync::Mutex<WARCFileOutput>>,
        mut dashboard: DashboardItem,
    ) -> ForkliftResult<WriterRunner> {
        let mut writer_tasks = Vec::with_capacity(config.workers);

        for idx in 0..config.workers {
            let mut task_logger = dashboard.add_child(format!("WRITER {idx}"));
            task_logger.init(
                None,
                Some(prodash::unit::dynamic_and_mode(
                    prodash::unit::Human::new(prodash::unit::human::Formatter::new(), "records"),
                    prodash::unit::display::Mode::with_throughput(),
                )),
            );

            task_logger.info("Starting up...");

            let mut writer = RecordProcessor::new(6, db.clone(), Arc::clone(&output))?;
            let record_rx = channels.record.rx.clone();

            writer_tasks.push(tokio::task::spawn_blocking(move || {
                while let Ok(response) = record_rx.recv_blocking() {
                    writer.add_record(WarcRecord::from_response(
                        &response.parts,
                        response.body,
                        response.target_url.current_url.as_str(),
                    )?)?;
                    task_logger.inc();
                }
                ForkliftResult::Ok(())
            }));
        }

        Ok(WriterRunner {
            tasks: writer_tasks,
            _dashboard: dashboard,
        })
    }

    pub async fn join_all(&mut self) -> ForkliftResult<()> {
        futures::future::try_join_all(&mut self.tasks).await?;
        Ok(())
    }
}

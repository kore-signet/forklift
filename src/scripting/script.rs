use async_channel::Sender as QueueSender;
use hyper::header::CONTENT_TYPE;
use neo_mime::{MediaRange, MediaType};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{
    collections::HashMap,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use unicode_truncate::UnicodeTruncateStr;

use prodash::tree::Item as DashboardItem;
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    process::{Child, ChildStdin, ChildStdout},
    sync::Mutex,
};
use url::Url;

use crate::{utils::SimplePermit, ForkliftResult, HttpJob, TransferResponse, UrlSource};

use super::protocol::{
    RpcMethod, RpcRequest, RpcResponse, ScriptInput, ScriptMessage, ScriptOutput,
};

pub type ScriptState = HashMap<String, Box<RawValue>>;

pub struct Script {
    pub(crate) max_hops: usize,
    pub(crate) filter: ScriptFilter,
    pub(crate) proc: Child,
    pub(crate) proc_in: Arc<Mutex<ScriptOutput<BufWriter<ChildStdin>>>>,
    pub(crate) proc_out: Arc<Mutex<ScriptInput<BufReader<ChildStdout>>>>,
    pub(crate) url_tx: QueueSender<UrlSource>,
    pub(crate) http_job_tx: QueueSender<HttpJob>,
    pub(crate) rpc_requests: Arc<AtomicUsize>,
    pub(crate) dashboard: DashboardItem,
}

impl Script {
    pub async fn run(
        &mut self,
        record: &TransferResponse,
        db: &sled::Tree,
        base_url: &Url,
    ) -> ForkliftResult<()> {
        if !self.filter.test(record) {
            return Ok(());
        }

        if record.body.is_empty() {
            return Ok(());
        }

        let mut logger = self.dashboard.add_child(
            format!(
                "{}{}",
                record.target_url.current_url.host_str().unwrap_or(""),
                record.target_url.current_url.path()
            )
            .unicode_truncate(60)
            .0,
        );
        logger.init(
            None,
            Some(prodash::unit::dynamic(prodash::unit::Human::new(
                prodash::unit::human::Formatter::new(),
                "urls found",
            ))),
        );

        let mut proc_in = self.proc_in.lock().await;
        proc_in
            .send_data(record.target_url.current_url.as_str(), &record.body)
            .await?;
        proc_in.flush().await?;
        drop(proc_in);

        let mut proc_out = self.proc_out.lock().await;
        loop {
            let v = proc_out.read().await?;
            match v {
                ScriptMessage::Url(v) => {
                    let url = if let Ok(url) = record.target_url.current_url.join(v) {
                        UrlSource::next(&record.target_url, base_url, url, false)
                    } else {
                        continue;
                    };

                    if db.insert(url.current_url.as_str(), &[])?.is_some() {
                        continue;
                    }

                    if url.hops_external > self.max_hops {
                        continue;
                    }

                    logger.inc();

                    self.url_tx.send(url).await.unwrap();
                }
                ScriptMessage::FileEnd => break,
                ScriptMessage::RpcRequest(req) => {
                    let mut proc_in = self.proc_in.lock().await;

                    match req.method {
                        RpcMethod::GetUrl => {
                            let proc_in_handle = Arc::clone(&self.proc_in);
                            self.send_get_url_job(req, &mut proc_in, proc_in_handle)
                                .await?;
                        }
                        RpcMethod::Unknown => {
                            proc_in
                                .send_rpc_response(RpcResponse::error(
                                    req.id,
                                    -32601,
                                    String::new(),
                                ))
                                .await?
                        }
                    }
                }
            }
        }

        logger.done(format!(
            "done scraping url {}",
            record.target_url.current_url.as_str()
        ));

        Ok(())
    }

    async fn send_get_url_job(
        &self,
        req: RpcRequest,
        proc_in: &mut ScriptOutput<BufWriter<ChildStdin>>,
        proc_in_handle: Arc<Mutex<ScriptOutput<BufWriter<ChildStdin>>>>,
    ) -> ForkliftResult<()> {
        let permit = SimplePermit::new(&self.rpc_requests);
        let (response_tx, response_rx) = tokio::sync::oneshot::channel::<TransferResponse>();

        let url = match req
            .params
            .first()
            .and_then(|v| serde_json::from_str::<&str>(v.get()).ok())
            .and_then(|v| Url::parse(v).ok())
        {
            Some(url) => UrlSource::start(url),
            None => {
                proc_in
                    .send_rpc_response(RpcResponse::error(req.id, -32602, String::new()))
                    .await?;
                return Ok(());
            }
        };

        self.http_job_tx
            .send(HttpJob {
                url,
                sender: response_tx,
            })
            .await
            .unwrap();

        // spawns a task that will wait for the request to be finished and send the response over

        tokio::task::spawn(async move {
            let permit = permit; // move permit into task explicitly
            let val = tokio::time::timeout(Duration::from_secs(60 * 5), response_rx).await;
            let mut proc_in = proc_in_handle.lock().await;
            match val {
                Ok(Ok(v)) => {
                    proc_in
                        .send_rpc_response(RpcResponse::ok(req.id, v.body))
                        .await?;
                    proc_in.flush().await?;
                    drop(permit);
                }
                _ => {
                    proc_in
                        .send_rpc_response(RpcResponse::error(req.id, -32603, String::new()))
                        .await?;
                    proc_in.flush().await?;
                    drop(permit);
                }
            };

            ForkliftResult::Ok(())
        });

        Ok(())
    }

    pub(crate) async fn close(mut self) -> ForkliftResult<()> {
        let mut proc_in = self.proc_in.lock().await;
        proc_in.send_close().await?;
        proc_in.flush().await?;
        self.proc.wait().await?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct ScriptFilter {
    #[serde(with = "serde_regex", default)]
    pub(crate) url_pattern: Option<Regex>,
    #[serde(default)]
    pub(crate) mime_types: Vec<MediaRange>,
}

impl ScriptFilter {
    pub fn test(&self, record: &TransferResponse) -> bool {
        let mime = record
            .parts
            .headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| MediaType::parse(v).ok());

        let mut check = if let Some(ty) = mime.filter(|_| !self.mime_types.is_empty()) {
            self.mime_types.iter().any(|v| v.matches(&ty))
        } else {
            true
        };

        if let Some(pat) = &self.url_pattern {
            check = check && pat.is_match(record.target_url.current_url.as_str());
        }

        check
    }
}

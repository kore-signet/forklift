use crate::{ForkliftError, ForkliftResult, HttpRateLimiter, TransferResponse, UrlSource};
use async_channel::Sender;

use futures::Future;
use governor::Jitter;
use hyper::{client::HttpConnector, header::LOCATION, Body, Client, HeaderMap, Request};
use hyper_rustls::HttpsConnector;
use hyper_trust_dns::TrustDnsResolver;
use prodash::{
    messages::MessageLevel,
    tree::Item as DashboardItem,
    unit::{display::Mode, human::Formatter},
};
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};
use url::Url;

type HttpsConn = HttpsConnector<HttpConnector<TrustDnsResolver>>;

pub struct HttpClient {
    inner: Arc<HttpClientInner>,
    tasks: JoinSet<Result<String, HttpFailure>>,
    semaphore: Arc<Semaphore>,
    task_max: usize,
    timeout: Duration,
    dashboard: DashboardItem,
    rate_limiter: Arc<HttpRateLimiter>,
    rate_jitter: Duration,
}

struct HttpFailure(String, HttpFailureReason);
impl Display for HttpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "couldn't get url {}: {}", self.0, self.1)
    }
}

enum HttpFailureReason {
    Fetch(ForkliftError),
    Callback(ForkliftError),
    Timeout,
}

impl Display for HttpFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpFailureReason::Fetch(e) => write!(f, "fetch failed ({})", e),
            HttpFailureReason::Callback(e) => write!(f, "result callback failed ({})", e),
            HttpFailureReason::Timeout => write!(f, "timed out"),
        }
    }
}

impl HttpClient {
    pub fn new(
        db: sled::Tree,
        client: Client<HttpsConn>,
        url_tx: Sender<UrlSource>,
        headers: HeaderMap,
        semaphore: Arc<Semaphore>,
        task_max: usize,
        timeout: Duration,
        mut dashboard: DashboardItem,
        rate_limiter: Arc<HttpRateLimiter>,
        rate_jitter: Duration,
    ) -> HttpClient {
        semaphore.add_permits(task_max);
        dashboard.init(
            None,
            Some(prodash::unit::dynamic_and_mode(
                prodash::unit::Human::new(Formatter::new(), "urls"),
                Mode::with_throughput(),
            )),
        );
        HttpClient {
            inner: Arc::new(HttpClientInner::new(db, client, url_tx, headers)),
            tasks: JoinSet::new(),
            semaphore,
            task_max,
            timeout,
            dashboard,
            rate_limiter,
            rate_jitter,
        }
    }

    pub async fn add_task<R, F>(&mut self, task: UrlSource, callback: F) -> ForkliftResult<()>
    where
        R: Future<Output = ForkliftResult<()>> + Send,
        F: FnOnce(TransferResponse) -> R + Send + 'static,
    {
        let permit = self.semaphore.clone().acquire_owned().await.unwrap();

        if self.tasks.len() == self.task_max {
            match self.tasks.join_next().await.transpose()? {
                Some(Ok(s)) => self
                    .dashboard
                    .message(MessageLevel::Success, format!("fetched {s}")),
                Some(Err(e)) => self
                    .dashboard
                    .message(MessageLevel::Failure, format!("{}", e)),
                _ => {}
            }
            self.dashboard.inc();
        }

        self.rate_limiter
            .until_ready_with_jitter(Jitter::up_to(self.rate_jitter))
            .await;

        let client = Arc::clone(&self.inner);
        let timeout = self.timeout;
        self.tasks.spawn(async move {
            let url = task.current_url.as_str().to_owned();
            match tokio::time::timeout(timeout, client.process_one(task)).await {
                Ok(v) => {
                    callback(
                        v.map_err(|e| HttpFailure(url.to_owned(), HttpFailureReason::Fetch(e)))?,
                    )
                    .await
                    .map_err(|e| HttpFailure(url.to_owned(), HttpFailureReason::Callback(e)))?;
                }
                Err(_) => return Err(HttpFailure(url, HttpFailureReason::Timeout)),
            };

            drop(permit);

            Ok(url.to_owned())
        });

        ForkliftResult::Ok(())
    }
}

pub struct HttpClientInner {
    db: sled::Tree,
    url_tx: Sender<UrlSource>,
    headers: HeaderMap,
    client: Client<HttpsConn>,
}

impl HttpClientInner {
    pub fn new(
        db: sled::Tree,
        client: Client<HttpsConn>,
        url_tx: Sender<UrlSource>,
        headers: HeaderMap,
    ) -> HttpClientInner {
        HttpClientInner {
            db,
            url_tx,
            headers,
            client,
        }
    }

    // this should only be run in a task!
    pub async fn process_one(&self, url: UrlSource) -> ForkliftResult<TransferResponse> {
        let uri_s = url.current_url.to_string();

        let mut request = Request::get(url.current_url.as_str()).body(Body::empty())?;
        let headers = request.headers_mut();
        headers.reserve(self.headers.len());

        for (header_name, header_value) in self.headers.iter() {
            headers.append(header_name.clone(), header_value.clone());
        }

        let res = self.client.request(request).await?;

        let next_uri = if res.status().is_redirection() {
            res.headers()
                .get(LOCATION)
                .and_then(|v| v.to_str().ok())
                .filter(|v| !self.db.contains_key(*v).unwrap())
                .and_then(|v| v.parse::<Url>().ok())
                .map(|new_url| UrlSource {
                    hops_external: url.hops_external,
                    redirect_hops: url.redirect_hops + 1,
                    current_url: new_url,
                    source_url: Some(url.current_url.clone()),
                })
        } else {
            None
        };

        self.db.insert(uri_s, &[])?;
        let resp = TransferResponse::from_response(res, url).await?;

        #[allow(unused_must_use)]
        if let Some(next_uri) = next_uri {
            self.url_tx.send(next_uri).await;
        }

        Ok(resp)
    }
}

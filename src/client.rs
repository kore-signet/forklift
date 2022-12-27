use crate::{ForkliftResult, TransferResponse, UrlSource};
use async_channel::Sender;

use hyper::{client::HttpConnector, header::LOCATION, Body, Client, HeaderMap, Request};
use hyper_tls::HttpsConnector;
use std::{sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};
use url::Url;

type HttpsConn = HttpsConnector<HttpConnector>;

pub struct HttpClient {
    inner: Arc<HttpClientInner>,
    tasks: JoinSet<ForkliftResult<()>>,
    semaphore: Arc<Semaphore>,
    task_max: usize,
    timeout: Duration,
}

impl HttpClient {
    pub fn new(
        db: sled::Tree,
        client: Client<HttpsConn>,
        url_tx: Sender<UrlSource>,
        record_tx: Sender<TransferResponse>,
        headers: HeaderMap,
        semaphore: Arc<Semaphore>,
        task_max: usize,
        timeout: Duration,
    ) -> HttpClient {
        semaphore.add_permits(task_max);
        HttpClient {
            inner: Arc::new(HttpClientInner::new(db, client, url_tx, record_tx, headers)),
            tasks: JoinSet::new(),
            semaphore,
            task_max,
            timeout,
        }
    }

    pub async fn add_task(&mut self, task: UrlSource) -> ForkliftResult<()> {
        if self.tasks.len() == self.task_max - 1 {
            self.tasks.join_next().await;
        }

        log::debug!(
            "getting {} | current tasks {}",
            task.current_url,
            self.tasks.len()
        );

        let permit = self.semaphore.clone().acquire_owned().await.unwrap();
        let client = Arc::clone(&self.inner);
        let timeout = self.timeout.clone();
        self.tasks.spawn(async move {
            match tokio::time::timeout(timeout, client.process_one(task.clone())).await {
                Ok(v) => v?,
                Err(_) => log::error!(
                    "url {} timed out after {:?}",
                    task.current_url.as_str(),
                    timeout
                ),
            }
            drop(permit);
            ForkliftResult::Ok(())
        });

        ForkliftResult::Ok(())
    }
}

pub struct HttpClientInner {
    db: sled::Tree,
    url_tx: Sender<UrlSource>,
    record_tx: Sender<TransferResponse>,
    headers: HeaderMap,
    client: Client<HttpsConn>,
}

impl HttpClientInner {
    pub fn new(
        db: sled::Tree,
        client: Client<HttpsConn>,
        url_tx: Sender<UrlSource>,
        record_tx: Sender<TransferResponse>,
        headers: HeaderMap,
    ) -> HttpClientInner {
        HttpClientInner {
            db,
            url_tx,
            record_tx,
            headers,
            client,
        }
    }

    // this should only be run in a task!
    pub async fn process_one(&self, uri: UrlSource) -> ForkliftResult<()> {
        let uri_s = uri.current_url.to_string();

        let mut request = Request::get(uri.current_url.as_str())
            .body(Body::empty())
            .unwrap();
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
                .map(|url| UrlSource {
                    hops_external: uri.hops_external,
                    redirect_hops: uri.redirect_hops + 1,
                    current_url: url,
                    source_url: Some(uri.current_url.clone()),
                })
        } else {
            None
        };

        self.db.insert(uri_s, &[])?;
        let resp = TransferResponse::from_response(res, uri).await?;

        #[allow(unused_must_use)]
        if let Some(next_uri) = next_uri {
            tokio::join![self.record_tx.send(resp), self.url_tx.send(next_uri)];
        } else {
            self.record_tx.send(resp).await;
        }

        Ok(())
    }
}

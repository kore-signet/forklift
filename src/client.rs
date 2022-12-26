use crate::{warc::WarcRecord, ForkliftResult};
use async_channel::{Receiver, Sender};
use hyper::{client::HttpConnector, header::LOCATION, Body, Client, HeaderMap, Request};
use hyper_tls::HttpsConnector;
use std::sync::Arc;
use tokio::sync::Semaphore;
use url::Url;

type HttpsConn = HttpsConnector<HttpConnector>;

const TASK_MAX: usize = 16;

pub struct HttpClient {
    db: sled::Db,
    url_rx: Receiver<Url>,
    url_tx: Sender<Url>,
    record_tx: Sender<WarcRecord>,
    headers: HeaderMap,
    client: Client<HttpsConn>,
    semaphore: Semaphore,
}

impl HttpClient {
    pub fn new(
        db: sled::Db,
        client: Client<HttpsConn>,
        url_tx: Sender<Url>,
        url_rx: Receiver<Url>,
        record_tx: Sender<WarcRecord>,
        headers: HeaderMap,
    ) -> HttpClient {
        HttpClient {
            db,
            url_rx,
            url_tx,
            record_tx,
            headers,
            client,
            semaphore: Semaphore::new(TASK_MAX),
        }
    }

    pub async fn run(self) -> ForkliftResult<()> {
        let arc_self = Arc::new(self);

        while let Some(next_uri) = arc_self.url_rx.recv().await.ok() {
            let arc = Arc::clone(&arc_self);
            tokio::task::spawn(async move {
                let permit = arc.semaphore.acquire().await.unwrap();
                (&arc).process_one(next_uri).await;
                drop(permit);
            });
        }

        Ok(())
    }

    // this should only be run in a task!
    pub async fn process_one(&self, uri: Url) -> ForkliftResult<()> {
        let uri_s = uri.to_string();
        let mut request = Request::get(uri.as_str()).body(Body::empty()).unwrap();
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
        } else {
            None
        };

        let record = WarcRecord::from_response(res, &uri_s).await?;

        if let Some(next_uri) = next_uri {
            tokio::join![self.record_tx.send(record), self.url_tx.send(next_uri)];
        } else {
            self.record_tx.send(record).await;
        }

        Ok(())
    }
}

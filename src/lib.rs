use std::sync::Arc;

use bytes::Bytes;
use hyper::{body::to_bytes, http::response::Parts, Body, Response};
use tokio::task::JoinError;
use url::Url;

pub mod client;
pub mod config;
pub mod scripting;
pub mod warc;
pub mod writer;

#[derive(thiserror::Error, Debug)]
pub enum ForkliftError {
    #[error("Body of response is too long")]
    BodyTooLong,
    #[error(transparent)]
    HyperError(#[from] hyper::Error),
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    SledError(#[from] sled::Error),
    #[error(transparent)]
    JoinError(#[from] JoinError),
    #[error(transparent)]
    Other(#[from] anyhow::Error), // source and Display delegate to anyhow::Error
}

pub type ForkliftResult<T> = Result<T, ForkliftError>;

#[derive(Clone)]
pub struct TransferResponse {
    pub parts: Arc<Parts>,
    pub body: Bytes,
    pub target_url: UrlSource,
}

impl TransferResponse {
    pub(crate) async fn from_response(
        res: Response<Body>,
        target_url: UrlSource,
    ) -> ForkliftResult<TransferResponse> {
        let (parts, body) = res.into_parts();
        let body = to_bytes(body).await?;

        Ok(TransferResponse {
            parts: Arc::new(parts),
            body,
            target_url,
        })
    }
}

#[derive(Clone, Debug)]
pub struct UrlSource {
    pub hops_external: usize,
    pub redirect_hops: usize,
    pub current_url: Url,
    pub source_url: Option<Url>,
}

impl UrlSource {
    pub fn start(url: Url) -> UrlSource {
        UrlSource {
            hops_external: 0,
            redirect_hops: 0,
            current_url: url,
            source_url: None,
        }
    }

    pub fn next(prev: &UrlSource, base: &Url, next: Url, subdomains_count: bool) -> UrlSource {
        let hopped = if let Some(url_domain) = base.domain() {
            if subdomains_count {
                next.domain().map(|v| v != url_domain).unwrap_or(false)
            } else {
                addr::parse_domain_name(url_domain)
                    .ok()
                    .zip(next.domain().and_then(|v| addr::parse_domain_name(v).ok()))
                    .map(|(a, b)| a.root() != b.root())
                    .unwrap_or(false)
            }
        } else {
            false
        };

        UrlSource {
            hops_external: if hopped {
                prev.hops_external + 1
            } else {
                prev.hops_external
            },
            redirect_hops: prev.redirect_hops,
            current_url: next,
            source_url: Some(prev.current_url.clone()),
        }
    }
}

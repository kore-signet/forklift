pub mod client;
pub mod processor;
pub mod warc;

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
    Other(#[from] anyhow::Error), // source and Display delegate to anyhow::Error
}

pub type ForkliftResult<T> = Result<T, ForkliftError>;

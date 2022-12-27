use async_channel::Sender;
use futures::{stream::TryStreamExt, StreamExt};
use hyper::header::CONTENT_TYPE;
use neo_mime::{MediaRange, MediaType};
use regex::Regex;
use serde::Deserialize;
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    process::{Child, ChildStdin, ChildStdout, Command},
};
use url::Url;

use crate::{ForkliftResult, TransferResponse, UrlSource};

pub struct Script {
    max_hops: usize,
    filter: ScriptFilter,
    proc: Child,
    proc_in: BufWriter<ChildStdin>,
    proc_out: BufReader<ChildStdout>,
    url_tx: Sender<UrlSource>,
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

        if record.body.len() == 0 {
            return Ok(());
        }

        self.proc_in.write_u64_le(record.body.len() as u64).await?;
        self.proc_in.write_all(&record.body).await?;
        self.proc_in.flush().await?;

        let mut url_buffer = String::with_capacity(128);

        loop {
            url_buffer.clear();
            let url_bytes = self.proc_out.read_line(&mut url_buffer).await?;
            if url_bytes <= 1 {
                break;
            }

            url_buffer.truncate(url_buffer.len() - 1);

            let url = if let Some(url) = record.target_url.current_url.join(&url_buffer).ok() {
                UrlSource::next(&record.target_url, &base_url, url, false)
            } else {
                continue;
            };

            if let Some(_) = db.insert(url.current_url.as_str(), &[])? {
                continue;
            }

            if url.hops_external > self.max_hops {
                continue;
            }

            self.url_tx.send(url).await.unwrap();
        }

        Ok(())
    }

    async fn close(mut self) -> ForkliftResult<()> {
        self.proc_in.write_u64_le(0).await?;
        self.proc_in.flush().await?;
        self.proc.wait().await?;
        Ok(())
    }
}

pub struct ScriptManager {
    base_url: Url,
    scripts: Vec<Script>,
    max_hops: usize,
    url_tx: Sender<UrlSource>,
    db: sled::Tree,
}

impl ScriptManager {
    pub fn new(
        max_hops: usize,
        url_tx: Sender<UrlSource>,
        db: sled::Tree,
        base_url: Url,
    ) -> ScriptManager {
        ScriptManager {
            scripts: Vec::new(),
            max_hops,
            url_tx,
            db,
            base_url,
        }
    }

    pub async fn close(self) -> ForkliftResult<()> {
        #[allow(unused_must_use)]
        for script in self.scripts {
            script.close().await;
        }

        Ok(())
    }

    pub fn script(&mut self, cfg: &ScriptConfig) -> ForkliftResult<()> {
        let mut proc = Command::new(&cfg.command)
            .args(&cfg.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;
        let proc_in = BufWriter::new(proc.stdin.take().unwrap());
        let proc_out = BufReader::new(proc.stdout.take().unwrap());
        self.scripts.push(Script {
            max_hops: self.max_hops,
            url_tx: self.url_tx.clone(),
            filter: cfg.filter.clone(),
            proc_in,
            proc_out,
            proc,
        });

        Ok(())
    }

    pub async fn run(&mut self, record: &TransferResponse) -> ForkliftResult<()> {
        futures::stream::iter(self.scripts.iter_mut())
            .map(Ok)
            .try_for_each_concurrent(4, |script| async {
                script.run(record, &self.db, &self.base_url).await
            })
            .await
    }
}

#[derive(Deserialize, Default, Clone)]
pub struct ScriptFilter {
    #[serde(with = "serde_regex", default)]
    url_pattern: Option<Regex>,
    #[serde(default)]
    mime_types: Vec<MediaRange>,
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

#[derive(Deserialize)]
pub struct ScriptConfig {
    #[serde(default)]
    filter: ScriptFilter,
    command: String,
    args: Vec<String>,
}

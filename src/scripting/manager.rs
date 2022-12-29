use async_channel::Sender;
use futures::{stream::TryStreamExt, StreamExt};

use std::{process::Stdio, sync::Arc};
use tokio::{
    io::{BufReader, BufWriter},
    process::Command,
    sync::Mutex,
};
use url::Url;

use crate::{ForkliftResult, HttpJob, TransferResponse, UrlSource};

use super::{
    protocol::{ScriptInput, ScriptOutput},
    Script, ScriptConfig,
};

pub struct ScriptManager {
    base_url: Url,
    scripts: Vec<Script>,
    max_hops: usize,
    url_tx: Sender<UrlSource>,
    http_job_tx: Sender<HttpJob>,
    db: sled::Tree,
}

impl ScriptManager {
    pub fn new(
        max_hops: usize,
        url_tx: Sender<UrlSource>,
        http_job_tx: Sender<HttpJob>,
        db: sled::Tree,
        base_url: Url,
    ) -> ScriptManager {
        ScriptManager {
            scripts: Vec::new(),
            max_hops,
            url_tx,
            http_job_tx,
            db,
            base_url,
        }
    }

    pub async fn close(&mut self) -> ForkliftResult<()> {
        #[allow(unused_must_use)]
        for script in self.scripts.drain(..) {
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
            http_job_tx: self.http_job_tx.clone(),
            filter: cfg.filter.clone(),
            proc_in: Arc::new(Mutex::new(ScriptOutput::new(proc_in))),
            proc_out: Arc::new(Mutex::new(ScriptInput::new(proc_out))),
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

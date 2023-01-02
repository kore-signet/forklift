use async_channel::Receiver;
use async_channel::Sender;

use prodash::tree::Item as DashboardItem;
use std::{
    process::Stdio,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    io::{BufReader, BufWriter},
    process::Command,
    sync::{Mutex, Semaphore},
    task::JoinSet,
};
use url::Url;

use crate::{
    config::{ForkliftConfig, ScriptConfig},
    runner::ChannelManager,
    ForkliftResult, TransferResponse,
};

use super::{
    protocol::{ScriptInput, ScriptOutput},
    Script, ScriptFilter,
};

pub struct ScriptRunner {
    db: sled::Tree,
    max_hops: usize,
    base_url: Url,
    total_permits: usize,
    semaphore: Arc<Semaphore>,
    rpc_counter: Arc<AtomicUsize>,
    tasks: JoinSet<ForkliftResult<()>>,
    dashboard: DashboardItem,
}

impl ScriptRunner {
    pub fn spawn(
        config: &ForkliftConfig,
        channels: &ChannelManager,
        db: &sled::Tree,
        dashboard: DashboardItem,
    ) -> ForkliftResult<ScriptRunner> {
        let total_permits = config.scripts.values().map(|v| v.workers).sum();
        let semaphore = Arc::new(Semaphore::new(total_permits));
        let rpc_counter = Arc::new(AtomicUsize::new(0));
        let tasks = JoinSet::new();
        let mut script_queues: Vec<(ScriptFilter, Sender<TransferResponse>)> = Vec::new();

        let mut manager = ScriptRunner {
            max_hops: config.crawl.max_hops,
            total_permits,
            semaphore,
            rpc_counter,
            tasks,
            dashboard,
            base_url: config.crawl.base_url.clone(),
            db: db.clone(),
        };

        for (name, script) in &config.scripts {
            let (script_tx, script_rx) = async_channel::bounded::<TransferResponse>(1024);

            manager.dashboard.info(format!("launching script {name}"));
            for idx in 0..script.workers {
                manager.script(idx, script, channels, script_rx.clone())?;
            }

            script_queues.push((script.filter.clone(), script_tx));
        }

        let script_rx = channels.script.rx.clone();
        let mut script_close_rx = channels.script_close.rx.clone();
        manager.tasks.spawn(async move {
            loop {
                tokio::select! {
                    Ok(response) = script_rx.recv() => {
                        #[allow(unused_must_use)]
                        for (_, send) in script_queues.iter().filter(|(filter, _)| filter.test(&response)) {
                            send.send(response.clone()).await;
                        }
                    }
                    _ = script_close_rx.changed() => {
                        break;
                    },
                    else => break
                }
            }

            ForkliftResult::Ok(())
        });

        Ok(manager)
    }

    pub fn script(
        &mut self,
        idx: usize,
        cfg: &ScriptConfig,
        channels: &ChannelManager,
        script_queue: Receiver<TransferResponse>,
    ) -> ForkliftResult<()> {
        let mut proc = Command::new(&cfg.command)
            .args(&cfg.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()?;

        let proc_in = BufWriter::new(proc.stdin.take().unwrap());
        let proc_out = BufReader::new(proc.stdout.take().unwrap());

        let mut script = Script {
            dashboard: self.dashboard.add_child(format!(
                "inst.{} {} {}",
                idx + 1,
                &cfg.command,
                &cfg.args.join(" ")
            )),
            max_hops: self.max_hops,
            url_tx: channels.url.tx.clone(),
            http_job_tx: channels.http_job.tx.clone(),
            filter: cfg.filter.clone(),
            proc_in: Arc::new(Mutex::new(ScriptOutput::new(proc_in))),
            proc_out: Arc::new(Mutex::new(ScriptInput::new(proc_out))),
            rpc_requests: Arc::clone(&self.rpc_counter),
            proc,
        };

        let mut script_close_rx = channels.script_close.rx.clone();

        let script_semaphore = self.semaphore.clone();
        let db = self.db.clone();
        let base_url = self.base_url.clone();

        self.tasks.spawn(async move {
            loop {
                tokio::select! {
                    Ok(response) = script_queue.recv() => {
                        let permit = script_semaphore.acquire().await.unwrap();
                        if script.filter.test(&response) {
                            script.run(&response, &db, &base_url).await?;
                        }

                        drop(permit);
                    }
                    _ = script_close_rx.changed() => {
                        script.close().await?;
                        break;
                    },
                    else => break
                }
            }

            ForkliftResult::Ok(())
        });

        Ok(())
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

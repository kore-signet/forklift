use std::{
    error::Error,
    fs::File,
    io::BufWriter,
    sync::{Arc, Mutex},
};

use forklift::{
    client::HttpClient, config::CrawlConfig, scripting::ScriptManager, warc::WarcRecord,
    writer::RecordProcessor, ForkliftError, ForkliftResult, TransferResponse, UrlSource,
};
use futures::TryFutureExt;
use hyper::{Client, HeaderMap};
use hyper_tls::HttpsConnector;

use std::time::Duration;
use tokio::{sync::Semaphore, task::JoinSet};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db: sled::Db = sled::open("idx")?;
    db.clear()?;
    let seen_url_db = db.open_tree("seen_urls")?;
    seen_url_db.clear()?;

    let (url_tx, url_rx) = async_channel::bounded::<UrlSource>(1024 * 8);
    let (response_tx, response_rx) = async_channel::bounded::<TransferResponse>(256);
    let (record_tx, record_rx) = async_channel::bounded::<TransferResponse>(256);
    let (script_tx, script_rx) = async_channel::bounded::<TransferResponse>(256);

    let config: CrawlConfig = toml::from_str(include_str!("../crawl.toml")).unwrap();

    let out = Arc::new(Mutex::new(BufWriter::new(File::create("nya.warc.gz")?)));

    let http_semaphore = Arc::new(Semaphore::new(0));
    let script_semaphore = Arc::new(Semaphore::new(config.script_tasks));

    let mut http_tasks = JoinSet::new();
    let mut writer_tasks = Vec::with_capacity(config.writer_tasks);
    let mut script_tasks = JoinSet::new();

    for _ in 0..config.http_workers {
        let hyper_client = Client::builder().build::<_, hyper::Body>(HttpsConnector::new());
        let url_rx = url_rx.clone();

        let mut client = HttpClient::new(
            seen_url_db.clone(),
            hyper_client,
            url_tx.clone(),
            response_tx.clone(),
            HeaderMap::new(),
            Arc::clone(&http_semaphore),
            config.tasks_per_http_worker,
        );

        http_tasks.spawn(async move {
            while let Some(url) = url_rx.recv().await.ok() {
                client.add_task(url).await?;
            }

            ForkliftResult::Ok(())
        });
    }

    let http_task_count = config.http_workers * config.tasks_per_http_worker;

    let links = include_str!("../cat-girl.txt").lines();

    let _splitter_task = {
        let script_tx = script_tx.clone();
        let _response_tx = response_tx.clone();
        tokio::task::spawn(async move {
            #[allow(unused_must_use)]
            while let Some(lhs) = response_rx.recv().await.ok() {
                let rhs = lhs.clone();
                let script_tx = script_tx.clone();
                let record_tx = record_tx.clone();

                tokio::task::spawn(async move {
                    script_tx.send(rhs).await;
                });

                tokio::task::spawn(async move {
                    record_tx.send(lhs).await;
                });
            }

            ForkliftResult::Ok(())
        });
    };

    for _ in 0..config.writer_tasks {
        let mut writer = RecordProcessor::new(6, db.clone(), Arc::clone(&out))?;
        let record_rx = record_rx.clone();

        writer_tasks.push(tokio::task::spawn_blocking(move || {
            while let Some(response) = record_rx.recv_blocking().ok() {
                writer.add_record(WarcRecord::from_response(
                    &response.parts,
                    response.body,
                    response.target_url.current_url.as_str(),
                )?)?;
            }
            ForkliftResult::Ok(())
        }));
    }

    for _ in 0..config.script_tasks {
        let script_rx = script_rx.clone();
        let mut script_manager = ScriptManager::new(
            1,
            url_tx.clone(),
            seen_url_db.clone(),
            config.base_url.clone(),
        );
        let script_semaphore = Arc::clone(&script_semaphore);

        for (k, script) in config.scripts.iter() {
            println!("starting script {k}");
            script_manager.script(&script)?;
        }

        script_tasks.spawn(async move {
            while let Some(response) = script_rx.recv().await.ok() {
                let permit = script_semaphore.acquire().await.unwrap();
                script_manager.run(&response).await?;
                drop(permit);
            }

            ForkliftResult::Ok(())
        });
    }

    drop(script_rx);

    let start = std::time::Instant::now();

    for link in links {
        url_tx
            .send(UrlSource::start(link.trim_end().parse::<Url>().unwrap()))
            .await
            .unwrap();
    }

    let mut checker_interval = tokio::time::interval(Duration::from_millis(100));
    checker_interval.tick().await;

    loop {
        checker_interval.tick().await;
        // dbg!(script_semaphore.available_permits());

        if http_semaphore.available_permits() == http_task_count
            && script_semaphore.available_permits() == config.script_tasks
            && url_rx.is_empty()
            && response_tx.is_empty()
            && record_rx.is_empty()
            && script_tx.is_empty()
        {
            break;
        }
    }

    url_tx.close();

    while let Some(_) = http_tasks.join_next().await.transpose()? {}

    response_tx.close();
    record_rx.close();

    futures::future::try_join_all(writer_tasks)
        .map_err(ForkliftError::JoinError)
        .await?;

    script_tx.close();

    script_tasks.shutdown().await;

    println!("{:?}", start.elapsed());

    Ok(())
}

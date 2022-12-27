use std::sync::{Arc, Mutex};

use forklift::{
    client::HttpClient,
    config::ForkliftConfig,
    scripting::ScriptManager,
    warc::WarcRecord,
    writer::{RecordProcessor, WARCFileOutput},
    ForkliftError, ForkliftResult, TransferResponse, UrlSource,
};
use futures::TryFutureExt;
use hyper::{Client, HeaderMap};
use hyper_tls::HttpsConnector;

use std::time::Duration;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::Semaphore,
    task::JoinSet,
};
use url::Url;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();

    let mut config: ForkliftConfig =
        toml::from_str(&std::fs::read_to_string(std::env::args().skip(1).next().unwrap()).unwrap())
            .unwrap();

    if config.scripts.is_empty() {
        config.script_manager.workers = 0;
    }

    let db: sled::Db = config.index.into_db(config.folder.join("idx"))?;
    let seen_url_db = db.open_tree("seen_urls")?;

    if config.index.overwrite {
        seen_url_db.clear()?;
    }

    let (url_tx, url_rx) = async_channel::bounded::<UrlSource>(1024 * 8);
    let (response_tx, response_rx) = async_channel::bounded::<TransferResponse>(256);
    let (record_tx, record_rx) = async_channel::bounded::<TransferResponse>(256);
    let (script_tx, script_rx) = async_channel::bounded::<TransferResponse>(256);

    let mut output_folder = config.folder.join("warcs");
    tokio::fs::create_dir_all(&output_folder).await?;
    output_folder = tokio::fs::canonicalize(output_folder).await?;

    let out = Arc::new(Mutex::new(WARCFileOutput::new(
        output_folder,
        config.output.file_prefix,
        config.output.file_size,
    )?));

    let http_semaphore = Arc::new(Semaphore::new(0));
    let script_semaphore = Arc::new(Semaphore::new(config.script_manager.workers));

    let mut http_tasks = JoinSet::new();
    let mut writer_tasks = Vec::with_capacity(config.output.workers);
    let mut script_tasks = JoinSet::new();

    for _ in 0..config.http.workers {
        let hyper_client = Client::builder().build::<_, hyper::Body>(HttpsConnector::new());
        let url_rx = url_rx.clone();

        let mut client = HttpClient::new(
            seen_url_db.clone(),
            hyper_client,
            url_tx.clone(),
            response_tx.clone(),
            HeaderMap::new(),
            Arc::clone(&http_semaphore),
            config.http.tasks_per_worker,
            config.http.request_timeout,
        );

        http_tasks.spawn(async move {
            while let Some(url) = url_rx.recv().await.ok() {
                client.add_task(url).await.unwrap();
            }

            ForkliftResult::Ok(())
        });
    }

    let http_task_count = config.http.workers * config.http.tasks_per_worker;

    let _splitter_task = {
        let script_tx = script_tx.clone();
        let _response_tx = response_tx.clone();
        tokio::task::spawn(async move {
            #[allow(unused_must_use)]
            while let Some(lhs) = response_rx.recv().await.ok() {
                let rhs = lhs.clone();
                let script_tx = script_tx.clone();
                let record_tx = record_tx.clone();

                if config.http.tasks_per_worker != 0 {
                    tokio::task::spawn(async move {
                        script_tx.send(rhs).await;
                    });
                }

                tokio::task::spawn(async move {
                    record_tx.send(lhs).await;
                });
            }

            ForkliftResult::Ok(())
        });
    };

    for _ in 0..config.output.workers {
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

    for _ in 0..config.script_manager.workers {
        let script_rx = script_rx.clone();
        let mut script_manager = ScriptManager::new(
            1,
            url_tx.clone(),
            seen_url_db.clone(),
            config.crawl.base_url.clone(),
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

    if let Some(url_file) = config.crawl.urls_file {
        let mut lines = BufReader::new(tokio::fs::File::open(&url_file).await?).lines();
        let url_tx = url_tx.clone();
        while let Some(line) = lines
            .next_line()
            .await?
            .and_then(|url| url.trim_end().parse::<Url>().ok())
        {
            url_tx.send(UrlSource::start(line)).await.unwrap();
        }
    } else {
        url_tx
            .send(UrlSource::start(config.crawl.base_url))
            .await
            .unwrap();
    };

    let mut checker_interval = tokio::time::interval(Duration::from_millis(100));
    checker_interval.tick().await;

    loop {
        checker_interval.tick().await;
        // dbg!(script_semaphore.available_permits());

        if http_semaphore.available_permits() == http_task_count
            && script_semaphore.available_permits() == config.script_manager.workers
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

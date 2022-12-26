use std::{
    error::Error,
    fs::File,
    io::BufWriter,
    sync::{Arc, Mutex},
};

use forklift::{client::HttpClient, processor::RecordProcessor, warc::WarcRecord, ForkliftResult};
use hyper::{Client, HeaderMap};
use hyper_tls::HttpsConnector;
use url::Url;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let db: sled::Db = sled::open("idx")?;
    let (url_tx, url_rx) = async_channel::bounded::<Url>(1024 * 8);
    let (record_tx, record_rx) = async_channel::bounded::<WarcRecord>(256);

    let out = Arc::new(Mutex::new(BufWriter::new(File::create("nya.warc.gz")?)));

    let mut tasks = Vec::new();

    for _ in 0..4 {
        let hyper_client = Client::builder().build::<_, hyper::Body>(HttpsConnector::new());

        let client = HttpClient::new(
            db.clone(),
            hyper_client,
            url_tx.clone(),
            url_rx.clone(),
            record_tx.clone(),
            HeaderMap::new(),
        );

        tasks.push(tokio::task::spawn(async move {
            client.run().await?;
            ForkliftResult::Ok(())
        }));
    }

    let links = include_str!("../100k.txt").lines();

    for _ in 0..2 {
        let mut writer = RecordProcessor::new(
            6,
            db.clone(),
            record_rx.clone(),
            url_tx.clone(),
            Arc::clone(&out),
        );
        tasks.push(tokio::task::spawn_blocking(move || {
            writer.run()?;
            ForkliftResult::Ok(())
        }));
    }

    let start = std::time::Instant::now();

    for link in links {
        url_tx.send(link.trim_end().parse::<Url>().unwrap()).await;
    }

    url_tx.close();
    drop(url_rx);
    drop(record_tx);

    futures::future::try_join_all(tasks).await?;

    println!("{:?}", start.elapsed());

    Ok(())
}

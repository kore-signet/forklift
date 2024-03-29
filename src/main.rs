use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use clap::Parser;
use forklift::{
    config::ForkliftConfig,
    runner::{ChannelManager, HttpRunner, WriterRunner},
    scripting::ScriptRunner,
    writer::WARCFileOutput,
    ForkliftResult, HttpRateLimiter, UrlSource,
};

use prodash::Root;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use url::Url;

#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: String,
    #[arg(short, long)]
    base_url: Option<String>,
    #[arg(short, long)]
    output_folder: String,
    #[arg(short, long)]
    file_prefix: Option<String>,
}

#[tokio::main]
async fn main() {
    inner_main().await.unwrap()
}

async fn inner_main() -> anyhow::Result<()> {
    // pretty_env_logger::init();
    let args = Args::parse();

    let dashboard_root = prodash::tree::Root::new();

    let dashboard_render = prodash::render::tui::render(
        std::io::stdout(),
        dashboard_root.downgrade(),
        prodash::render::tui::Options {
            title: "forklift crawl".into(),
            stop_if_progress_missing: false,
            throughput: true,
            ..Default::default()
        },
    )?;
    let (render_fut, _render_abort) = futures::future::abortable(dashboard_render);
    tokio::task::spawn(async move {
        render_fut.await.unwrap();
    });

    let mut dashboard_total = dashboard_root.add_child("TOTAL");
    dashboard_total.init(
        None,
        Some(prodash::unit::dynamic_and_mode(
            prodash::unit::Human::new(prodash::unit::human::Formatter::new(), "records"),
            prodash::unit::display::Mode::with_throughput(),
        )),
    );

    let mut config: ForkliftConfig =
        toml::from_str(&tokio::fs::read_to_string(args.config).await?).unwrap();

    if let Some(base_url) = args.base_url {
        config.crawl.base_url = Url::parse(&base_url).unwrap();
    }

    if let Some(file_prefix) = args.file_prefix {
        config.output.file_prefix = file_prefix;
    }

    config.folder = PathBuf::from(args.output_folder);

    let channels = ChannelManager::new(&config);

    let mut output_folder = config.folder.join("warcs");
    tokio::fs::create_dir_all(&output_folder).await?;
    output_folder = tokio::fs::canonicalize(output_folder).await?;

    tokio::fs::write(
        config.folder.join("config.json"),
        serde_json::to_vec(&config)?,
    )
    .await?;

    let db: sled::Db = config.index.into_db(config.folder.join("idx"))?;
    let seen_url_db = db.open_tree("seen_urls")?;

    if config.index.overwrite {
        seen_url_db.clear()?;
    }

    let output = Arc::new(Mutex::new(WARCFileOutput::new(
        output_folder,
        &config.output.file_prefix,
        config.output.file_size,
    )?));

    let rate_limiter = Arc::new(HttpRateLimiter::direct(config.http.rate_limiter.as_quota()));

    let mut http_runner = HttpRunner::spawn(
        &config.http,
        &channels,
        &seen_url_db,
        dashboard_root.add_child("HTTP"),
        &rate_limiter,
    );

    let mut writer_runner = WriterRunner::spawn(
        &config.output,
        &channels,
        &db,
        output,
        dashboard_root.add_child("OUTPUT"),
    )?;
    let mut script_runner = ScriptRunner::spawn(
        &config,
        &channels,
        &seen_url_db,
        dashboard_root.add_child("SCRIPTING"),
    )?;

    let has_scripts = !config.scripts.is_empty();

    {
        let script_tx = channels.script.tx.clone();
        let response_rx = channels.response.rx.clone();
        let record_tx = channels.record.tx.clone();
        let mut queue_dash = dashboard_root.add_child("RECORD QUEUE");

        tokio::task::spawn(async move {
            #[allow(unused_must_use)]
            while let Ok(lhs) = response_rx.recv().await {
                dashboard_total.inc();

                let rhs = lhs.clone();
                let script_tx = script_tx.clone();
                let record_tx = record_tx.clone();

                if has_scripts {
                    queue_dash.blocked("sending to script queue", None);
                    script_tx.send(rhs).await;
                    queue_dash.running();
                }

                tokio::task::spawn(async move {
                    record_tx.send(lhs).await;
                });
            }

            ForkliftResult::Ok(())
        });
    };

    let start = std::time::Instant::now();

    if let Some(url_file) = config.crawl.urls_file {
        let mut lines = BufReader::new(tokio::fs::File::open(&url_file).await?).lines();
        while let Some(line) = lines
            .next_line()
            .await?
            .and_then(|url| url.trim_end().parse::<Url>().ok())
        {
            channels.url.tx.send(UrlSource::start(line)).await.unwrap();
        }
    } else {
        channels
            .url
            .tx
            .send(UrlSource::start(config.crawl.base_url))
            .await
            .unwrap();
    };

    let mut checker_interval = tokio::time::interval(Duration::from_millis(100));
    checker_interval.tick().await;

    loop {
        checker_interval.tick().await;

        if channels.is_empty() && http_runner.is_idle() && script_runner.is_idle() {
            break;
        }
    }

    dbg!("closing.?");

    channels.url.tx.close();
    channels.http_job.tx.close();

    http_runner.join_all().await?;

    channels.response.tx.close();
    channels.record.rx.close();

    writer_runner.join_all().await?;

    channels.script.tx.close();

    #[allow(unused_must_use)]
    {
        channels.script_close.tx.send(true);
    };

    drop(channels);

    script_runner.join_all().await?;

    // render_abort.abort();

    println!("Crawl done in {:?}!", start.elapsed());

    Ok(())
}

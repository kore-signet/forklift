use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Deserializer};
use url::Url;

use crate::scripting::ScriptConfig;

macro_rules! default_vals {
    ($($mod_name:ident {
        $($name:ident: $t:ty = $val:expr);* $(;)*
    });* $(;)*) => {
        $(
            mod $mod_name {
                $(
                    default_vals!($name,$t,$val);
                )*
            }
        )*
    };
    ($($name:ident: $t:ty = $val:expr);* $(;)*) => {
        $(
             default_vals!($name,$t,$val);
        )*
    };
    ($name:ident,$t:ty,$val:expr) => {
        pub(super) fn $name() -> $t {
            $val
        }
    };
}

default_vals! {
    base {
        folder: std::path::PathBuf = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap().join("crawl/idx");
    };
    http {
        workers: usize = 4;
        tasks_per_worker: usize = 16;
        request_timeout: std::time::Duration = std::time::Duration::from_secs(60 * 20);
    };
    output {
        workers: usize = 4;
        prefix: String = "forklift_crawl".to_owned();
        file_size: u64 = 2000000000; // 2GB
    };
    script_manager {
        workers: usize = 2;
    };
    crawl {
        base_url: url::Url = url::Url::parse("http://forklift.local").unwrap();
    };
    index {
        compression: bool = true;
        compression_level: i32 = 5;
        overwrite: bool = true;
    };
}

fn deserialize_byte_size<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = bytesize_serde::deserialize(deserializer)?;
    Ok(bytes.as_u64())
}

#[derive(Deserialize)]
pub struct ForkliftConfig {
    #[serde(default = "base::folder")]
    pub folder: PathBuf,
    #[serde(default)]
    pub scripts: BTreeMap<String, ScriptConfig>,
    #[serde(default)]
    pub http: HTTPConfig,
    #[serde(default)]
    pub script_manager: ScriptManagerConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub crawl: CrawlConfig,
    #[serde(default)]
    pub index: IndexConfig,
}

#[derive(Deserialize)]
pub struct ScriptManagerConfig {
    #[serde(default = "script_manager::workers")]
    pub workers: usize,
}

impl Default for ScriptManagerConfig {
    fn default() -> Self {
        Self {
            workers: script_manager::workers(),
        }
    }
}

#[derive(Deserialize)]
pub struct HTTPConfig {
    #[serde(default = "http::workers")]
    pub workers: usize,
    #[serde(default = "http::tasks_per_worker")]
    pub tasks_per_worker: usize,
    #[serde(default = "http::request_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,
}

impl Default for HTTPConfig {
    fn default() -> Self {
        Self {
            workers: http::workers(),
            tasks_per_worker: http::tasks_per_worker(),
            request_timeout: http::request_timeout(),
        }
    }
}

#[derive(Deserialize)]
pub struct OutputConfig {
    #[serde(default = "output::workers")]
    pub workers: usize,
    #[serde(default = "output::prefix")]
    pub file_prefix: String,
    #[serde(
        default = "output::file_size",
        deserialize_with = "deserialize_byte_size"
    )]
    pub file_size: u64,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            workers: output::workers(),
            file_prefix: output::prefix(),
            file_size: output::file_size(),
        }
    }
}

#[derive(Deserialize)]
pub struct CrawlConfig {
    #[serde(default)]
    pub urls_file: Option<String>,
    #[serde(default = "crawl::base_url")]
    pub base_url: Url,
}

impl Default for CrawlConfig {
    fn default() -> Self {
        Self {
            urls_file: Default::default(),
            base_url: crawl::base_url(),
        }
    }
}

#[derive(Deserialize)]
pub struct IndexConfig {
    #[serde(default = "index::compression")]
    pub compression: bool,
    #[serde(default = "index::compression_level")]
    pub compression_level: i32,
    #[serde(default = "index::overwrite")]
    pub overwrite: bool,
}

impl Default for IndexConfig {
    fn default() -> Self {
        Self {
            compression: index::compression(),
            compression_level: index::compression_level(),
            overwrite: index::overwrite(),
        }
    }
}

impl IndexConfig {
    pub fn into_db(&self, path: impl AsRef<Path>) -> sled::Result<sled::Db> {
        sled::Config::new()
            .path(&path)
            .use_compression(self.compression)
            .compression_factor(self.compression_level)
            .open()
    }
}

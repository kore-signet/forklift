use std::{
    borrow::Cow,
    collections::BTreeMap,
    num::NonZeroU32,
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use governor::Quota;
use hyper::{header::HeaderName, http::HeaderValue, HeaderMap};
use serde::{
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Deserializer, Serialize, Serializer,
};
use url::Url;

use crate::scripting::ScriptFilter;

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
        folder: std::path::PathBuf = std::path::PathBuf::new();
    };
    http {
        workers: usize = 4;
        tasks_per_worker: usize = 16;
        request_timeout: std::time::Duration = std::time::Duration::from_secs(60 * 20);
        enable_http2: bool = true;
        dns_cache_size: usize = 1024;
        rate_limiter_jitter: std::time::Duration = std::time::Duration::from_secs(1);
    };
    scripts {
        workers: usize = 4;
    };
    output {
        workers: usize = 4;
        prefix: String = "forklift_crawl".to_owned();
        file_size: u64 = 2000000000; // 2GB
    };
    crawl {
        base_url: url::Url = url::Url::parse("http://forklift.local").unwrap();
        max_hops: usize = 1;
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

fn deserialize_headers<'de, D>(deserializer: D) -> Result<HeaderMap, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }

    let deser_map = BTreeMap::<Cow<'_, str>, OneOrMany>::deserialize(deserializer)?;
    let mut headers = HeaderMap::with_capacity(deser_map.len());

    deser_map
        .into_iter()
        .filter_map(|(key, vals)| HeaderName::from_str(&key).ok().zip(Some(vals)))
        .filter_map(|(key, vals)| {
            let vals = match vals {
                OneOrMany::One(ref s) => HeaderValue::from_str(s).ok().map(|v| vec![v]),
                OneOrMany::Many(vals) => vals
                    .into_iter()
                    .map(|v| HeaderValue::from_str(&v).ok())
                    .collect::<Option<Vec<HeaderValue>>>(),
            }?;

            Some((key, vals))
        })
        .for_each(|(k, v)| {
            for val in v {
                headers.append(&k, val);
            }
        });

    Ok(headers)
}

#[repr(transparent)]
struct HeaderSeqSerializer<'a>(hyper::header::GetAll<'a, HeaderValue>);

impl<'a> Serialize for HeaderSeqSerializer<'a> {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let iter = self.0.iter();
        let mut seq = ser.serialize_seq(iter.size_hint().1)?;
        for val in iter.filter_map(|v| v.to_str().ok()) {
            seq.serialize_element(val)?;
        }

        seq.end()
    }
}

fn serialize_headers<S>(map: &HeaderMap, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut seq = ser.serialize_map(Some(map.len()))?;
    for key in map.keys() {
        seq.serialize_key(key.as_str())?;
        seq.serialize_value(&HeaderSeqSerializer(map.get_all(key)))?;
    }

    seq.end()
}

#[derive(Serialize, Deserialize)]
pub struct ForkliftConfig {
    #[serde(default = "base::folder")]
    pub folder: PathBuf,
    #[serde(default)]
    pub scripts: BTreeMap<String, ScriptConfig>,
    #[serde(default)]
    pub http: HTTPConfig,
    #[serde(default)]
    pub output: OutputConfig,
    #[serde(default)]
    pub crawl: CrawlConfig,
    #[serde(default)]
    pub index: IndexConfig,
}

#[derive(Serialize, Deserialize)]
pub struct ScriptConfig {
    #[serde(default)]
    pub(crate) filter: ScriptFilter,
    pub(crate) command: String,
    #[serde(default)]
    pub(crate) args: Vec<String>,
    #[serde(default = "scripts::workers")]
    pub(crate) workers: usize,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimitingDuration {
    Second,
    Minute,
    Hour,
}

impl RateLimitingDuration {
    pub fn as_duration(&self) -> Duration {
        match self {
            RateLimitingDuration::Second => Duration::from_secs(1),
            RateLimitingDuration::Minute => Duration::from_secs(60),
            RateLimitingDuration::Hour => Duration::from_secs(60 * 60),
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct RateLimitingConfig {
    pub n: NonZeroU32,
    pub per: RateLimitingDuration,
    #[serde(default = "http::rate_limiter_jitter", with = "humantime_serde")]
    pub jitter: Duration,
}

impl Default for RateLimitingConfig {
    fn default() -> Self {
        Self {
            n: NonZeroU32::new(200).unwrap(),
            per: RateLimitingDuration::Second,
            jitter: http::rate_limiter_jitter(),
        }
    }
}

impl RateLimitingConfig {
    pub fn as_quota(&self) -> Quota {
        let replenish_interval_ns = self.per.as_duration().as_nanos() / (self.n.get() as u128);
        Quota::with_period(Duration::from_nanos(replenish_interval_ns as u64))
            .unwrap()
            .allow_burst(self.n)
    }
}

#[derive(Serialize, Deserialize)]
pub struct HTTPConfig {
    #[serde(default = "http::workers")]
    pub workers: usize,
    #[serde(default = "http::tasks_per_worker")]
    pub tasks_per_worker: usize,
    #[serde(default = "http::request_timeout", with = "humantime_serde")]
    pub request_timeout: Duration,
    #[serde(
        default,
        deserialize_with = "deserialize_headers",
        serialize_with = "serialize_headers"
    )]
    pub headers: HeaderMap,
    #[serde(default = "http::enable_http2")]
    pub enable_http2: bool,
    #[serde(default = "http::dns_cache_size")]
    pub dns_cache_size: usize,
    #[serde(default)]
    pub rate_limiter: RateLimitingConfig,
}

impl Default for HTTPConfig {
    fn default() -> Self {
        Self {
            workers: http::workers(),
            tasks_per_worker: http::tasks_per_worker(),
            request_timeout: http::request_timeout(),
            headers: HeaderMap::new(),
            enable_http2: http::enable_http2(),
            dns_cache_size: http::dns_cache_size(),
            rate_limiter: RateLimitingConfig::default(),
        }
    }
}

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
pub struct CrawlConfig {
    #[serde(default)]
    pub urls_file: Option<String>,
    #[serde(default = "crawl::base_url")]
    pub base_url: Url,
    #[serde(default = "crawl::max_hops")]
    pub max_hops: usize,
}

impl Default for CrawlConfig {
    fn default() -> Self {
        Self {
            urls_file: Default::default(),
            base_url: crawl::base_url(),
            max_hops: crawl::max_hops(),
        }
    }
}

#[derive(Serialize, Deserialize)]
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

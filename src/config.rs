use std::collections::BTreeMap;

use serde::Deserialize;
use url::Url;

use crate::scripting::ScriptConfig;

macro_rules! default_vals {
    ($($name:ident: $t:ty = $val:literal);* $(;)*) => {
        $(
             default_vals!($name,$t,$val);
        )*
    };
    ($name:ident,$t:ty,$val:literal) => {
        const fn $name() -> $t {
            $val
        }
    };
}

default_vals! {
    http_workers: usize = 4;
    tasks_per_http_worker: usize = 16;
    writer_tasks: usize = 2;
    script_tasks: usize = 2;
}

#[derive(Deserialize)]
pub struct CrawlConfig {
    pub base_url: Url,
    #[serde(default)]
    pub scripts: BTreeMap<String, ScriptConfig>,
    #[serde(default = "http_workers")]
    pub http_workers: usize,
    #[serde(default = "tasks_per_http_worker")]
    pub tasks_per_http_worker: usize,
    #[serde(default = "writer_tasks")]
    pub writer_tasks: usize,
    #[serde(default = "script_tasks")]
    pub script_tasks: usize,
}

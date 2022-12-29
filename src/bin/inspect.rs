use std::{fs::File, path::PathBuf};

use forklift::{config::ForkliftConfig, warc::CdxRecord};
use rkyv::Deserialize;

fn main() -> anyhow::Result<()> {
    let path = PathBuf::from(std::env::args().nth(1).unwrap());

    let config: ForkliftConfig = serde_json::from_reader(File::open(path.join("config.json"))?)?;

    let db = config.index.into_db(path.join("idx"))?;

    let mut iter = db.iter();

    while let Some((k, v)) = iter.next().transpose()? {
        let archived: CdxRecord = unsafe { rkyv::archived_root::<CdxRecord>(&v[..]) }
            .deserialize(&mut rkyv::Infallible)
            .unwrap();
        println!(
            "{}: {}",
            std::str::from_utf8(&k).unwrap(),
            serde_json::to_string_pretty(&archived).unwrap()
        );
    }

    Ok(())
}

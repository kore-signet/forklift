use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use hyper::http::HeaderValue;
use miniz_oxide::deflate::{
    self,
    core::{create_comp_flags_from_zip_params, CompressorOxide, TDEFLFlush, TDEFLStatus},
};
use sha2::{Digest, Sha256};
use time::{
    format_description::well_known::{
        iso8601::{self, TimePrecision},
        Iso8601,
    },
    OffsetDateTime,
};

use crate::{warc::WarcRecord, ForkliftError, ForkliftResult};

const WARC_FILENAME_TIMESTAMP_FORMAT: iso8601::EncodedConfig = iso8601::Config::DEFAULT
    .set_time_precision(TimePrecision::Second {
        decimal_digits: None,
    })
    .set_offset_precision(iso8601::OffsetPrecision::Hour)
    .set_use_separators(false)
    .encode();

pub trait WARCOutput {
    fn position(&mut self) -> ForkliftResult<u64>;
    fn write_record(&mut self, record: &[u8]) -> ForkliftResult<()>;
}

impl<T> WARCOutput for T
where
    T: Write + Seek,
{
    #[inline]
    fn position(&mut self) -> ForkliftResult<u64> {
        self.stream_position().map_err(ForkliftError::IOError)
    }

    #[inline]
    fn write_record(&mut self, record: &[u8]) -> ForkliftResult<()> {
        self.write_all(record).map_err(ForkliftError::IOError)
    }
}

pub struct WARCFileOutput {
    writer: Option<BufWriter<File>>,
    byte_counter: u64,
    file_size_for_rotation: u64,
    warc_path: PathBuf,
    warc_prefix: String,
    file_counter: usize,
}

impl WARCFileOutput {
    pub fn new(
        path: impl AsRef<Path>,
        warc_prefix: impl AsRef<str>,
        file_size_for_rotation: u64,
    ) -> ForkliftResult<WARCFileOutput> {
        let mut out = WARCFileOutput {
            writer: None,
            byte_counter: 0,
            file_size_for_rotation,
            warc_path: path.as_ref().to_owned(),
            warc_prefix: warc_prefix.as_ref().to_owned(),
            file_counter: 0,
        };

        out.rotate()?;

        Ok(out)
    }

    fn rotate(&mut self) -> ForkliftResult<()> {
        self.byte_counter = 0;
        self.file_counter += 1;

        let path = self.warc_path.join(format!(
            "{prefix}-{timestamp}-{filecounter:07}.warc.gz",
            prefix = &self.warc_prefix,
            timestamp = OffsetDateTime::now_utc()
                .format(&Iso8601::<WARC_FILENAME_TIMESTAMP_FORMAT>)
                .unwrap(),
            filecounter = self.file_counter
        ));

        self.writer = Some(BufWriter::new(File::create(path)?));

        Ok(())
    }
}

impl WARCOutput for WARCFileOutput {
    #[inline]
    fn position(&mut self) -> ForkliftResult<u64> {
        self.writer
            .as_mut()
            .unwrap()
            .stream_position()
            .map_err(ForkliftError::IOError)
    }

    #[inline]
    fn write_record(&mut self, record: &[u8]) -> ForkliftResult<()> {
        self.writer.as_mut().unwrap().write_all(record)?;

        self.byte_counter += record.len() as u64;
        if self.byte_counter > self.file_size_for_rotation {
            let mut writer = self.writer.take().unwrap();
            writer.flush()?;
            let file = writer.into_inner().unwrap();
            file.sync_all()?;
            drop(file);

            self.rotate()?;
        }

        Ok(())
    }
}

pub struct RecordProcessor<W: WARCOutput> {
    compression_buffer: Vec<u8>,
    hasher: Sha256,
    db: sled::Db,
    writer: Arc<Mutex<W>>,
    compressor: CompressorOxide,
}

impl<W: WARCOutput> RecordProcessor<W> {
    pub fn new(
        compress_level: u8,
        db: sled::Db,
        writer: Arc<Mutex<W>>,
    ) -> ForkliftResult<RecordProcessor<W>> {
        Ok(RecordProcessor {
            compression_buffer: Vec::with_capacity(1_000_000),
            hasher: Sha256::new(),
            db,
            writer,
            compressor: CompressorOxide::new(create_comp_flags_from_zip_params(
                compress_level.into(),
                0,
                0,
            )),
        })
    }

    fn compress(&mut self, record: &[u8]) {
        let crc32_hash = crc32fast::hash(record);

        self.compression_buffer.resize(record.len() + 18, 0);

        self.compression_buffer[0..10].copy_from_slice(&[
            0x1f, 0x8b, // ID
            8,    // DEFLATE
            0,    // FLAGS
            0, 0, 0, 0,   // MTIME
            0,   // XFL
            255, // OS
        ]);

        let mut in_pos = 0;
        let mut out_pos = 10;

        // borrowing from https://docs.rs/miniz_oxide/0.6.2/src/miniz_oxide/deflate/mod.rs.html#121

        loop {
            let (status, bytes_in, bytes_out) = deflate::core::compress(
                &mut self.compressor,
                &record[in_pos..],
                &mut self.compression_buffer[out_pos..],
                TDEFLFlush::Finish,
            );

            out_pos += bytes_out;
            in_pos += bytes_in;

            match status {
                TDEFLStatus::Done => {
                    self.compression_buffer.truncate(out_pos);
                    break;
                }
                TDEFLStatus::Okay => {
                    if self.compression_buffer.len().saturating_sub(out_pos) < 30 {
                        self.compression_buffer
                            .resize(self.compression_buffer.len() * 2, 0)
                    }
                }
                _ => panic!("{:?}", status),
            }
        }

        let len = record.len() as u32;

        self.compression_buffer.extend_from_slice(&[
            (crc32_hash >> 0) as u8,
            (crc32_hash >> 8) as u8,
            (crc32_hash >> 16) as u8,
            (crc32_hash >> 24) as u8,
            (len >> 0) as u8,
            (len >> 8) as u8,
            (len >> 16) as u8,
            (len >> 24) as u8,
        ]);

        self.compressor.reset();
    }

    pub fn add_record(&mut self, mut record: WarcRecord) -> ForkliftResult<()> {
        self.add_digest(&mut record);

        let (mut cdx, mut record) = record.into_bytes();

        record.extend_from_slice(b"\r\n\r\n");

        self.compress(&record);

        let mut output = self.writer.lock().unwrap();

        cdx.offset = output.position()?;

        let len = record.len();
        output.write_record(&self.compression_buffer)?;
        drop(output);

        cdx.length = len as u64;
        self.db.insert(
            cdx.url.clone(),
            rkyv::util::to_bytes::<_, 1024>(&cdx).unwrap().as_ref(),
        )?;

        Ok(())
    }

    fn add_digest(&mut self, record: &mut WarcRecord) {
        let mut hash_hex = [0u8; 71];
        hash_hex[0..7].copy_from_slice(b"sha256:");

        self.hasher.update(&record.block);
        let block_hash = self.hasher.finalize_reset();
        base16::encode_config_slice(&block_hash, base16::EncodeLower, &mut hash_hex[7..]);
        record.headers.insert(
            "Warc-Block-Digest",
            HeaderValue::from_bytes(&hash_hex).unwrap(),
        );

        if record.payload_start != 0 {
            self.hasher.update(&record.block[record.payload_start..]);
            let payload_hash = self.hasher.finalize_reset();
            base16::encode_config_slice(&payload_hash, base16::EncodeLower, &mut hash_hex[7..]);
            record.headers.insert(
                "Warc-Payload-Digest",
                HeaderValue::from_bytes(&hash_hex).unwrap(),
            );
        }
    }
}

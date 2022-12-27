use std::{
    io::{BufWriter, Seek, Write},
    sync::{Arc, Mutex},
};

use hyper::http::HeaderValue;
use miniz_oxide::deflate::{
    self,
    core::{create_comp_flags_from_zip_params, CompressorOxide, TDEFLFlush, TDEFLStatus},
};
use sha2::{Digest, Sha256};

use crate::{warc::WarcRecord, ForkliftResult};

pub struct RecordProcessor<W: Write + Seek> {
    compression_buffer: Vec<u8>,
    hasher: Sha256,
    db: sled::Db,
    writer: Arc<Mutex<BufWriter<W>>>,
    compressor: CompressorOxide,
}

impl<W: Write + Seek> RecordProcessor<W> {
    pub fn new(
        compress_level: u8,
        db: sled::Db,
        writer: Arc<Mutex<BufWriter<W>>>,
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

        cdx.offset = output.stream_position()?;

        let len = record.len();
        output.write_all(&self.compression_buffer)?;
        output.flush()?;
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

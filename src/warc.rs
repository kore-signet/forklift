use bytes::{BufMut, BytesMut};
use hyper::{
    body::Bytes,
    client::connect::HttpInfo,
    header::{CONTENT_TYPE, LOCATION},
    http::{response::Parts as ResponseParts, HeaderValue},
    HeaderMap, Version,
};
use rkyv::{Archive, Deserialize, Serialize};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

use uuid::Uuid;

use crate::ForkliftResult;

//const MAX_RESPONSE_SIZE: usize = 8000000000;

#[derive(Archive, Deserialize, Serialize, serde::Serialize, Default, Debug, Clone)]
pub struct CdxRecord {
    pub timestamp: i128,
    pub url: String,
    pub mime: Option<String>,
    pub status: u16,
    pub length: u64,
    pub offset: u64,
    pub redirect: Option<String>,
    pub filename: Option<String>,
}

pub struct WarcRecord {
    pub(crate) metadata: CdxRecord,
    pub(crate) headers: HeaderMap,
    pub(crate) payload_start: usize,
    pub(crate) block: Bytes,
}

impl WarcRecord {
    pub(crate) fn as_bytes(&self) -> (CdxRecord, Vec<u8>) {
        let mut out: Vec<u8> = Vec::with_capacity(1024 + self.block.len());
        out.extend_from_slice(b"WARC/1.1\r\n");

        for (name, value) in self.headers.iter() {
            out.extend_from_slice(name.as_ref());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }

        out.extend_from_slice(b"\r\n");

        out.extend_from_slice(self.block.as_ref());

        (self.metadata.clone(), out)
    }

    pub fn from_response(
        res: &ResponseParts,
        body: Bytes,
        target_url: &str,
    ) -> ForkliftResult<WarcRecord> {
        let mut cdx = CdxRecord::default();

        let mut warc_headers = HeaderMap::new();
        warc_headers.insert(
            "WARC-Target-URI",
            HeaderValue::from_str(target_url).unwrap(),
        );
        cdx.url = target_url.to_string();

        warc_headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/http;msgtype=response"),
        );
        warc_headers.insert("WARC-Type", HeaderValue::from_static("response"));

        let time = OffsetDateTime::now_utc();
        warc_headers.insert(
            "WARC-Date",
            HeaderValue::try_from(time.format(&Rfc3339).unwrap()).unwrap(),
        );
        cdx.timestamp = time.unix_timestamp_nanos();
        cdx.mime = res
            .headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_owned());
        cdx.status = res.status.as_u16();

        cdx.redirect = if res.status.is_redirection() {
            res.headers
                .get(LOCATION)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_owned())
        } else {
            None
        };

        warc_headers.insert(
            "WARC-Record-ID",
            HeaderValue::try_from(format!("<urn:uuid:{}>", Uuid::new_v4().hyphenated())).unwrap(),
        );

        res.extensions.get::<HttpInfo>().map(|info| {
            warc_headers.insert(
                "WARC-IP-Address",
                HeaderValue::try_from(info.remote_addr().to_string()).unwrap(),
            );
        });

        warc_headers.insert("WARC-Protocol", HeaderValue::from_static(match res.version {
            Version::HTTP_09 => "http/0.9",
            Version::HTTP_10 => "http/1.0",
            Version::HTTP_11 => "http/1.1",
            Version::HTTP_2 => "h2",
            Version::HTTP_3 => "h3",
            _ => unreachable!()
        }));

        let mut block = BytesMut::with_capacity(body.len() + 1024);

        block.extend_from_slice(b"HTTP/1.1 ");
        block.extend_from_slice(res.status.as_str().as_bytes());
        block.put_u8(b' ');
        block.extend_from_slice(
            res.status
                .canonical_reason()
                .unwrap_or("<unknown status code>")
                .as_bytes(),
        );
        block.extend_from_slice(b"\r\n");

        for (name, value) in res.headers.iter() {
            block.extend_from_slice(name.as_ref());
            block.extend_from_slice(b": ");
            block.extend_from_slice(value.as_bytes());
            block.extend_from_slice(b"\r\n");
        }

        block.extend_from_slice(b"\r\n");

        let payload_start = block.len();
        let has_body = !body.is_empty();
        block.put(body);

        warc_headers.append(
            "Content-Length",
            HeaderValue::from_str(&format!("{}", block.len())).unwrap(),
        ); //switch to itoa

        Ok(WarcRecord {
            headers: warc_headers,
            payload_start: if has_body { payload_start } else { 0 },
            block: block.freeze(),
            metadata: cdx,
        })
    }
}

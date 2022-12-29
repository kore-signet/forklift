use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use crate::{ForkliftError, ForkliftResult};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum RpcMethod {
    GetUrl,
    #[serde(other)]
    Unknown,
}

#[allow(dead_code)]
#[repr(u8)]
pub enum OpCode {
    DataIn = 0, // followed by length (u64, le) and data of <length>
    Url = 1,    // followed by url; read until \n
    FileEnd = 2,
    Rpc = 3, // followed by length (u64, le) and a json-rpc message of <length (bytes)>
    ScriptClose = 4,
}

impl OpCode {
    pub fn from_u8(v: u8) -> Option<OpCode> {
        if v > 3 {
            None
        } else {
            Some(unsafe { std::mem::transmute(v) })
        }
    }
}

#[derive(Debug)]
pub enum ScriptMessage<'a> {
    Url(&'a str),
    FileEnd,
    RpcRequest(RpcRequest),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcRequest {
    pub method: RpcMethod,
    #[serde(default)]
    pub params: Vec<Box<RawValue>>,
    pub id: String,
}

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum RpcResponse<T: Serialize> {
    Ok { result: T, id: String },
    Err { error: RpcError<T>, id: String },
}

impl<T: Serialize> RpcResponse<T> {
    pub fn ok(id: String, result: T) -> RpcResponse<T> {
        RpcResponse::Ok { result, id }
    }

    #[allow(dead_code)]
    pub fn error_with_payload(id: String, code: i64, message: String, data: T) -> RpcResponse<T> {
        RpcResponse::Err {
            error: RpcError {
                code,
                message,
                data: Some(data),
            },
            id,
        }
    }
}

impl RpcResponse<()> {
    pub fn error(id: String, code: i64, message: String) -> RpcResponse<()> {
        RpcResponse::Err {
            error: RpcError {
                code,
                message,
                data: None,
            },
            id,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct RpcError<T> {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
}

#[repr(transparent)]
pub struct ScriptOutput<W: AsyncWriteExt> {
    inner: W,
}

impl<W: AsyncWriteExt> Deref for ScriptOutput<W> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<W: AsyncWriteExt> DerefMut for ScriptOutput<W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

pub struct ScriptInput<R: AsyncReadExt + AsyncBufReadExt> {
    inner: R,
    scratch_space: Vec<u8>,
    url_buffer: String,
}

impl<R: AsyncReadExt + AsyncBufReadExt> Deref for ScriptInput<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<R: AsyncReadExt + AsyncBufReadExt> DerefMut for ScriptInput<R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

struct UrlBufferPtr(*const String);

impl UrlBufferPtr {
    unsafe fn as_mut_str(&self) -> &mut String {
        &mut *(self.0 as *mut String)
    }
}

struct ScratchPtr(*const u8, usize);

impl ScratchPtr {
    unsafe fn as_mut_slice(&self) -> &mut [u8] {
        std::slice::from_raw_parts_mut(self.0 as *mut u8, self.1)
    }
}

unsafe impl Send for UrlBufferPtr {}
unsafe impl Sync for UrlBufferPtr {}
unsafe impl Send for ScratchPtr {}
unsafe impl Sync for ScratchPtr {}

impl<R: AsyncReadExt + AsyncBufReadExt + Unpin> ScriptInput<R> {
    pub fn new(inner: R) -> ScriptInput<R> {
        ScriptInput {
            inner,
            scratch_space: Vec::with_capacity(128),
            url_buffer: String::with_capacity(128),
        }
    }

    pub async fn read(&mut self) -> ForkliftResult<ScriptMessage<'_>> {
        let op_code = OpCode::from_u8(self.read_u8().await?).ok_or(ForkliftError::InvalidOpcode)?;
        match op_code {
            OpCode::DataIn | OpCode::ScriptClose => Err(ForkliftError::InvalidOpcode),
            OpCode::Url => {
                self.url_buffer.clear();

                let ptr = UrlBufferPtr(&self.url_buffer as *const String);
                let inner = self.deref_mut();

                // SAFETY: this is just a split borrow problem. we know we hold a unique mutable reference to both inner & url_buffer
                let len_read = unsafe {
                    let url_buffer = ptr.as_mut_str();
                    inner.read_line(url_buffer).await?
                };

                if let Some(len) = len_read.checked_sub(1) {
                    self.url_buffer.truncate(len);
                }

                Ok(ScriptMessage::Url(&self.url_buffer))
            }
            OpCode::FileEnd => Ok(ScriptMessage::FileEnd),
            OpCode::Rpc => {
                let len = self.read_u64_le().await?;
                self.scratch_space.resize(len as usize, 0);

                let ptr = ScratchPtr(self.scratch_space.as_ptr(), len as usize);
                let inner = self.deref_mut();

                // SAFETY: this is just a split borrow problem. we know we hold a unique mutable reference to both inner & scratch_space
                let _len_read = unsafe {
                    let slice = ptr.as_mut_slice();
                    inner.read_exact(slice).await?
                };

                Ok(ScriptMessage::RpcRequest(serde_json::from_slice(
                    &self.scratch_space,
                )?))
            }
        }
    }
}

impl<W: AsyncWriteExt + Send + Unpin> ScriptOutput<W> {
    pub fn new(inner: W) -> ScriptOutput<W> {
        ScriptOutput { inner }
    }

    pub async fn send_close(&mut self) -> ForkliftResult<()> {
        self.write_u8(OpCode::ScriptClose as u8).await?;
        Ok(())
    }

    pub async fn send_rpc_response<T: Serialize + Send>(
        &mut self,
        req: RpcResponse<T>,
    ) -> ForkliftResult<()> {
        let bytes = serde_json::to_vec(&req)?;
        self.write_u8(OpCode::Rpc as u8).await?;
        self.write_u64_le(bytes.len() as u64).await?;
        self.write_all(&bytes).await?;

        Ok(())
    }

    pub async fn send_data<T: AsRef<[u8]> + Send>(
        &mut self,
        url: &str,
        data: T,
    ) -> ForkliftResult<()> {
        let data_ref = data.as_ref();
        self.write_u8(OpCode::DataIn as u8).await?;
        self.write_all(url.as_bytes()).await?;
        self.write_u8(b'\n').await?;
        self.write_u64_le(data_ref.len() as u64).await?;
        self.write_all(data_ref).await?;

        Ok(())
    }
}

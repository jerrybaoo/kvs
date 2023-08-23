use std::io::Cursor;
use std::result::Result::Ok;

use anyhow::{anyhow, Result};
use bytes::{Buf, BytesMut};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

pub(crate) struct Connection {
    stream: BufWriter<TcpStream>,

    buf: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buf: BytesMut::with_capacity(1024 * 4),
        }
    }

    pub async fn read<T: DeserializeOwned>(&mut self) -> Result<Option<T>> {
        loop {
            match self.parse() {
                Ok(req) => return Result::Ok(Some(req)),
                Err(_) => {
                    if 0 == self.stream.read_buf(&mut self.buf).await? {
                        if self.buf.is_empty() {
                            return Ok(None);
                        } else {
                            return Err(anyhow!("connection reset by peer"));
                        }
                    }
                }
            };
        }
    }

    fn parse<T: DeserializeOwned>(&mut self) -> Result<T> {
        let mut buf: Cursor<&[u8]> = Cursor::new(&self.buf[..]);
        match bson::from_reader::<_, T>(&mut buf) {
            Ok(req) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                self.buf.advance(len);

                Ok(req)
            }
            Err(e) => Err(anyhow!(e)),
        }
    }

    pub async fn write<T: Serialize>(&mut self, info: T) -> Result<()> {
        let resp_bz = bson::to_vec(&info)?;

        self.stream.write(&resp_bz).await?;

        self.stream.flush().await.map_err(|e| anyhow!(e))
    }
}

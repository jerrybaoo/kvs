// use std::io::Write;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::connection::Connection;
use crate::engine::KvsEngine;
// use crate::thread_pool::{shared_queue::SharedQueueThreadPool, ThreadPool};

pub struct Server<E: KvsEngine> {
    engine: E,
    // thread_pool: SharedQueueThreadPool,
}

impl<E: KvsEngine> Server<E> {
    pub fn new(engine: E) -> Result<Self> {
        Ok(Self {
            engine,
            //thread_pool: SharedQueueThreadPool::new(12).expect("create thread pool failed"),
        })
    }

    pub async fn serve(&mut self, addr: String) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let conn = Connection::new(stream);
            let mut engine = self.engine.clone();
            tokio::spawn(async move {
                let res = Self::process_connection(&mut engine, conn).await;
                if let Err(e) = res {
                    log::error!("connection has error {}", e);
                }
            });
        }
    }

    async fn process_connection(engine: &mut E, mut conn: Connection) -> Result<()> {
        loop {
            let req = conn.read::<Request>().await?;

            if let Some(r) = req {
                let resp = Self::process_request(engine, &r)?;
                conn.write(Response {
                    response: String::from_utf8(resp)?,
                })
                .await?;
            }
        }
    }

    fn process_request(engine: &mut E, request: &Request) -> Result<Vec<u8>> {
        let response = Self::process_transaction(engine, &request);
        bson::ser::to_vec(&response).map_err(|e| anyhow!(e))
    }

    fn process_transaction(engine: &mut E, request: &Request) -> Response {
        match request {
            Request::Get(key) => Self::get(engine, key),
            Request::Set(key, value) => Self::set(engine, key, value),
            Request::Remove(key) => Self::remove(engine, key),
        }
    }

    fn get(engine: &mut E, key: &String) -> Response {
        engine.get(key.to_owned()).map_or_else(
            |_| Response {
                response: "Key not found".to_string(),
            },
            |value| Response { response: value },
        )
    }

    fn set(engine: &mut E, key: &String, value: &String) -> Response {
        engine.set(key.to_owned(), value.to_owned()).map_or_else(
            |e| Response {
                response: e.to_string(),
            },
            |_| Response {
                response: "".to_string(),
            },
        )
    }

    fn remove(engine: &mut E, key: &String) -> Response {
        engine.remove(key.to_owned()).map_or_else(
            |e| Response {
                response: e.to_string(),
            },
            |_| Response {
                response: "".to_string(),
            },
        )
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    Get(String),
    Set(String, String),
    Remove(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub response: String,
}

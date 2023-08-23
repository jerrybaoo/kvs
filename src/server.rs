use std::io::Write;
use std::net::{TcpListener, TcpStream};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::engine::KvsEngine;
use crate::thread_pool::{shared_queue::SharedQueueThreadPool, ThreadPool};

pub struct Server<E: KvsEngine> {
    engine: E,
    thread_pool: SharedQueueThreadPool,
}

impl<E: KvsEngine> Server<E> {
    pub fn new(engine: E) -> Result<Self> {
        Ok(Self {
            engine,
            thread_pool: SharedQueueThreadPool::new(12).expect("create thread pool failed"),
        })
    }

    pub fn serve(&mut self, addr: String) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for in_coming in listener.incoming() {
            let mut engine = self.engine.clone();
            self.thread_pool.spawn(move || {
                match in_coming {
                    Ok(mut stream) => {
                        let res = Self::process_request(&mut engine, &mut stream)
                            .and_then(|data| stream.write(&data).map_err(|e| anyhow!(e)));
                        log::info!("process stream result: {:#?}", res)
                    }
                    Err(e) => log::error!("connection has error: {}", e),
                };
            })
        }

        Ok(())
    }

    fn process_request(engine: &mut E, stream: &mut TcpStream) -> Result<Vec<u8>> {
        let request = bson::from_reader::<_, Request>(stream)?;
        let response = Self::process_transaction(engine, &request);
        bson::ser::to_vec(&response).map_err(|e| anyhow!(e))
    }

    fn process_transaction(engine: &mut E, request: &Request) -> Response {
        match request {
            Request::Get(key) => Self::get_from_engine(engine, key),
            Request::Set(key, value) => Self::set_to_engine(engine, key, value),
            Request::Remove(key) => Self::remove_from_engine(engine, key),
        }
    }

    fn get_from_engine(engine: &mut E, key: &String) -> Response {
        engine.get(key.to_owned()).map_or_else(
            |_| Response {
                response: "Key not found".to_string(),
            },
            |value| Response { response: value },
        )
    }

    fn set_to_engine(engine: &mut E, key: &String, value: &String) -> Response {
        engine.set(key.to_owned(), value.to_owned()).map_or_else(
            |e| Response {
                response: e.to_string(),
            },
            |_| Response {
                response: "".to_string(),
            },
        )
    }

    fn remove_from_engine(engine: &mut E, key: &String) -> Response {
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

use std::io::Write;
use std::net::TcpListener;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::engine::KvsEngine;
use crate::thread_pool::NativeThreadPool;

pub struct Server<E: KvsEngine> {
    engine: E,
    thread_pool: NativeThreadPool,
}

impl<E: KvsEngine> Server<E> {
    pub fn new(engine: E) -> Result<Self> {
        Ok(Self {
            engine,
            thread_pool: NativeThreadPool::new(12),
        })
    }

    pub fn serve(&mut self, addr: String) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        
        for in_coming in listener.incoming() {
            let mut engine = self.engine.clone();
            self.thread_pool.spawn(move || -> Result<()> {
                match in_coming {
                    Ok(mut stream) => {
                        let request = bson::from_reader::<_, Request>(&stream)?;
                        let response = Self::process_transaction(&mut engine, &request);
                        let resp_bz = bson::ser::to_vec(&response).map_err(|e| anyhow!(e))?;
                        stream.write_all(&resp_bz).map_err(|e| anyhow!(e))?;
                    }
                    Err(e) => log::error!("connection error: {}", e),
                }
                Ok(())
            })
        }

        Ok(())
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

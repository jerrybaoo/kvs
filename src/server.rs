use std::io::Write;
use std::net::{TcpListener, TcpStream};

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::kvs::KvsEngine;

pub struct Server<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> Server<E> {
    pub fn new(engine: E) -> Result<Self> {
        Ok(Self { engine })
    }

    pub fn serve(&mut self, addr: String) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for mut in_coming in listener.incoming() {
            match in_coming.as_mut() {
                Ok(stream) => {
                    let vec = self.process_stream(stream)?;
                    if let Err(e) = stream.write_all(&vec) {
                        log::error!("write response failed, reason: {}", e);
                    }
                }
                Err(e) => log::error!("connection error: {}", e),
            };
        }

        Ok(())
    }

    fn process_stream(&mut self, stream: &mut TcpStream) -> Result<Vec<u8>> {
        let request = bson::from_reader::<_, Request>(stream)?;
        let response = self.process_transaction(&request);
        bson::ser::to_vec(&response).map_err(|e| anyhow!(e))
    }

    fn process_transaction(&mut self, request: &Request) -> Response {
        match request {
            Request::Get(key) => self.get_from_engine(key),
            Request::Set(key, value) => self.set_to_engine(key, value),
            Request::Remove(key) => self.remove_from_engine(key),
        }
    }

    fn get_from_engine(&mut self, key: &String) -> Response {
        self.engine.get(key.to_owned()).map_or_else(
            |_| Response {
                response: "Key not found".to_string(),
            },
            |value| Response { response: value },
        )
    }

    fn set_to_engine(&mut self, key: &String, value: &String) -> Response {
        self.engine
            .set(key.to_owned(), value.to_owned())
            .map_or_else(
                |e| Response {
                    response: e.to_string(),
                },
                |_| Response {
                    response: "".to_string(),
                },
            )
    }

    fn remove_from_engine(&mut self, key: &String) -> Response {
        self.engine.remove(key.to_owned()).map_or_else(
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

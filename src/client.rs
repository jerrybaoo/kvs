use anyhow::{anyhow, Result};
use tokio::net::TcpStream;

use crate::connection::Connection;
use crate::server::{Request, Response};

pub struct Client {
    connection: Connection,
}

impl Client {
    pub async fn connect(addr: &String) -> Result<Client> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Client {
            connection: Connection::new(stream),
        })
    }

    pub async fn get(&mut self, key: String) -> Result<String> {
        self.connection.write(Request::Get(key)).await?;
        let response: Option<Response> = self.connection.read().await?;
        match response {
            Some(v) => Ok(v.response),
            None => Err(anyhow!("key not found")),
        }
    }

    pub async fn set(&mut self, key: String, value: String) -> Result<String> {
        self.connection.write(Request::Set(key, value)).await?;
        let response: Option<Response> = self.connection.read().await?;
        match response {
            Some(v) => Ok(v.response),
            None => Err(anyhow!("key not found")),
        }
    }

    pub async fn remove(&mut self, key: String) -> Result<String> {
        self.connection.write(Request::Remove(key)).await?;
        let response: Option<Response> = self.connection.read().await?;
        match response {
            Some(v) => Ok(v.response),
            None => Err(anyhow!("key not found")),
        }
    }
}

use std::path::PathBuf;

use anyhow::{anyhow, Ok, Result};
use sled::Db;

use crate::engine::KvsEngine;

pub struct Sled {
    pub db: Db,
}

impl Sled {
    pub fn new(root_path: &PathBuf) -> Result<Self> {
        Ok(Sled {
            db: sled::open(root_path)?,
        })
    }
}

impl KvsEngine for Sled {
    fn get(&mut self, key: String) -> Result<String> {
        self.db
            .get(key)?
            .map(|v| v.to_vec())
            .map(String::from_utf8)
            .transpose()?
            .ok_or(anyhow!("Key not found"))
    }

    fn set(&mut self, key: String, value: String) -> Result<Option<String>> {
        self.db
            .insert(key, value.as_bytes())?
            .map(|v| v.to_vec())
            .map(String::from_utf8)
            .transpose()
            .map_err(|e| anyhow!(e))
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db
            .remove(key)?
            .ok_or_else(|| anyhow!("Key not found"))
            .map(|_| ())
    }
}

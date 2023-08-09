// kv store
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom::Start, Write};
use std::path::PathBuf;
use std::vec;

use anyhow::{anyhow, Ok, Result};
use serde::{Deserialize, Serialize};

pub struct KVStore {
    path: PathBuf,

    readers: HashMap<u32, BufferReader<File>>,
    writer: BufferWriter<File>,
    last_reader_index: u32,

    index: HashMap<String, TransactionPosition>,
}

impl KVStore {
    pub fn new(root_path: &PathBuf) -> Result<Self> {
        let path = root_path.join("db");
        if !path.exists() {
            fs::create_dir(&path)?;
        }

        let mut storage_files = fs::read_dir(&path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        storage_files.sort();

        if storage_files.is_empty() {
            storage_files.insert(0, path.join("0.log").to_path_buf());
        }

        let f = File::options().create(true).read(true).append(true).open(
            storage_files
                .last()
                .ok_or(anyhow!("inner file system error"))?,
        )?;

        let writer = BufferWriter::<File> {
            writer: BufWriter::new(f),
            pos: 0,
        };

        let mut readers: HashMap<u32, BufferReader<File>> = HashMap::new();
        for (i, fp) in storage_files.iter().enumerate() {
            readers.insert(
                i as u32,
                BufferReader::<File> {
                    reader: BufReader::new(File::open(fp)?),
                },
            );
        }

        let mut kv_store: KVStore = KVStore {
            path: path.clone(),
            readers,
            writer,
            index: HashMap::new(),
            last_reader_index: (storage_files.len() - 1) as u32,
        };

        kv_store.load_index()?;

        return Ok(kv_store);
    }
}

impl KVStore {
    fn load_index(&mut self) -> Result<()> {
        let f = File::options()
            .read(true)
            .append(true)
            .open(self.path.join("index").to_path_buf());

        if let std::io::Result::Ok(index_file) = f {
            self.load_index_from_file(index_file)
        } else {
            self.load_index_from_readers()?;
        }

        Ok(())
    }

    fn load_index_from_file(&mut self, _index_file: File) {}

    // type(1bit)| timestamp(32bit) | ksz(32bit) | vsz(32bite)| key| value
    fn load_index_from_readers(&mut self) -> Result<()> {
        for i in 0..=self.last_reader_index {
            let reader = self.readers.get_mut(&i).ok_or(anyhow!("index error"))?;
            loop {
                let pos_before = reader.reader.stream_position()? as u32;

                if let std::result::Result::Ok(t) =
                    bson::from_reader::<_, Transaction>(&mut reader.reader)
                {
                    let pos_after = reader.reader.stream_position()? as u32;

                    let t_pos = TransactionPosition {
                        reader_index: i,
                        offset: pos_before,
                        len: pos_after - pos_before,
                    };

                    match t {
                        Transaction::Set(k, _) => self.index.insert(k, t_pos),
                        Transaction::Remove(k) => self.index.remove(&k),
                    };
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let transaction: Transaction = Transaction::Set(key.to_string(), value.to_string());
        let bytes = transaction.to_bytes()?;

        let size: u32 = self.writer.write(&bytes)?;
        let pos = TransactionPosition {
            reader_index: self.last_reader_index,
            offset: self.writer.pos,
            len: size,
        };

        self.index.insert(key.to_string(), pos);

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<String> {
        let pos = self.index.get(key).ok_or(anyhow!("key not found"))?;
        let reader = self
            .readers
            .get_mut(&pos.reader_index)
            .ok_or(anyhow!("db maybe breaded"))?;

        let mut data = vec![0; pos.len as usize];
        reader.read_exact(pos.offset, &mut data)?;

        match Transaction::from_bytes(&data)? {
            Transaction::Set(_, value) => Ok(value),
            Transaction::Remove(_) => Err(anyhow!("key not found")),
        }
    }

    pub fn remove(&mut self, key: &str) -> Result<()> {
        self.index.remove(key).ok_or(anyhow!("key not found"))?;

        let transaction: Transaction = Transaction::Remove(key.to_string());
        let bytes = transaction.to_bytes()?;

        self.writer.write(&bytes)?;

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
enum Transaction {
    Set(String, String),
    Remove(String),
}

impl Transaction {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bson::ser::to_vec(self).map_err(|e| anyhow!(e))
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        bson::de::from_slice(bytes).map_err(|e| anyhow!(e))
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct TransactionPosition {
    reader_index: u32,
    offset: u32,
    len: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct TransactionIndex {
    key: String,
    transaction_pos: TransactionPosition,
}

pub struct BufferReader<T: Read + Seek> {
    reader: BufReader<T>,
}

impl<T: Read + Seek> BufferReader<T> {
    pub fn read_exact(&mut self, pos: u32, data: &mut [u8]) -> Result<()> {
        self.reader
            .seek(Start(pos as u64))
            .map_err(|e| anyhow!(e.to_string()))?;

        self.reader
            .read_exact(data)
            .map_err(|e| anyhow!(e.to_string()))
    }
}

pub struct BufferWriter<T: Write> {
    writer: BufWriter<T>,
    pos: u32,
}

impl<T: Write> BufferWriter<T> {
    pub fn write(&mut self, data: &[u8]) -> Result<u32> {
        let size = self
            .writer
            .write(data)
            .map_err(|e| anyhow!(e.to_string()))?;

        let size = size as u32;
        self.pos += size;

        Ok(size)
    }
}

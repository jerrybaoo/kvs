// kv store
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom::Start, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex, RwLock,
    },
    vec,
};

use anyhow::{anyhow, Ok, Result};
use serde::{Deserialize, Serialize};

use crate::engine::KvsEngine;

#[derive(Clone)]
pub struct KVStore {
    path: PathBuf,

    max_reader_id: Arc<AtomicU32>,
    readers: Arc<RwLock<HashMap<u32, BufferReader<File>>>>,

    writer: Arc<Mutex<BufferWriter<File>>>,
    index: Arc<RwLock<HashMap<String, TransactionPosition>>>,
}

const LOG_MAX_SIZE: u32 = 1024 * 1024 * 24;

impl KVStore {
    pub fn new(root_path: &PathBuf) -> Result<Self> {
        let path = root_path.join("db");
        if !path.exists() {
            fs::create_dir(&path)?;
        }

        let mut files = fs::read_dir(&path)?
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()?;
        files.sort();

        if files.is_empty() {
            files.insert(0, path.join("0.log").to_path_buf());
        }

        let active_file = File::options()
            .create(true)
            .read(true)
            .append(true)
            .open(files.last().ok_or(anyhow!("inner file system error"))?)?;

        let writer = BufferWriter::<File> {
            writer: BufWriter::new(active_file),
            pos: 0,
        };

        let mut readers: HashMap<u32, BufferReader<File>> = HashMap::new();
        for (i, fp) in files.iter().enumerate() {
            readers.insert(
                i as u32,
                BufferReader::<File> {
                    inner: BufReader::new(File::open(fp)?),
                },
            );
        }

        let mut kv_store: KVStore = KVStore {
            path: path.clone(),
            max_reader_id: Arc::new(AtomicU32::new((files.len() - 1) as u32)),
            readers: Arc::new(RwLock::new(readers)),
            writer: Arc::new(Mutex::new(writer)),
            index: Arc::new(RwLock::new(HashMap::new())),
        };

        kv_store.load_index()?;

        return Ok(kv_store);
    }

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
        let mut readers = self.readers.write().map_err(|e| anyhow!(e.to_string()))?;
        let mut index = self.index.write().map_err(|e| anyhow!(e.to_string()))?;

        for i in 0..=self.max_reader_id.load(Ordering::Relaxed) {
            let reader = readers.get_mut(&i).ok_or(anyhow!("index error"))?;
            loop {
                let pos_before = reader.inner.stream_position()? as u32;

                if let std::result::Result::Ok(t) =
                    bson::from_reader::<_, Transaction>(&mut reader.inner)
                {
                    let pos_after = reader.inner.stream_position()? as u32;
                    let t_pos = TransactionPosition {
                        log_reader_id: i,
                        offset: pos_before,
                        len: pos_after - pos_before,
                    };

                    match t {
                        Transaction::Set(k, _) => index.insert(k, t_pos),
                        Transaction::Remove(k) => index.remove(&k),
                    };
                } else {
                    break;
                }
            }
        }

        Ok(())
    }

    // The main purpose of compression is to remove content that is not pointed to by the index.
    // The simplest approach is to iterate through the entire index, copy the indexed content
    // to a newly created compressed file,and update the index to point to the new locations.
    // Finally, all previous files can be deleted.
    // If the index is very large, then this will lock the db for a long time.
    pub fn compress_by_index(&mut self) -> Result<()> {
        let mut readers = self.readers.write().map_err(|e| anyhow!(e.to_string()))?;
        let mut index = self.index.write().map_err(|e| anyhow!(e.to_string()))?;

        let max_reader_id = self.max_reader_id.load(Ordering::Relaxed);
        let compress_log_id = max_reader_id + 1;
        let mut new_offset = 0;

        let mut compress_log_writer = new_log_writer(compress_log_id, &self.path)?;

        for (_, pos) in index.iter_mut() {
            if pos.log_reader_id == max_reader_id {
                continue;
            }

            let reader = readers
                .get_mut(&pos.log_reader_id)
                .ok_or(anyhow!("index has err"))?;
            reader.inner.seek(Start(pos.offset as u64))?;

            let mut buf = vec![0; pos.len as usize];
            reader.inner.read_exact(&mut buf)?;

            let len = io::copy(&mut buf.as_slice(), &mut compress_log_writer.writer)?;
            pos.log_reader_id = compress_log_id;
            pos.offset = new_offset;
            new_offset = new_offset + len as u32;
        }

        compress_log_writer.writer.flush()?;

        let compressed_log_ids: Vec<_> = readers
            .keys()
            .filter(|&&id| id < max_reader_id)
            .cloned()
            .collect();

        for id in compressed_log_ids {
            readers.remove(&id);
            let _ = fs::remove_file(log_path(id, &self.path));
        }

        self.max_reader_id.store(compress_log_id, Ordering::Relaxed);
        readers.insert(
            compress_log_id,
            new_log_reader(compress_log_id, &self.path)?,
        );

        Ok(())
    }

    // A better way to compress logs may be to read the Transaction from the Reader,
    // and then query whether the key exists in the index, so that we can split the
    // compression task into more small tasks.
    // These small tasks can be designed to be parallelized.
    pub fn parallel_compress() {}
}

impl KvsEngine for KVStore {
    fn get(&self, key: String) -> Result<String> {
        let index = self
            .index
            .read()
            .map_err(|_| anyhow!("acquire index read lock failed"))?;
        let mut readers = self
            .readers
            .write()
            .map_err(|_| anyhow!("acquire reader read lock failed"))?;

        let pos = index.get(&key).ok_or(anyhow!("key not found"))?;
        let reader = readers
            .get_mut(&pos.log_reader_id)
            .ok_or(anyhow!("db maybe breaded"))?;

        let mut data = vec![0; pos.len as usize];
        reader.read_exact(pos.offset, &mut data)?;

        match Transaction::from_bytes(&data)? {
            Transaction::Set(_, value) => Ok(value),
            Transaction::Remove(_) => Err(anyhow!("Key not found")),
        }
    }

    fn set(&self, key: String, value: String) -> Result<Option<String>> {
        let mut writer = self.writer.lock().map_err(|e| anyhow!(e.to_string()))?;
        let mut max_reader_id = self.max_reader_id.load(Ordering::Relaxed);

        if writer.pos > LOG_MAX_SIZE {
            let mut readers = self.readers.write().map_err(|e| anyhow!(e.to_string()))?;
            max_reader_id += 1;
            let last_writer = new_log_writer(max_reader_id, &self.path)?;
            let last_reader: BufferReader<File> = new_log_reader(max_reader_id, &self.path)?;

            readers.insert(max_reader_id, last_reader);
            self.max_reader_id.store(max_reader_id, Ordering::Relaxed);

            writer.writer = last_writer.writer;
            writer.pos = 0;
        }

        let old_value = self.get(key.clone()).ok();
        let mut index = self.index.write().map_err(|e| anyhow!(e.to_string()))?;

        let transaction: Transaction = Transaction::Set(key.to_string(), value.to_string());
        let bytes = transaction.to_bytes()?;

        index.insert(
            key.to_string(),
            TransactionPosition {
                log_reader_id: max_reader_id,
                offset: writer.pos,
                len: bytes.len() as u32,
            },
        );
        writer.write(&bytes)?;
        writer.flush()?;

        Ok(old_value)
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut index = self
            .index
            .write()
            .map_err(|_| anyhow!("acquire index read lock failed"))?;

        index.remove(&key).ok_or(anyhow!("Key not found"))?;
        let mut writer = self.writer.lock().map_err(|e| anyhow!(e.to_string()))?;

        let transaction: Transaction = Transaction::Remove(key);
        let bytes = transaction.to_bytes()?;

        writer.write(&bytes)?;
        writer.flush()?;
        Ok(())
    }
}

fn new_log_writer(log_id: u32, path_buf: &PathBuf) -> Result<BufferWriter<File>> {
    File::options()
        .create(true)
        .read(true)
        .append(true)
        .open(log_path(log_id, path_buf))
        .map(|f| BufferWriter::<File> {
            writer: BufWriter::new(f),
            pos: 0,
        })
        .map_err(|e| anyhow!(e))
}

fn new_log_reader(log_id: u32, path_buf: &PathBuf) -> Result<BufferReader<File>> {
    File::open(log_path(log_id, path_buf))
        .map(|f| BufferReader::<File> {
            inner: BufReader::new(f),
        })
        .map_err(|e| anyhow!(e))
}

fn log_path(log_id: u32, path_buf: &PathBuf) -> PathBuf {
    path_buf.join(format!("{}.log", log_id))
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Transaction {
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
    log_reader_id: u32,
    offset: u32,
    len: u32,
}

#[derive(Serialize, Deserialize, Debug)]
struct TransactionIndex {
    key: String,
    transaction_pos: TransactionPosition,
}

pub struct BufferReader<T: Read + Seek> {
    inner: BufReader<T>,
}

impl<T: Read + Seek> BufferReader<T> {
    pub fn read_exact(&mut self, pos: u32, data: &mut [u8]) -> Result<()> {
        self.inner
            .seek(Start(pos as u64))
            .map_err(|e| anyhow!(e.to_string()))?;

        self.inner
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

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(|e| anyhow!(e.to_string()))
    }
}

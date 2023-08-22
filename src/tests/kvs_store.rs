use std::fs::{self};

use anyhow::Result;
use tempfile::TempDir;

use crate::{engine::KvsEngine, kvs::KVStore};

#[test]
fn kvs_engine_new_write_log() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();
    let kv_store = KVStore::new(&path.to_path_buf()).unwrap();
    let mut key_id = 1;

    loop {
        key_id += 1;
        kv_store
            .set(key_id.to_string(), (key_id * 20).to_string())
            .unwrap();

        let mut files = fs::read_dir(&path.join("db"))
            .unwrap()
            .map(|e| e.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();

        if files.len() >= 2 {
            files.sort();
            assert!(files.last().unwrap().ends_with("1.log"));
            break;
        }
    }
    assert!(kv_store
        .get(key_id.to_string())
        .unwrap()
        .eq(&(key_id * 20).to_string()));
}

#[test]
fn kvs_engine_compress() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();
    let mut kv_store = KVStore::new(&path.to_path_buf()).unwrap();
    let mut key_id = 1;
    loop {
        key_id += 1;
        kv_store
            .set(key_id.to_string(), (key_id * 20).to_string())
            .unwrap();

        let mut files = fs::read_dir(&path.join("db"))
            .unwrap()
            .map(|e| e.map(|e| e.path()))
            .collect::<Result<Vec<_>, std::io::Error>>()
            .unwrap();

        if files.len() >= 3 {
            files.sort();
            assert!(files.last().unwrap().ends_with("2.log"));
            break;
        }
    }

    kv_store.compress_by_index().unwrap();
    let mut files = fs::read_dir(&path.join("db"))
        .unwrap()
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, std::io::Error>>()
        .unwrap();

    files.sort();
    assert_eq!(files.len(), 2);
    assert!(files.last().unwrap().ends_with("3.log"));
    assert!(kv_store
        .get(key_id.to_string())
        .unwrap()
        .eq(&(key_id * 20).to_string()));
}

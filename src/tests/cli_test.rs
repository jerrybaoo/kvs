use std::fs::{self};
use std::process::Command;

use anyhow::Result;
use assert_cmd::prelude::*;
use predicates::ord::eq;
use predicates::prelude::*;
use tempfile::TempDir;

use crate::store::KVStore;

#[test]
fn cli_get() -> Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .current_dir(&tmp_dir)
        .arg("get")
        .arg("k1-not-exits")
        .assert()
        .failure()
        .stderr(predicate::str::contains("key not found"));

    Ok(())
}

#[test]
fn cli_set() -> Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .current_dir(&tmp_dir)
        .arg("set")
        .arg("k1")
        .arg("v1")
        .assert()
        .success();

    Ok(())
}

#[test]
fn cli_remove() -> Result<()> {
    let tmp_dir = TempDir::new().unwrap();
    Command::cargo_bin("kvs")
        .unwrap()
        .current_dir(&tmp_dir)
        .arg("rm")
        .arg("k1")
        .assert()
        .success();

    Ok(())
}

#[test]
fn cli_get_exist_value() {
    let tmp_dir = TempDir::new().unwrap();
    let mut kv_store = KVStore::new(&tmp_dir.path().to_path_buf()).unwrap();
    kv_store.set("k1", "v1").unwrap();
    kv_store.set("k2", "v2").unwrap();
    kv_store.set("k3", "v3").unwrap();

    drop(kv_store);

    Command::cargo_bin("kvs")
        .unwrap()
        .arg("get")
        .arg("k1")
        .current_dir(&tmp_dir)
        .assert()
        .success()
        .stdout(eq("key:k1, value:v1").trim());
}

#[test]
fn kvs_new_write_log() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();
    let mut kv_store = KVStore::new(&path.to_path_buf()).unwrap();
    let mut key_id = 1;

    loop {
        key_id += 1;
        kv_store
            .set(&key_id.to_string(), &(key_id * 20).to_string())
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
        .get(&key_id.to_string())
        .unwrap()
        .eq(&(key_id * 20).to_string()));
}

#[test]
fn kvs_compress() {
    let tmp_dir = TempDir::new().unwrap();
    let path = tmp_dir.path();
    let mut kv_store = KVStore::new(&path.to_path_buf()).unwrap();
    let mut key_id = 1;
    loop {
        key_id += 1;
        kv_store
            .set(&key_id.to_string(), &(key_id * 20).to_string())
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
        .get(&key_id.to_string())
        .unwrap()
        .eq(&(key_id * 20).to_string()));
}

#[test]
fn cli_version() -> Result<()> {
    let tmp_dir = TempDir::new().unwrap();

    Command::cargo_bin("kvs")
        .unwrap()
        .arg("V")
        .current_dir(&tmp_dir)
        .assert()
        .success()
        .stdout(predicates::str::contains(env!("CARGO_PKG_VERSION")));

    Ok(())
}

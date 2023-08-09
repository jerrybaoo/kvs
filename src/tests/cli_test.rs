use std::env::current_dir;
use std::process::Command;

use anyhow::Result;
use assert_cmd::prelude::*;
use predicates::prelude::*;
use predicates::ord::eq;

use crate::store::KVStore;

#[test]
fn cli_get() -> Result<()> {
    let mut cmd = Command::cargo_bin("kvs")?;

    cmd.arg("get").arg("k1");
    cmd.assert()
        .failure()
        .stderr(predicate::str::contains("key not found"));

    Ok(())
}

#[test]
fn cli_set() -> Result<()> {
    let mut set_cmd = Command::cargo_bin("kvs")?;

    set_cmd.arg("set").arg("k1").arg("v1");
    set_cmd.assert().success();

    Ok(())
}

#[test]
fn cli_remove() -> Result<()> {
    let mut cmd = Command::cargo_bin("kvs")?;

    cmd.arg("rm").arg("k1");
    cmd.assert().success();

    Ok(())
}

#[test]
fn cli_get_exist_value() {
    let path = current_dir().unwrap();
    let mut kv_store = KVStore::new(&path).unwrap();
    kv_store.set("k1", "v1").unwrap();
    kv_store.set("k2", "v2").unwrap();
    kv_store.set("k3", "v3").unwrap();

    
    drop(kv_store);

    Command::cargo_bin("kvs")
        .unwrap()
        .arg("get")
        .arg("k1")
        .assert()
        .success()
        .stdout(eq("key:k1, value:v1").trim());
}

#[test]
fn cli_version() -> Result<()> {
    let mut cmd = Command::cargo_bin("kvs")?;

    cmd.arg("V");
    cmd.assert()
        .success()
        .stdout(predicates::str::contains(env!("CARGO_PKG_VERSION")));

    Ok(())
}

use assert_cmd::prelude::*;
use predicates::str::{contains, is_empty};
use std::process::Command;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn cli_access_server(engine: &str, addr: &str) {
    let (sender, receiver) = mpsc::sync_channel(0);
    let temp_dir = TempDir::new().unwrap();
    let mut server = Command::cargo_bin("kvs_server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--listen-addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "set", "key1", "value1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout("value1\n");

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "set", "key1", "value2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout("value2\n");

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(contains("Key not found"));

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "rm", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(contains("Key not found"));

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "set", "key2", "value3"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "rm", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(is_empty());

    sender.send(()).unwrap();
    handle.join().unwrap();

    // Reopen and check value
    let (sender, receiver) = mpsc::sync_channel(0);
    let mut server = Command::cargo_bin("kvs_server").unwrap();
    let mut child = server
        .args(&["--engine", engine, "--listen-addr", addr])
        .current_dir(&temp_dir)
        .spawn()
        .unwrap();
    let handle = thread::spawn(move || {
        let _ = receiver.recv(); // wait for main thread to finish
        child.kill().expect("server exited before killed");
    });
    thread::sleep(Duration::from_secs(1));

    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "get", "key2"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(contains("value3"));
    Command::cargo_bin("kvs_client")
        .unwrap()
        .args(&["--addr", addr, "get", "key1"])
        .current_dir(&temp_dir)
        .assert()
        .success()
        .stdout(contains("Key not found"));
    sender.send(()).unwrap();
    handle.join().unwrap();
}

#[test]
fn cli_access_server_kvs_engine() {
    cli_access_server("kvs", "127.0.0.1:11221");
}

#[test]
fn cli_access_server_sled_engine() {
    cli_access_server("sled", "127.0.0.1:10001");
}

use std::sync::{Arc, Mutex};
use std::thread::{self};
use std::time::Duration;

use anyhow::Result;

use crate::thread_pool::NativeThreadPool;

#[test]
fn native_thread_pool_run_jobs() {
    let value = Arc::new(Mutex::new(5u8));
    let value2 = value.clone();
    let value3 = value.clone();

    let expensive_job = move || -> Result<()> {
        thread::sleep(Duration::from_secs(11));
        let mut locked_value = value.lock().unwrap();
        *locked_value = *locked_value + 19;

        Ok(())
    };

    let cheap_job = move || -> Result<()> {
        thread::sleep(Duration::from_secs(10));
        let mut locked_value = value2.lock().unwrap();
        *locked_value = *locked_value + 31;

        Ok(())
    };

    let mut thread_pool = NativeThreadPool::new(10);
    thread_pool.spawn(expensive_job);
    thread_pool.spawn(cheap_job);

    thread_pool.wait_then_close();

    assert_eq!(*value3.lock().unwrap(), 55);
}

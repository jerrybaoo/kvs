use std::thread;

use anyhow::Result;

use crate::thread_pool::ThreadPool;

pub struct NativeThreadPool {}

impl ThreadPool for NativeThreadPool {
    fn new(_num: u32) -> Result<Self> {
        Ok(NativeThreadPool {})
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(move || job());
    }
}

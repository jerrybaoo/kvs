use std::sync::mpsc::{self, Receiver};
use std::thread::{self};

use anyhow::Result;

pub struct NativeThreadPool {
    receivers: Vec<Receiver<bool>>,
}

impl NativeThreadPool {
    pub fn new(_num: usize) -> Self {
        NativeThreadPool {
            receivers: Vec::new(),
        }
    }

    pub fn spawn<F>(&mut self, job: F)
    where
        F: FnOnce() -> Result<()> + Send + 'static,
    {
        let (send, recv) = mpsc::channel();
        thread::spawn(move || {
            match job() {
                Ok(_) => {}
                Err(e) => log::error!("job has error: {}", e),
            };
            send.send(true).expect("send failed");
        });
        self.receivers.push(recv);
    }

    pub fn wait_then_close(&self) {
        for recv in &self.receivers {
            if let Err(e) = recv.recv() {
                log::error!("thread pool receive failed, reason: {}", e);
            }
        }
    }
}

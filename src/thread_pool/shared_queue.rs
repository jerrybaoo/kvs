use std::thread;

use crossbeam::channel::{unbounded, Receiver, Sender};

use crate::thread_pool::ThreadPool;

pub struct SharedQueueThreadPool {
    sender: Sender<ThreadPoolMessage>,
}

pub enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(num: u32) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = unbounded::<ThreadPoolMessage>();

        for _ in 0..num {
            let recv = receiver.clone();
            let job_receiver = JobReceiver(recv);
            thread::spawn(move || run_job(&job_receiver));
        }

        Ok(SharedQueueThreadPool { sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let _ = self.sender.send(ThreadPoolMessage::RunJob(Box::new(job)));
    }
}

struct JobReceiver(Receiver<ThreadPoolMessage>);

impl Drop for JobReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            run_job(self)
        }
    }
}

fn run_job(job_receiver: &JobReceiver) {
    loop {
        match job_receiver.0.recv() {
            Ok(msg) => match msg {
                ThreadPoolMessage::RunJob(job) => job(),
                ThreadPoolMessage::Shutdown => break,
            },
            Err(e) => log::error!("job receiver has error: {}", e),
        }
    }
}

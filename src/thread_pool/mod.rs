use anyhow::Result;

pub mod native;
pub mod rayon;
pub mod shared_queue;

pub trait ThreadPool {
    fn new(num: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

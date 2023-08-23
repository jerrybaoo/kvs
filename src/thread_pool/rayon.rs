use anyhow::{anyhow, Result};

use crate::thread_pool::ThreadPool;

pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(num: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num as usize)
            .build()
            .map_err(|e| anyhow!(e))?;
        Ok(RayonThreadPool(pool))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(job)
    }
}

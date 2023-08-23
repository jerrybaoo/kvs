use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use crossbeam::sync::WaitGroup;

use crate::thread_pool::ThreadPool;
use crate::thread_pool::native::NativeThreadPool;
use crate::thread_pool::shared_queue::SharedQueueThreadPool;
use crate::thread_pool::rayon::RayonThreadPool;

#[test]
fn native_thread_pool_run_jobs() {
    let thread_pool = NativeThreadPool::new(10).unwrap();
    multi_thread_jobs(thread_pool, 32)
}

#[test]
fn shared_queue_pool_run_jobs() {
    let thread_pool = SharedQueueThreadPool::new(16).unwrap();
    multi_thread_jobs(thread_pool, 32)
}

#[test]
fn shared_queue_pool_run_panic_jobs() {
    let thread_pool = SharedQueueThreadPool::new(12).unwrap();
    panic_jobs(thread_pool, 12)
}

#[test]
fn rayon_thread_pool_run_jobs(){
    let thread_pool = RayonThreadPool::new(16).unwrap();
    multi_thread_jobs(thread_pool, 12)
}

// #[test]
// fn rayon_thread_pool_run_panic_jobs(){
//     let thread_pool = RayonThreadPool::new(16).unwrap();
//     panic_jobs(thread_pool, 12)
// }

fn multi_thread_jobs<T: ThreadPool>(pool: T, jobs: u32) {
    let value = Arc::new(AtomicU32::new(10));
    let wg = WaitGroup::new();

    for _ in 0..jobs {
        let wg1 = wg.clone();
        let v1 = value.clone();
        pool.spawn(move || {
            v1.fetch_add(10, Ordering::Relaxed);
            drop(wg1)
        });
    }
    wg.wait();

    assert_eq!(value.load(Ordering::Relaxed), 10 + jobs * 10);
}

fn panic_jobs<T: ThreadPool>(pool: T, jobs: u32) {
    let value = Arc::new(AtomicU32::new(10));
    let wg = WaitGroup::new();

    for i in 0..jobs {
        let wg1 = wg.clone();
        let v1 = value.clone();
        pool.spawn(move || {
            if i % 2 == 0 {
                drop(wg1);
                panic_control::disable_hook_in_current_thread();
                panic!("panic jobs");
            }

            v1.fetch_add(10, Ordering::Relaxed);
            drop(wg1)
        });
    }
    wg.wait();
    assert_eq!(value.load(Ordering::Relaxed), 10 + (jobs / 2) * 10);
}

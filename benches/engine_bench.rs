use criterion::{criterion_group, criterion_main, Criterion};
use rand::{distributions::Alphanumeric, Rng};
use tempfile::TempDir;

use kvs::{engine::KvsEngine, kvs::KVStore, sled::Sled};

fn generate_random_string(length: usize) -> String {
    let mut rng = rand::thread_rng();
    let len = rng.gen_range(1..length);
    (&mut rng)
        .sample_iter(Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

pub fn bench_write(c: &mut Criterion) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for _ in 0..100 {
        keys.push(generate_random_string(100000));
        values.push(generate_random_string(100000));
    }

    let tmp_dir = TempDir::new().unwrap();
    let sled = Sled::new(&tmp_dir.path().to_path_buf()).unwrap();

    let kvs_tmp_dir = TempDir::new().unwrap();
    let kvs_store = KVStore::new(&kvs_tmp_dir.path().to_path_buf()).unwrap();

    let mut group: criterion::BenchmarkGroup<'_, criterion::measurement::WallTime> =
        c.benchmark_group("get_write");

    group.bench_function("sled_write", |b| {
        b.iter(|| {
            keys.iter().enumerate().for_each(|(index, elem)| {
                sled.set(elem.to_owned(), values[index].to_owned()).unwrap();
            })
        })
    });

    group.bench_function("kvs_write", |b| {
        b.iter(|| {
            keys.iter().enumerate().for_each(|(index, elem)| {
                let _ = kvs_store.set(elem.to_owned(), values[index].to_owned());
            })
        })
    });

    group.finish();
}

pub fn bench_read(c: &mut Criterion) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    for _ in 0..100 {
        keys.push(generate_random_string(1000));
        values.push(generate_random_string(10000));
    }

    let tmp_dir = TempDir::new().unwrap();
    let sled = Sled::new(&tmp_dir.path().to_path_buf()).unwrap();
    keys.iter().enumerate().for_each(|(index, elem)| {
        sled.set(elem.to_owned(), values[index].to_owned()).unwrap();
    });

    let kvs_tmp_dir = TempDir::new().unwrap();
    let kvs_store = KVStore::new(&kvs_tmp_dir.path().to_path_buf()).unwrap();
    keys.iter().enumerate().for_each(|(index, elem)| {
        kvs_store
            .set(elem.to_owned(), values[index].to_owned())
            .unwrap();
    });

    let mut group: criterion::BenchmarkGroup<'_, criterion::measurement::WallTime> =
        c.benchmark_group("get_read");

    group.bench_function("sled_read", |b| {
        b.iter(|| {
            keys.iter().for_each(|elem| {
                sled.get(elem.to_owned()).unwrap();
            })
        })
    });

    group.bench_function("kvs_read", |b| {
        b.iter(|| {
            keys.iter().for_each(|elem| {
                kvs_store.get(elem.to_owned()).unwrap();
            })
        })
    });

    group.finish();
}

criterion_group!(benches, bench_write, bench_read);
criterion_main!(benches);

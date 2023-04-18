use criterion::{criterion_group, criterion_main};

mod grace_hashjoin_bench;

criterion_group!(benches, grace_hashjoin_bench::grace_hashjoin_benchmark);
criterion_main!(benches);

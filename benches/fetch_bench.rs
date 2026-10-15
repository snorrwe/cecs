//! Criterion benchmarks for random access queries (`fetch`, `fetch_mut`, `contains`,
//! `World::get_component`).
//!
//! Run with:
//!   cargo bench --bench fetch_bench
//!
//! Each iteration performs `BATCH` random accesses over pre-seeded entity ids, so
//! the reported time is for a full batch; `Throughput::Elements` additionally reports
//! elements/second.

use std::hint::black_box;

use cecs::prelude::*;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

macro_rules! components {
    ($($x:ident),*) => {
        $(
            #[derive(Clone, Copy)]
            #[allow(dead_code)]
            struct $x(pub u64);
        )*
    };
}

components!(C1, C2, C3, C4, C5, C6, C7, C8);

/// Number of random accesses per benchmark iteration.
const BATCH: usize = 1 << 16;

/// Bundle inserted into every spawned entity.
const BUNDLE: (C1, C2, C3, C4, C5, C6, C7, C8) =
    (C1(1), C2(2), C3(3), C4(4), C5(5), C6(6), C7(7), C8(8));

macro_rules! bench_query {
    ($group:expr, $name:literal, $n:expr, $access:expr, $world:expr, $query:ty, $method:ident) => {
        $group.bench_with_input(
            BenchmarkId::new($name, $n),
            &$access,
            |b, access| {
                #[allow(unused_mut)] // `fetch_mut` needs the mutable binding, others don't
                let mut q = <$query>::new(&$world);
                b.iter(|| {
                    for &id in access {
                        black_box(q.$method(id));
                    }
                });
            },
        );
    };
}

fn random_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_access");
    group.throughput(Throughput::Elements(BATCH as u64));

    for n in [1 << 10, 1 << 16] {
        let mut world = World::new(n as u32);
        let mut ids = Vec::with_capacity(n);

        world
            .run_system(|mut cmd: Commands| {
                for _ in 0..n {
                    ids.push(cmd.spawn().insert_bundle(BUNDLE).id().unwrap());
                }
            })
            .unwrap();

        // Pre-generate the random access pattern so it isn't part of the timed region.
        let mut rng = fastrand::Rng::with_seed(0xC0FFEE);
        let access: Vec<EntityId> = (0..BATCH).map(|_| ids[rng.usize(..ids.len())]).collect();

        bench_query!(group, "fetch::<&C1>", n, access, world, Query<&C1>, fetch);
        bench_query!(
            group,
            "fetch::<(&C1,&C2,&C3)>",
            n,
            access,
            world,
            Query<(&C1, &C2, &C3)>,
            fetch
        );
        bench_query!(group, "fetch_mut::<&mut C1>", n, access, world, Query<&mut C1>, fetch_mut);
        bench_query!(group, "contains::<&C1>", n, access, world, Query<&C1>, contains);

        group.bench_with_input(
            BenchmarkId::new("world.get_component::<C1>", n),
            &access,
            |b, access| {
                b.iter(|| {
                    for &id in access {
                        black_box(world.get_component::<C1>(id));
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, random_access);
criterion_main!(benches);

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;

use cecs::prelude::*;

macro_rules! components {
    ($ ($x: ident),*) => {

        $(
        #[derive(Clone)]
        #[allow(unused)]
        struct $x (pub [u8; 32]);
        )*

        fn add_entity<'a>(cmd: &'a mut Commands) -> &'a mut EntityCommands {
            let mut cmd = cmd.spawn();
            $(
                if fastrand::bool() {
                    cmd = cmd.insert(
                        $x([42; 32])
                    );
                }
            )*
            cmd
        }

        fn make_stage() -> SystemStage<'static> {
            SystemStage::new("test")
            $(
                .with_system(move |_q: Query<&mut $x>| {})
            )*
            .build()
        }

    };
}

components!(
    C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16, C17, C18, C19, C20, C21,
    C22, C23, C24, C25, C26, C27, C28, C29, C30, C31, C32, C33, C34, C35, C36, C37, C38, C39, C40,
    C41, C42, C43, C44, C45, C46, C47, C48, C49, C50
);

#[derive(Clone, Copy)]
struct TestComponent(pub u64);

fn benchmark_queries(c: &mut Criterion) {
    fastrand::seed(0xdeadbeef);
    let mut group = c.benchmark_group("query");

    for n in (10..=14).step_by(2) {
        let n = 1 << n;
        let mut world = World::new(n);

        world
            .run_system(|mut cmd: Commands| {
                for _ in 0..n {
                    add_entity(&mut cmd).insert(TestComponent(0));
                }
            })
            .unwrap();

        group.bench_with_input(BenchmarkId::new("mutate-single-serial", n), &n, |b, _n| {
            b.iter(|| {
                world
                    .run_system(|mut q: Query<&mut TestComponent>| {
                        for c in q.iter_mut() {
                            c.0 = 0xBEEF;
                        }
                    })
                    .unwrap();

                black_box(&world);
            });
        });

        group.bench_with_input(
            BenchmarkId::new("mutate-single-parallel", n),
            &n,
            |b, _n| {
                b.iter(|| {
                    world
                        .run_system(|mut q: Query<&mut TestComponent>| {
                            q.par_for_each_mut(|c| {
                                c.0 = 0xBEEF;
                            });
                        })
                        .unwrap();

                    black_box(&world);
                });
            },
        );
    }
}

fn benchmark_systems(c: &mut Criterion) {
    let mut group = c.benchmark_group("system_stages");

    group.bench_function("tick", |b| {
        let mut world = World::new(16);
        world.add_stage(make_stage());
        b.iter(|| {
            world.tick();
        });
    });

    group.bench_function("reuse-stage", |b| {
        let mut world = World::new(16);
        let mut stage = make_stage();
        b.iter(move || {
            stage = world.run_stage(std::mem::take(&mut stage)).unwrap();
        });
    });

    group.bench_function("clone-stage", |b| {
        let mut world = World::new(16);
        let stage = make_stage();
        b.iter(move || {
            world.run_stage(stage.clone()).unwrap();
        });
    });
}

criterion_group!(query_benches, benchmark_queries);
criterion_group!(systems_benches, benchmark_systems);
criterion_main!(query_benches, systems_benches);

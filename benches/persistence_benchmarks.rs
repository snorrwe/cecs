#![recursion_limit = "256"]
use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};

use cecs::{
    World,
    prelude::*,
    serde::{WorldPersister, WorldSerializer},
};

macro_rules! components {
    ($ ($x: ident),*) => {

        $(
        #[derive(Clone, serde::Deserialize, serde::Serialize)]
        struct $x (pub [u8; 32]);
        )*

        fn add_entity(cmd: &mut Commands) {
            cmd.spawn()
            $(
                .insert(
                    $x([42; 32])
                )
            )*;
        }

        fn persister() -> impl WorldSerializer {
            WorldPersister::new()
            $(
                .with_component::<$x>()
            )*
        }

    };
}

components!(
    C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, C11, C12, C13, C14, C15, C16, C17, C18, C19, C20, C21,
    C22, C23, C24, C25, C26, C27, C28, C29, C30, C31, C32, C33, C34, C35, C36, C37, C38, C39, C40,
    C41, C42, C43, C44, C45, C46, C47, C48, C49, C50, C51, C52, C53, C54, C55, C56, C57, C58, C59,
    C60, C61, C62, C63, C64, C65, C66, C67, C68, C69, C70, C71, C72, C73, C74, C75, C76, C77, C78,
    C79, C80, C81, C82, C83, C84, C85, C86, C87, C88, C89, C90, C91, C92, C93, C94, C95, C96, C97,
    C98, C99, C100
);

fn benchmark_loading(c: &mut Criterion) {
    let mut group = c.benchmark_group("persister-loading");

    for n in [100, 200, 500, 1000] {
        let mut world = World::new(1024);

        let persister = persister();

        world
            .run_system(|mut cmd: Commands| {
                for _ in 0..n {
                    add_entity(&mut cmd);
                }
            })
            .unwrap();

        group.bench_with_input(BenchmarkId::new("bincode", n), &n, |b, _n| {
            let mut payload = Vec::<u8>::new();
            let mut s =
                bincode::Serializer::new(&mut payload, bincode::config::DefaultOptions::new());
            persister.save(&mut s, &world).unwrap();
            b.iter(|| {
                let world = persister
                    .load(&mut bincode::de::Deserializer::from_slice(
                        payload.as_slice(),
                        bincode::config::DefaultOptions::new(),
                    ))
                    .unwrap();

                black_box(&world);
            });
        });

        group.bench_with_input(BenchmarkId::new("json", n), &n, |b, _n| {
            let mut payload = Vec::<u8>::new();
            let mut s = serde_json::ser::Serializer::new(&mut payload);
            persister.save(&mut s, &world).unwrap();
            b.iter(|| {
                let world = persister
                    .load(&mut serde_json::Deserializer::from_slice(
                        payload.as_slice(),
                    ))
                    .unwrap();

                black_box(&world);
            });
        });
    }
}

fn benchmark_saving(c: &mut Criterion) {
    let mut group = c.benchmark_group("persister-saving");

    for n in [100, 200, 500, 1000] {
        let mut world = World::new(1024);

        let persister = persister();

        world
            .run_system(|mut cmd: Commands| {
                for _ in 0..n {
                    add_entity(&mut cmd);
                }
            })
            .unwrap();

        group.bench_with_input(BenchmarkId::new("bincode", n), &n, |b, _n| {
            let mut payload = Vec::<u8>::new();
            b.iter(|| {
                payload.clear();
                let mut s =
                    bincode::Serializer::new(&mut payload, bincode::config::DefaultOptions::new());
                persister.save(&mut s, &world).unwrap();
                black_box(&payload);
            });
        });

        group.bench_with_input(BenchmarkId::new("json", n), &n, |b, _n| {
            let mut payload = Vec::<u8>::new();
            b.iter(|| {
                let mut s = serde_json::ser::Serializer::new(&mut payload);
                persister.save(&mut s, &world).unwrap();
                black_box(&payload);
            });
        });
    }
}

criterion_group!(persister_benches, benchmark_loading, benchmark_saving);
criterion_main!(persister_benches);

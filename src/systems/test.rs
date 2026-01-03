use crate::{World, prelude::ResMut, systems::IntoSystem};

use super::SystemStage;

#[test]
fn test_unordered_systems_keep_the_original_ordering() {
    let mut stage = SystemStage::new("test");

    stage.add_system(move |mut c: ResMut<Vec<usize>>| c.push(0));
    stage.add_system(move |mut c: ResMut<Vec<usize>>| c.push(1));
    stage.add_system(move |mut c: ResMut<Vec<usize>>| c.push(2));
    stage.add_system(move |mut c: ResMut<Vec<usize>>| c.push(3));
    stage.add_system(move |mut c: ResMut<Vec<usize>>| c.push(4));

    let stage = stage.build();

    let mut w = World::new(0);
    w.insert_resource(Vec::<usize>::with_capacity(5));

    w.run_stage(stage);

    let nums = w.get_resource::<Vec<usize>>().unwrap();

    assert_eq!(nums.len(), 5);

    for (i, j) in nums.iter().enumerate() {
        assert_eq!(i, *j);
    }
}

#[derive(Debug, Clone)]
#[cfg(feature = "parallel")]
struct Lock(pub bool);

#[cfg(feature = "parallel")]
fn test_lock(l: &mut Lock) {
    assert!(!l.0);
    l.0 = true;
    std::thread::yield_now();
    assert!(l.0);
    l.0 = false;
    assert!(!l.0);
}

/// Test that systems do not contend on ResMut resources by using a shared 'lock' resource that's
/// checked for locking, then locked, then the thread yields, to give a better chance for competing
/// threads, then checks again, then unlocks
#[cfg(feature = "parallel")]
#[test]
fn test_resmut_are_not_shared() {
    let mut stage = SystemStage::new("test");

    stage.add_system(move |mut c: ResMut<Lock>| {
        test_lock(&mut c);
    });
    stage.add_system(move |mut c: ResMut<Lock>| {
        test_lock(&mut c);
    });
    stage.add_system(move |mut c: ResMut<Lock>| {
        test_lock(&mut c);
    });
    stage.add_system(move |mut c: ResMut<Lock>| {
        test_lock(&mut c);
    });
    stage.add_system(move |mut c: ResMut<Lock>| {
        test_lock(&mut c);
    });

    let stage = stage.build();

    let mut w = World::new(0);
    w.insert_resource(Lock(false));

    w.run_stage(stage);
}

#[derive(Debug, Clone, Copy, Default)]
struct Counter(i32);

#[test]
fn test_nested_stage_should_run() {
    let mut w = World::new(0);
    w.insert_resource(Counter(0));

    let stage = SystemStage::new("root")
        .with_should_run(|| true)
        .with_system(|mut c: ResMut<Counter>| {
            c.0 += 1;
        })
        .with_nested_stage(
            SystemStage::new("nested")
                .with_should_run(|| false)
                .with_system(|| unreachable!()),
        );

    w.run_stage(stage);

    let c: &Counter = w.get_resource().unwrap();

    assert_eq!(c.0, 1);
}

#[test]
fn test_nested_stage_parent_falsy_should_run_disabled_child_systems() {
    let mut w = World::new(0);

    let stage = SystemStage::new("root")
        .with_should_run(|| false)
        .with_system(|| unreachable!())
        .with_nested_stage(SystemStage::new("nested").with_system(|| unreachable!()));

    w.run_stage(stage);
}

#[test]
fn test_nested_stage() {
    let mut w = World::new(0);
    w.insert_resource(Counter(0));

    let stage = SystemStage::new("root")
        .with_system(|mut c: ResMut<Counter>| {
            c.0 += 1;
        })
        .with_nested_stage(
            SystemStage::new("nested").with_system(|mut c: ResMut<Counter>| {
                c.0 += 1;
            }),
        )
        .build();

    assert_eq!(stage.systems.len(), 2);

    w.run_stage(stage);

    let c: &Counter = w.get_resource().unwrap();

    assert_eq!(c.0, 2);
}

/// The system should_run masks should work even if the should_run systems are reordered
#[test]
fn test_nested_stage_should_run_with_reorder() {
    let mut w = World::new(0);
    w.insert_resource(Counter(0));

    fn should_always_run() -> bool {
        true
    }
    fn should_never_run() -> bool {
        false
    }

    let stage = SystemStage::new("root")
        .with_should_run(should_always_run.after(should_never_run))
        .with_system(|mut c: ResMut<Counter>| {
            c.0 += 1;
        })
        .with_nested_stage(
            SystemStage::new("nested")
                .with_should_run(should_never_run)
                .with_system(|| unreachable!()),
        );

    w.run_stage(stage);

    let c: &Counter = w.get_resource().unwrap();

    assert_eq!(c.0, 1);
}

/// The system should_run masks should respect siblings
#[test]
fn test_nested_stage_should_run_with_siblings() {
    let mut w = World::new(0);
    w.insert_resource(Counter(0));

    let stage = SystemStage::new("root")
        .with_should_run(|| true)
        .with_system(|mut c: ResMut<Counter>| {
            c.0 += 1;
        })
        .with_nested_stage(
            SystemStage::new("nested1")
                .with_should_run(|| false)
                .with_system(|| unreachable!()),
        )
        .with_nested_stage(
            SystemStage::new("nested2")
                .with_should_run(|| false)
                .with_system(|| unreachable!()),
        )
        .with_nested_stage(
            SystemStage::new("nested3")
                .with_should_run(|| true)
                .with_system(|mut c: ResMut<Counter>| {
                    c.0 += 1;
                }),
        );

    w.run_stage(stage);

    let c: &Counter = w.get_resource().unwrap();

    assert_eq!(c.0, 2);
}

use crate::{World, prelude::ResMut};

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

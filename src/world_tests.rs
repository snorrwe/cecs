use commands::Commands;
use world_access::WorldAccess;

use crate::prelude::{IntoSystem as _, ResMut};
use crate::query::resource_query::Res;
use crate::query::{Query, filters::WithOut};
use crate::table::ArchetypeHash;

use super::*;

#[test]
fn world_deinit_test() {
    let mut w = World::new(500);
    let _id = w.insert_entity();
}

#[test]
fn can_insert_component_test() {
    let mut w = World::new(500);

    let id = w.insert_entity();
    w.set_component(id, "poggers".to_string()).unwrap();
    w.set_component(id, 32u32).unwrap();
    w.set_component(id, 42u64).unwrap();

    let id2 = w.insert_entity();
    w.set_component(id2, 1u32).unwrap();
    w.set_component(id2, "poggers2".to_string()).unwrap();
    w.delete_entity(id2).unwrap();

    let id3 = w.insert_entity();
    w.set_component(id3, 2u32).unwrap();
    w.set_component(id3, "poggers3".to_string()).unwrap();
    w.remove_component::<String>(id3).unwrap();
    w.delete_entity(id3).unwrap();
}

#[test]
fn can_remove_component_test() {
    let mut w = World::new(500);

    let id = w.insert_entity();
    w.set_component(id, 2u32).unwrap();
    w.set_component(id, "poggers3".to_string()).unwrap();

    assert!(w.get_component::<String>(id).unwrap() == "poggers3");

    w.remove_component::<String>(id).unwrap();
    let res = w.get_component::<String>(id);
    assert!(res.is_none(), "Expected none, got: {:?}", res);
}

#[test]
fn can_update_component_test() {
    let mut w = World::new(500);

    let id = w.insert_entity();
    w.set_component(id, "poggers3".to_string()).unwrap();
    assert!(w.get_component::<String>(id).unwrap() == "poggers3");

    w.set_component(id, "poggers2".to_string()).unwrap();
    assert!(w.get_component::<String>(id).unwrap() == "poggers2");
}

#[test]
fn query_can_iter_multiple_archetypes_test() {
    let mut world = World::new(500);

    let id = world.insert_entity();
    world.set_component(id, "poggers".to_string()).unwrap();
    world.set_component(id, 16).unwrap();

    let id = world.insert_entity();
    world.set_component(id, "poggers".to_string()).unwrap();

    let mut count = 0;
    for pog in Query::<&String>::new(&world).iter() {
        assert_eq!(*pog, "poggers");
        count += 1;
    }
    assert_eq!(count, 2);

    // test if compiles
    Query::<(&u32, &String)>::new(&world);
    Query::<(&mut u32, &String)>::new(&world);
    Query::<(&u32, &mut String)>::new(&world);
    Query::<(&mut String, &u32)>::new(&world);
}

#[test]
fn can_query_entity_id_test() {
    let mut world = World::new(500);

    let id1 = world.insert_entity();
    world.set_component(id1, "poggers1".to_string()).unwrap();
    world.set_component(id1, 16).unwrap();

    let id2 = world.insert_entity();
    world.set_component(id2, "poggers2".to_string()).unwrap();

    let mut exp = vec![(id1, "poggers1"), (id2, "poggers2")];
    exp.sort_by_key(|(id, _)| *id);

    let q = Query::<(EntityId, &String)>::new(&world);
    let mut act = q.iter().map(|(id, s)| (id, s.as_str())).collect::<Vec<_>>();
    act.sort_by_key(|(id, _)| *id);

    assert_eq!(&exp[..], &act[..]);

    // test if compiles
    Query::<EntityId>::new(&world);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Foo {
    value: i32,
}

#[test]
fn system_test() {
    fn my_system(mut q: Query<(&mut Foo, EntityId)>) {
        for (foo, _id) in q.iter_mut() {
            foo.value = 69;
        }
    }

    let mut world = World::new(500);

    for i in 0..4 {
        let id = world.insert_entity();
        world.set_component(id, Foo { value: i }).unwrap();
        if i % 2 == 0 {
            world.set_component(id, "poggers".to_string()).unwrap();
        }
        dbg!(id, i);
    }

    my_system(Query::new(&world));

    for foo in Query::<&Foo>::new(&world).iter() {
        assert_eq!(foo.value, 69);
    }
}

#[test]
fn can_fetch_single_entity_test() {
    let mut world = World::new(500);

    for i in 0..4 {
        let id = world.insert_entity();
        world.set_component(id, Foo { value: i }).unwrap();
        if i % 2 == 0 {
            world.set_component(id, "poggers".to_string()).unwrap();
        }
    }
    let id = world.insert_entity();
    world.set_component(id, Foo { value: 0xbeef }).unwrap();
    world.set_component(id, "winnie".to_string()).unwrap();
    for i in 0..4 {
        let id = world.insert_entity();
        world.set_component(id, Foo { value: i }).unwrap();
        if i % 2 == 0 {
            world.set_component(id, "poggers".to_string()).unwrap();
        }
        dbg!(id, i);
    }

    let mut q = Query::<(&mut Foo, &String)>::new(&world);
    let (foo, s) = q.fetch_mut(id).unwrap();

    assert_eq!(foo.value, 0xbeef);
    foo.value = 0;
    assert_eq!(s, "winnie");
}

#[test]
fn optional_query_test() {
    let mut world = World::new(500);

    for i in 0..4 {
        let id = world.insert_entity();
        world.set_component(id, Foo { value: i }).unwrap();
        if i % 2 == 0 {
            world.set_component(id, "poggers".to_string()).unwrap();
        }
    }

    let cnt = Query::<(&Foo, Option<&String>)>::new(&world).iter().count();
    assert_eq!(cnt, 4);

    // assert compiles
    Query::<(&Foo, Option<&mut String>)>::new(&world);
}

#[test]
#[cfg(feature = "parallel")]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn test_parallel() {
    let mut world = World::new(500);

    for i in 0..500 {
        let id = world.insert_entity();
        world.set_component(id, Foo { value: 0 }).unwrap();
        if i % 2 == 0 {
            world.set_component(id, "poggers".to_string()).unwrap();
        }
    }

    fn par_sys(mut q: Query<(&mut Foo, &String)>) {
        let i = 1;
        q.par_for_each_mut(|(foo, _)| {
            foo.value += i;
        });
    }

    fn asserts(q: Query<(&Foo, Option<&String>)>) {
        q.iter().for_each(|(f, s)| {
            if s.is_some() {
                assert_eq!(f.value, 1);
            } else {
                assert_eq!(f.value, 0);
            }
        })
    }

    world.run_system(par_sys).unwrap();
    world.run_system(asserts).unwrap();
}

#[test]
#[cfg(feature = "clone")]
fn world_clone_test() {
    let mut world = World::new(500);

    for i in 0..4 {
        let id = world.insert_entity();
        world.set_component(id, Foo { value: i }).unwrap();
        if i % 2 == 0 {
            world.set_component(id, "poggers".to_string()).unwrap();
        }
    }

    let w2 = world.clone();

    let a = Query::<(EntityId, &Foo, Option<&String>)>::new(&world);
    let b = Query::<(EntityId, &Foo, Option<&String>)>::new(&w2);

    for (a, b) in a.iter().zip(b.iter()) {
        assert_eq!(a, b);
    }
}

#[test]
fn filtered_query_test() {
    let mut world = World::new(500);

    for i in 0..4 {
        let id = world.insert_entity();
        world.set_component(id, Foo { value: i }).unwrap();
        if i % 2 == 0 {
            world.set_component(id, "poggers".to_string()).unwrap();
        }
    }

    for i in Query::<&Foo, WithOut<String>>::new(&world).iter() {
        assert!(i.value % 2 == 1);
    }
}

#[test]
fn resource_test() {
    let mut world = World::new(4);
    world.insert_resource(4i32);

    let res = world.get_resource::<i32>().unwrap();
    assert_eq!(res, &4);

    world.remove_resource::<i32>();

    assert!(world.get_resource::<i32>().is_none());
}

#[test]
fn resource_query_test() {
    let mut world = World::new(4);
    world.insert_resource(4i32);

    fn sys(res: Res<i32>) {
        assert_eq!(*res, 4i32);
    }

    sys(Res::new(&world));
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn world_execute_systems_test() {
    let mut world = World::new(400);

    for i in 0..400 {
        let id = world.insert_entity();
        world.set_component(id, Foo { value: i }).unwrap();
        if i % 2 == 0 {
            world.set_component(id, "poggers".to_string()).unwrap();
        }
    }

    fn sys0(mut q: Query<(&mut Foo, &())>) {
        for (foo, _) in q.iter_mut() {
            foo.value = 42;
        }
    }

    fn assert_sys(q: Query<(&Foo, &())>) {
        for (foo, _) in q.iter() {
            assert_eq!(foo.value, 42);
        }
    }

    world.add_stage(
        SystemStage::new("many_systems")
            .with_system(sys0)
            .with_system(assert_sys.after(sys0)),
    );

    world.tick();

    world.run_system(assert_sys).unwrap();

    world
        .run_stage(SystemStage::new("").with_system(assert_sys))
        .unwrap();
}

#[test]
fn can_skip_stage_test() {
    let mut world = World::new(4);

    world.insert_resource(0i32);

    fn system(mut q: ResMut<i32>) {
        *q += 1i32;
    }

    fn should_not_run() -> bool {
        false
    }

    world
        .run_stage(
            SystemStage::new("instant-run")
                .with_should_run(should_not_run)
                .with_system(system),
        )
        .unwrap();

    world.add_stage(
        SystemStage::new("tick-run")
            .with_should_run(should_not_run)
            .with_system(system),
    );

    world.tick();

    assert_eq!(world.get_resource::<i32>().unwrap(), &0);
}

#[test]
fn borrowing_same_type_const_twice_is_ok_test() {
    fn sys(_valid_query1: Query<(&i32, &i32)>, _valid_query2: Query<(&i32, &i32)>) {}

    let mut world = World::new(1);

    world.run_system(sys).unwrap();
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn invalid_query_panics_double_mut_test() {
    fn sys(_invalid_query: Query<(&mut i32, &mut i32)>) {}

    let mut world = World::new(1);

    world.run_system(sys).unwrap();
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn invalid_query_panics_test() {
    fn sys(_invalid_query: Query<(&i32, &mut i32)>) {}

    let mut world = World::new(1);

    world.run_system(sys).unwrap();
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn borrowing_same_type_mutable_twice_panics_test() {
    fn sys(_valid_query_1: Query<&mut i32>, _valid_query_2: Query<&mut i32>) {}

    let mut world = World::new(1);

    world.run_system(sys).unwrap();
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn borrowing_same_resource_mutable_twice_panics_test() {
    fn sys(_valid_query_1: Res<i32>, _valid_query_2: ResMut<i32>) {}

    let mut world = World::new(1);

    world.run_system(sys).unwrap();
}

#[test]
fn can_iterate_over_immutable_iter_of_refmut_component_test() {
    fn sys(q: Query<(&mut i32, &u32)>) {
        for (a, _b) in q.iter() {
            assert_eq!(a, &69);
        }
    }

    let mut world = World::new(1);
    world.run_system(sys).unwrap();
}

#[test]
fn can_insert_bundle_test() {
    let mut world = World::new(2);

    let entity_id = world.insert_entity();
    let entity_id2 = world.insert_entity();

    world.set_bundle(entity_id, (42i32, 38u32)).unwrap();
    world
        .set_bundle(entity_id2, ("exo".to_string(), "poggers", 62i32, 88u32))
        .unwrap();

    let a = world.get_component::<i32>(entity_id).unwrap();
    assert_eq!(a, &42);

    let a = world.get_component::<u32>(entity_id).unwrap();
    assert_eq!(a, &38);
}

#[test]
fn can_insert_bundle_via_command_test() {
    let mut world = World::new(2);

    fn sys(mut cmd: Commands) {
        cmd.spawn().insert_bundle((42i32, 38u32));
    }

    world.run_system(sys).unwrap();

    for (a, b) in Query::<(&u32, &i32)>::new(&world).iter() {
        assert_eq!(a, &38);
        assert_eq!(b, &42);
    }
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn fetching_same_resource_twice_mutable_is_panic_test() {
    let mut world = World::new(0);
    world.insert_resource(0i32);

    fn bad_sys(_r0: ResMut<i32>, _r1: ResMut<i32>) {}

    world.run_system(bad_sys).unwrap();
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn fetching_same_resource_twice_is_panic_test() {
    let mut world = World::new(0);
    world.insert_resource(0i32);

    fn bad_sys(_r0: ResMut<i32>, _r1: Res<i32>) {}

    world.run_system(bad_sys).unwrap();
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn mutating_world_inside_system_test() {
    fn mutation(mut access: WorldAccess) {
        let w = access.world_mut();
        for _ in 0..100 {
            w.add_stage(SystemStage::new("kekw").with_system(|| {}));
        }
        for _ in 0..5 {
            let id = w.insert_entity();
            w.set_component(id, 30i32).unwrap();
            w.set_component(id, 42u32).unwrap();
        }

        w.run_system(|mut cmd: Commands| {
            cmd.spawn().insert(69u64);
        })
        .unwrap();
    }

    let mut world = World::new(100);
    world.add_stage(SystemStage::new("monka").with_system(mutation));

    world.tick();
    world.tick();
}

#[cfg(debug_assertions)]
#[test]
#[should_panic]
fn world_access_is_unique_test() {
    fn bad_sys(_access: WorldAccess, _cmd: Commands) {}

    let mut world = World::new(0);
    world.run_system(bad_sys).unwrap();
}

#[test]
#[should_panic]
fn stacked_world_access_should_panic_test() {
    fn sys(mut access: WorldAccess) {
        let w = access.world_mut();
        // this should panic
        w.run_system(sys).unwrap();
    }

    let mut world = World::new(0);
    world.run_system(sys).unwrap();
}

#[test]
fn optional_resource_test() {
    fn sys(res: Option<Res<i32>>) {
        assert!(res.is_none());
    }

    fn mut_sys(res: Option<ResMut<i32>>) {
        assert!(res.is_none());
    }

    let mut world = World::new(0);
    world.run_system(sys).unwrap();
    world.run_system(mut_sys).unwrap();
}

#[test]
fn can_fetch_entity_id_in_cmd_test() {
    #[derive(Clone, Copy, Debug)]
    struct Foo(EntityId);

    fn sys(mut cmd: Commands) {
        let cmd = cmd.spawn();
        let id = cmd.id().unwrap();
        cmd.insert(Foo(id));
    }

    fn assert_sys(q: Query<(EntityId, &Foo)>) {
        for (id, foo) in q.iter() {
            assert_eq!(id, foo.0);
        }
    }

    let mut world = World::new(1);
    world.run_system(sys).unwrap();
    world.run_system(assert_sys).unwrap();
}

#[test]
fn can_reserve_and_insert_in_same_system_test() {
    fn sys(mut cmd: Commands) {
        cmd.reserve_entities(128);
        for i in 0..128 {
            cmd.spawn().insert(i);
        }
    }

    let mut world = World::new(0);
    world.run_system(sys).unwrap();
}

#[test]
fn can_merge_entities_test() {
    let mut world = World::new(16);

    let a = world.insert_entity();
    let b = world.insert_entity();

    world.set_component(a, 1u64).unwrap();
    world.set_component(b, 2u32).unwrap();
    world.set_component(b, 2u64).unwrap();

    world.merge_entities(a, b).unwrap();

    assert!(!world.is_id_valid(a));

    let c = world.get_component::<u64>(b).unwrap();
    assert_eq!(c, &1);
    let c = world.get_component::<u32>(b).unwrap();
    assert_eq!(c, &2);
}

#[test]
fn can_merge_entities_test_2() {
    // same as above but swapped
    let mut world = World::new(16);

    let a = world.insert_entity();
    let b = world.insert_entity();

    world.set_component(a, 1u64).unwrap();
    world.set_component(b, 2u32).unwrap();
    world.set_component(b, 2u64).unwrap();

    world.merge_entities(b, a).unwrap();

    assert!(!world.is_id_valid(b));

    let c = world.get_component::<u64>(a).unwrap();
    assert_eq!(c, &2);
    let c = world.get_component::<u32>(a).unwrap();
    assert_eq!(c, &2);
}

#[test]
fn can_merge_entities_test_3() {
    // the two entities are disjoint
    //
    let mut world = World::new(16);

    let a = world.insert_entity();
    let b = world.insert_entity();
    // control
    let c = world.insert_entity();

    world.set_component(a, 1u64).unwrap();
    world.set_component(a, 1u32).unwrap();
    world.set_component(b, 2i32).unwrap();
    world.set_component(b, 2i64).unwrap();

    world.set_component(c, 3u64).unwrap();
    world.set_component(c, 3u32).unwrap();
    world.set_component(c, 3i64).unwrap();
    world.set_component(c, 3i32).unwrap();

    world.merge_entities(b, a).unwrap();

    assert!(!world.is_id_valid(b), "Entity b should have been deleted");

    // test if c entity is intact
    let comp = world.get_component::<u64>(c).unwrap();
    assert_eq!(comp, &3);
    let comp = world.get_component::<u32>(c).unwrap();
    assert_eq!(comp, &3);
    let comp = world.get_component::<i64>(c).unwrap();
    assert_eq!(comp, &3);
    let comp = world.get_component::<i32>(c).unwrap();
    assert_eq!(comp, &3);

    let c = world.get_component::<u64>(a).unwrap();
    assert_eq!(c, &1);
    let c = world.get_component::<u32>(a).unwrap();
    assert_eq!(c, &1);
    let c = world.get_component::<i64>(a).unwrap();
    assert_eq!(c, &2);
    let c = world.get_component::<i32>(a).unwrap();
    assert_eq!(c, &2);
}

#[test]
fn unsafe_test() {
    let mut world = World::new(128);

    for _ in 0..64 {
        let a = world.insert_entity();
        world.set_component(a, 1u64).unwrap();
        world.set_component(a, 42u32).unwrap();
    }

    // note that q is not mutable
    fn unsafe_iter_sys(q: Query<&u32>) {
        unsafe {
            for p in q.iter_unsafe() {
                assert_eq!(*p, 42);
                *p = 69;
            }
        }
    }

    world.run_system(unsafe_iter_sys).unwrap();
}

#[cfg(feature = "parallel")]
#[test]
fn unsafe_partition_test() {
    use prelude::JobPool;

    use crate::prelude::With;

    #[derive(Debug, Clone)]
    struct Children(Vec<EntityId>);

    let mut world = World::new(128);

    {
        let parent = world.insert_entity();
        world.set_component(parent, 69u32).unwrap();
        world
            .set_component(parent, Children(Vec::default()))
            .unwrap();
        for _ in 0..32 {
            let a = world.insert_entity();
            world
                .get_component_mut::<Children>(parent)
                .unwrap()
                .0
                .push(a);
            world.set_component(a, 1u32).unwrap();
        }
    }

    unsafe fn add(i: &u32, id: EntityId, q: &Query<&u32>) {
        unsafe {
            *q.fetch_unsafe(id).unwrap() += *i;
        }
    }

    /// This system updates children values based on parent value, in parallel for all parents
    ///
    /// Cecs has no way of knowing that no aliasing happens, parents' values are not changed and
    /// all children must be unique. We can, carefully, use unsafe accessors to achieve our goal
    fn unsafe_iter_sys(q: Query<&u32>, parents: Query<(&u32, &Children)>, pool: Res<JobPool>) {
        pool.scope(|s| {
            for (i, children) in parents.iter() {
                s.spawn(|_| {
                    for child in (*children).0.iter().copied() {
                        unsafe {
                            add(i, child, &q);
                        }
                    }
                });
            }
        });
    }

    fn asserts(q0: Query<&u32, WithOut<Children>>, q1: Query<&u32, With<Children>>) {
        for i in q0.iter().copied() {
            assert_eq!(i, 70);
        }
        for i in q1.iter().copied() {
            assert_eq!(i, 69);
        }
    }

    world.run_system(unsafe_iter_sys).unwrap();
    world.run_system(asserts).unwrap();
}

#[test]
#[cfg(feature = "parallel")]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn test_par_foreach() {
    let mut world = World::new(128);

    {
        for _ in 0..128 {
            let a = world.insert_entity();
            world.set_component(a, 0u32).unwrap();
        }
    }

    fn update_sys(mut q: Query<&mut u32>) {
        q.par_for_each_mut(|i| {
            *i += 1;
        });
    }

    fn asserts(q0: Query<&u32>) {
        for i in q0.iter().copied() {
            assert_eq!(i, 1);
        }
        assert_eq!(q0.count(), 128);
    }

    world.run_system(update_sys).unwrap();
    world.run_system(asserts).unwrap();
}

#[test]
#[cfg(feature = "parallel")]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn par_ordering_test() {
    use crate::systems::IntoSystem;
    use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    };

    let mut world = World::new(128);

    world.insert_resource(Arc::new(AtomicU8::new(0)));

    fn sys0(i: Res<Arc<AtomicU8>>) {
        let i = i.fetch_add(1, Ordering::Relaxed);
        assert_eq!(i, 0);
    }

    fn sys1(i: Res<Arc<AtomicU8>>) {
        let i = i.fetch_add(1, Ordering::Relaxed);
        assert_eq!(i, 1);
    }

    world
        .run_stage(
            SystemStage::new("")
                .with_system(sys1.after(sys0))
                .with_system(move || {})
                .with_system(move || {})
                .with_system(move || {})
                .with_system(move || {})
                .with_system(move || {})
                .with_system(sys0),
        )
        .unwrap();

    let i = world.get_resource::<Arc<AtomicU8>>().unwrap();

    assert_eq!(i.load(Ordering::Relaxed), 2);
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn serial_ordering_test() {
    use crate::systems::IntoSystem;
    use std::sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    };

    let mut world = World::new(128);

    world.insert_resource(Arc::new(AtomicU8::new(0)));

    fn sys0(i: Res<Arc<AtomicU8>>) {
        let i = i.fetch_add(1, Ordering::Relaxed);
        assert_eq!(i, 0);
    }

    fn sys1(i: Res<Arc<AtomicU8>>) {
        let i = i.fetch_add(1, Ordering::Relaxed);
        assert_eq!(i, 1);
    }

    world
        .run_stage(
            SystemStage::new("")
                .with_system(sys1.after(sys0))
                .with_system(sys0),
        )
        .unwrap();

    let i = world.get_resource::<Arc<AtomicU8>>().unwrap();

    assert_eq!(i.load(Ordering::Relaxed), 2);
}

#[test]
fn with_and_without_test() {
    use crate::prelude::*;

    // the two entities are disjoint
    //
    let mut world = World::new(16);

    let a = world.insert_entity();
    let b = world.insert_entity();

    world.set_component(a, 1u64).unwrap();
    world.set_component(a, 1u32).unwrap();
    world.set_component(b, 2i32).unwrap();
    world.set_component(b, 2i64).unwrap();

    let q = Query::<(EntityId, &u64), (With<u32>, WithOut<String>)>::new(&world);

    let count = q
        .iter()
        .inspect(|(id, n)| {
            assert_eq!(id, &a);
            assert_eq!(n, &&1);
        })
        .count();

    assert_eq!(count, 1);
}

#[test]
fn archetype_id_query() {
    let mut world = World::new(16);

    let a = world.insert_entity();
    let b = world.insert_entity();
    let c = world.insert_entity();

    world.set_component(a, 1u64).unwrap();
    world.set_component(a, 1u32).unwrap();

    world.set_component(b, 2u32).unwrap();
    world.set_component(b, 2u64).unwrap();

    world.set_component(c, 2i32).unwrap();

    let q = Query::<ArchetypeHash>::new(&world);

    assert_eq!(q.fetch(a).unwrap(), q.fetch(b).unwrap());
    assert_ne!(q.fetch(a).unwrap(), q.fetch(c).unwrap());
}

#[test]
fn vacuum_must_not_collect_void_ty_test() {
    let mut w = World::new(8);

    w.run_system(|mut cmd: Commands| {
        cmd.spawn().insert_bundle((1u8, 2u32));
    })
    .unwrap();

    w.vacuum();

    let mut found = false;
    for (x, _a) in w.archetypes().iter() {
        if x == &VOID_TY {
            found = true;
        }
    }

    assert!(found);

    // try inserting another
    w.run_system(|mut cmd: Commands| {
        cmd.spawn().insert_bundle((1u8, 2u32));
    })
    .unwrap();
}

#[test]
fn test_fn_once() {
    let mut w = World::new(4);

    let result = "Delicious".to_string();
    let result = w
        .run_system(move || {
            // this system is FnOnce, it consumes the provided string
            result
        })
        .unwrap();

    assert_eq!(result, "Delicious");
}

#[test]
fn single_test() {
    let mut w = World::new(4);

    let id = w.insert_entity();
    w.set_bundle(id, ("Delicious".to_string(),)).unwrap();
    w.run_system(move |mut q: Query<&mut String>| {
        *q.single_mut().unwrap() = "Cookie".to_string();
    })
    .unwrap();

    let result = w
        .run_system(move |q: Query<&String>| q.single().unwrap().clone())
        .unwrap();

    assert_eq!(result, "Cookie");
}

#[test]
fn world_should_not_contain_empty_archetypes() {
    let mut world = World::new(16);

    world
        .run_system(|mut cmd: Commands| {
            cmd.spawn()
                .insert(1u64)
                .insert(1u32)
                .insert(1u16)
                .insert("name");
        })
        .unwrap();

    let mut count = 0;
    for (_key, arch) in world.archetypes().iter().filter(|(k, _)| *k != &VOID_TY) {
        assert!(!arch.is_empty());
        count += 1;
    }

    assert_eq!(count, 1);
}

#[test]
fn nested_tuple_query() {
    let mut world = World::new(4);

    let id = world.insert_entity();

    world.set_bundle(id, (1i32, 2i64, 3u32, 4u64)).unwrap();

    let i = world.run_view_system(|q: Query<(&i32, (&i64, &u32), &u64)>| {
        let mut i = 0;
        for (a, (b, c), d) in q.iter() {
            assert_eq!(a, &1);
            assert_eq!(b, &2);
            assert_eq!(c, &3);
            assert_eq!(d, &4);
            i += 1;
        }
        i
    });

    assert_eq!(i, 1);
}

#[test]
fn tuple_queries() {
    let mut world = World::new(4);

    let id = world.insert_entity();

    world.set_bundle(id, (1i32, 2i64, 3u32, 4u64)).unwrap();

    world.insert_resource(42i32);

    let i = world.run_view_system(|(q, r): (Query<(&i32, (&i64, &u32), &u64)>, Res<i32>)| {
        let mut i = 0;
        for (a, (b, c), d) in q.iter() {
            assert_eq!(a, &1);
            assert_eq!(b, &2);
            assert_eq!(c, &3);
            assert_eq!(d, &4);
            i += 1;
        }
        assert_eq!(*r, 42);
        i
    });

    assert_eq!(i, 1);
}

/// Regression test:
/// remove_component had a bug where if the entity's new archetype was in staging, then it would
/// create a new archetype, resuling in use-after-free bugs
///
/// To reproduce:
/// - have an empty archetype in staging
/// - move entities into the empty archetype via set_component
/// - in the same commands set, remove a component from an entity so it's put in this archetype
#[test]
fn test_remove_doesnt_create_duplicate_archetype() {
    let mut w = World::new(8);

    let e1 = w.insert_entity();
    let e2 = w.insert_entity();

    // create archetype with i32 component
    w.set_component(e1, 1i32).unwrap();
    w.set_bundle(e2, (1i32, 2i64)).unwrap();

    // i32 archetype is empty after this
    w.run_system(|mut cmd: Commands| {
        cmd.entity(e1).remove::<i32>();
    })
    .unwrap();

    w.archetypes_staging
        .iter()
        .find(|(_, v)| v.contains_column::<i32>() && v.components.len() == 2);

    // move e1 into this archetype by adding component
    // move e2 into this archetype by removing component
    // order matters!
    w.run_system(|mut cmd: Commands| {
        cmd.entity(e1).insert(2i32);
        // this would trigger an assertion before the fix
        cmd.entity(e2).remove::<i64>();
    })
    .unwrap();

    let i: &i32 = w.get_component(e1).unwrap();
    assert_eq!(i, &2);

    let i: &i32 = w.get_component(e2).unwrap();
    assert_eq!(i, &1);
}

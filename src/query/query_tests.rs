use crate::query::filters::Or;

use super::{filters::With, *};

#[test]
fn iter_query_test() {
    let mut archetype = EntityTable::empty()
        .extend_with_column::<String>()
        .extend_with_column::<u32>();

    {
        let id = EntityId::new(0, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog".to_string());
        archetype.set_component(index, 42u32);

        let id = EntityId::new(32, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog32".to_string());
        archetype.set_component(index, 69u32);
    }

    for (comp, exp) in <&String>::iter(&archetype).zip(["pog", "pog32"].iter()) {
        assert_eq!(*comp, *exp);
    }
}

#[test]
fn joined_iter_test() {
    let mut archetype = EntityTable::empty()
        .extend_with_column::<String>()
        .extend_with_column::<u32>();

    {
        let id = EntityId::new(0, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog".to_string());
        archetype.set_component(index, 42u32);

        let id = EntityId::new(32, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog32".to_string());
        archetype.set_component(index, 69u32);
    }

    for ((s, i), (exps, expi)) in
        <(&String, &u32)>::iter(&archetype).zip([("pog", 42), ("pog32", 69)].iter())
    {
        assert_eq!(&*s, exps);
        assert_eq!(&*i, expi);
    }
}

#[test]
fn non_existent_component_iter_is_empty_test() {
    let mut archetype = EntityTable::empty()
        .extend_with_column::<String>()
        .extend_with_column::<u32>();

    {
        let id = EntityId::new(0, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog".to_string());
        archetype.set_component(index, 42u32);

        let id = EntityId::new(32, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog32".to_string());
        archetype.set_component(index, 69u32);
    }

    for _ in <(&String, &u64)>::iter(&archetype) {
        panic!();
    }
}

#[test]
fn mutable_iterator_test() {
    let mut archetype = EntityTable::empty()
        .extend_with_column::<String>()
        .extend_with_column::<u32>();

    {
        let id = EntityId::new(0, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog".to_string());
        archetype.set_component(index, 42u32);

        let id = EntityId::new(32, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog32".to_string());
        archetype.set_component(index, 69u32);
    }

    for s in <&mut String>::iter_mut(&mut archetype) {
        *s = "winnie".to_string();
    }

    for s in <&String>::iter(&archetype) {
        assert_eq!(*s, "winnie");
    }
}

#[test]
fn can_mix_mut_ref_test() {
    let mut archetype = EntityTable::empty()
        .extend_with_column::<String>()
        .extend_with_column::<u32>();

    {
        let id = EntityId::new(0, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog".to_string());
        archetype.set_component(index, 42u32);

        let id = EntityId::new(32, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog32".to_string());
        archetype.set_component(index, 69u32);
    }

    for (_a, b) in <(&String, &mut u32)>::iter_mut(&archetype) {
        *b = 42424242;
    }
    for val in <&u32>::iter(&archetype) {
        assert_eq!(*val, 42424242);
    }

    for (a, _b) in <(&mut String, &u32)>::iter_mut(&archetype) {
        *a = "winnie".to_string();
    }

    for val in <&String>::iter(&archetype) {
        assert_eq!(*val, "winnie");
    }
}

#[test]
fn basic_filter_test() {
    let mut archetype = EntityTable::empty()
        .extend_with_column::<String>()
        .extend_with_column::<u32>();

    {
        let id = EntityId::new(0, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog".to_string());
        archetype.set_component(index, 42u32);

        let id = EntityId::new(32, 1);
        let index = archetype.insert_entity(id);
        archetype.set_component(index, "pog32".to_string());
        archetype.set_component(index, 69u32);
    }

    assert!(!With::<i32>::filter(&archetype));

    assert!(With::<u32>::filter(&archetype));

    assert!(Or::<With<u32>, With<i32>>::filter(&archetype));
    assert!(<(With::<u32>, With::<String>) as Filter>::filter(
        &archetype
    ));
}

#[test]
fn test_subset() {
    #[derive(Clone, Copy)]
    struct A;
    #[derive(Clone, Copy)]
    struct B;
    #[derive(Clone, Copy)]
    struct C;

    let mut world = World::new(4);

    let e = world.insert_entity();
    world.set_bundle(e, (A, B, C)).unwrap();

    let mut q = Query::<(EntityId, &mut A, &mut B, &C)>::new(&world);

    let sub: Query<(&A, &C, EntityId)> = q.subset();

    let mut count = 0;
    for (_a, _c, id) in sub.iter() {
        assert_eq!(id, e);
        count += 1;
    }
    assert_eq!(count, 1);

    let _sub: Query<(&mut A, &C, EntityId)> = q.subset();
}

#[test]
#[should_panic]
#[cfg(debug_assertions)]
fn invalid_subset() {
    #[derive(Clone, Copy)]
    struct A;
    #[derive(Clone, Copy)]
    struct B;
    #[derive(Clone, Copy)]
    struct C;

    let mut world = World::new(4);

    let e = world.insert_entity();
    world.set_bundle(e, (A, B, C)).unwrap();

    let mut q = Query::<(EntityId, &mut A, &mut B, &C)>::new(&world);

    let _sub: Query<(&A, &mut C, EntityId)> = q.subset();
}

/// Test if this compiles
///
/// Ensures that ParForEachDescriptor ergonomics don't suck
#[test]
fn can_specify_batch_ergonomics() {
    #[derive(Clone)]
    struct Pollution(pub i32);
    #[expect(unused)]
    fn decrease_pollution_batched(mut q: Query<&mut Pollution>) {
        q.parallel()
            .with_batch_size(NonZeroUsize::new(256))
            .for_each_mut(|p| {
                p.0 -= 1;
            });
    }

    #[expect(unused)]
    fn decrease_pollution(mut q: Query<&mut Pollution>) {
        q.par_for_each_mut(|p| {
            p.0 -= 1;
        });
    }
}

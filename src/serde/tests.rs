use semver::Version;

use super::*;
use crate::prelude::*;
use std::collections::HashSet;

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Clone)]
struct Foo {
    value: u32,
}

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Clone)]
struct Never;

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Clone, PartialEq, Eq)]
enum Bar {
    Foo,
    Bar,
    Baz,
}

#[test]
fn save_load_json_test() {
    let mut world0 = World::new(10);

    for i in 0u32..10u32 {
        let id = world0.insert_entity();
        world0.set_component(id, 42i32).unwrap();
        world0.set_component(id, i).unwrap();
        world0.set_component(id, Foo { value: i }).unwrap();
        world0.set_component(id, Bar::Baz).unwrap();
    }

    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_component::<Foo>()
        .with_component::<Bar>();
    let mut result = Vec::<u8>::new();
    let mut s = serde_json::Serializer::pretty(&mut result);

    p.save(&mut s, &world0).unwrap();

    let result = String::from_utf8(result).unwrap();

    println!("{result}");

    let world1 = p
        .load(&mut serde_json::Deserializer::from_str(result.as_str()))
        .unwrap();

    type QueryTuple<'a> = (EntityId, &'a i32, &'a Foo);

    for ((id0, i0, f0), (id1, i1, f1)) in Query::<QueryTuple>::new(&world0)
        .iter()
        .zip(Query::<QueryTuple>::new(&world1).iter())
    {
        assert_eq!(id0, id1);
        assert_eq!(i0, i1);
        assert_eq!(f0.value, f1.value);
    }

    assert_eq!(
        Query::<&u32>::new(&world1).count(),
        0,
        "Assumes that non registered types are not (de)serialized"
    );
}

#[test]
fn save_load_bincode_test() {
    let mut world0 = World::new(10);

    for i in 0u32..10u32 {
        let id = world0.insert_entity();
        world0.set_component(id, 42i32).unwrap();
        world0.set_component(id, i).unwrap();
        world0.set_component(id, Foo { value: i }).unwrap();
    }

    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_component::<Never>()
        .with_component::<Foo>();

    let mut result = Vec::<u8>::new();
    let mut s = bincode::Serializer::new(&mut result, bincode::config::DefaultOptions::new());
    p.save(&mut s, &world0).unwrap();

    let world1 = p
        .load(&mut bincode::de::Deserializer::from_slice(
            result.as_slice(),
            bincode::config::DefaultOptions::new(),
        ))
        .unwrap();

    let mut result1 = Vec::<u8>::new();
    let mut s = bincode::Serializer::new(&mut result1, bincode::config::DefaultOptions::new());
    p.save(&mut s, &world1).unwrap();

    assert_eq!(
        result, result1,
        "Re-serializing the deserialized world should yield the same payload"
    );

    type QueryTuple<'a> = (EntityId, &'a i32, &'a Foo);

    for ((id0, i0, f0), (id1, i1, f1)) in Query::<QueryTuple>::new(&world0)
        .iter()
        .zip(Query::<QueryTuple>::new(&world1).iter())
    {
        assert_eq!(id0, id1);
        assert_eq!(i0, i1);
        assert_eq!(f0.value, f1.value);
    }

    assert_eq!(
        Query::<&u32>::new(&world1).count(),
        0,
        "Assumes that non registered types are not (de)serialized"
    );
}

#[test]
fn resource_saveload_json_test() {
    let mut world0 = World::new(4);

    for i in 0u32..4u32 {
        let id = world0.insert_entity();
        world0.set_component(id, 42i32).unwrap();
        world0.set_component(id, i).unwrap();
        world0.set_component(id, Foo { value: i }).unwrap();
    }

    world0.insert_resource(Foo { value: 69 });

    let p = WorldPersister::new()
        .with_component::<Foo>()
        .with_resource::<Foo>();

    let mut pl = Vec::<u8>::new();
    let mut s = serde_json::Serializer::pretty(&mut pl);

    p.save(&mut s, &world0).unwrap();

    let pretty = std::str::from_utf8(&pl).unwrap();
    println!("{}", pretty);

    let world1 = p
        .load(&mut serde_json::Deserializer::from_reader(pl.as_slice()))
        .unwrap();

    type QueryTuple<'a> = (EntityId, &'a Foo);

    let mut count = 0;
    for ((id0, f0), (id1, f1)) in Query::<QueryTuple>::new(&world0)
        .iter()
        .zip(Query::<QueryTuple>::new(&world1).iter())
    {
        assert_eq!(id0, id1);
        assert_eq!(f0.value, f1.value);
        count += 1;
    }
    assert_eq!(count, 4);

    assert_eq!(
        world1.get_resource::<Foo>().expect("foo not found").value,
        69
    );
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn can_serde_multiple_resources_test() {
    // regression test: had a bug where the first resource would not be deserialized

    let mut world0 = World::new(4);
    world0.insert_resource(42i64);
    world0.insert_resource(69u32);

    let p = WorldPersister::new()
        .with_resource::<u32>()
        .with_resource::<i64>();

    let mut result = Vec::<u8>::new();
    let mut s = bincode::Serializer::new(&mut result, bincode::config::DefaultOptions::new());
    p.save(&mut s, &world0).unwrap();

    let world1 = p
        .load(&mut bincode::de::Deserializer::from_slice(
            result.as_slice(),
            bincode::config::DefaultOptions::new(),
        ))
        .unwrap();

    let i = world1.get_resource::<i64>().unwrap();
    assert_eq!(i, &42);
    let u = world1.get_resource::<u32>().unwrap();
    assert_eq!(u, &69);
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn ids_are_stable_test() {
    let mut world0 = World::new(10);

    for i in 0u32..10u32 {
        // produce some gaps
        for _ in 0..4 {
            let _id = world0.insert_entity();
        }
        // bump generation
        for _ in 0..4 {
            let id = world0.insert_entity();
            world0.delete_entity(id).unwrap();
        }
        let id = world0.insert_entity();
        world0.set_component(id, 42i32).unwrap();
        // produce multiple archetypes
        if i % 2 == 0 {
            world0.set_component(id, i).unwrap();
        }
        if i % 3 == 0 {
            world0.set_component(id, 4.2f32).unwrap();
        }
        world0.set_component(id, Foo { value: i }).unwrap();
    }

    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_component::<Foo>();

    let mut result = Vec::<u8>::new();
    let mut s = bincode::Serializer::new(&mut result, bincode::config::DefaultOptions::new());
    p.save(&mut s, &world0).unwrap();

    let mut world1 = p
        .load(&mut bincode::de::Deserializer::from_slice(
            result.as_slice(),
            bincode::config::DefaultOptions::new(),
        ))
        .unwrap();

    type Q<'a> = Query<'a, (EntityId, ArchetypeHash, Has<u32>, Has<f32>), (With<i32>, With<Foo>)>;

    let u32_hash = crate::hash_ty::<u32>();
    let f32_hash = crate::hash_ty::<f32>();

    let q0 = Q::new(&world0);
    let q1 = Q::new(&world1);

    assert_eq!(q0.count(), q1.count());

    // check if the archetypes match for each entity
    for ((id0, mut h0, c, d), (id1, h1, _, _)) in q0.iter().zip(q1.iter()) {
        assert_eq!(id0, id1);
        if c {
            // unserialized components will be lost
            h0.0 ^= u32_hash;
        }
        if d {
            // unserialized components will be lost
            h0.0 ^= f32_hash;
        }
        assert_eq!(h0, h1);
    }
    let mut ids = Query::<EntityId>::new(&world1)
        .iter()
        .collect::<HashSet<_>>();

    for _ in 0..1024 {
        let id = world1.insert_entity();
        assert!(!ids.contains(&id));
        ids.insert(id);
    }

    assert_eq!(
        Query::<&u32>::new(&world1).count(),
        0,
        "Assumes that non registered types are not (de)serialized"
    );
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn version_saved_test() {
    let world0 = World::new(8);
    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_version(Version::parse("1.0.0").unwrap());

    let mut result = Vec::<u8>::new();
    let mut s = serde_json::Serializer::pretty(&mut result);

    p.save(&mut s, &world0).unwrap();

    println!("{}", String::from_utf8(result.clone()).unwrap());

    let version = p
        .load_version(&mut serde_json::Deserializer::from_slice(result.as_slice()))
        .unwrap();

    assert_eq!(version.as_ref(), Some(&Version::parse("1.0.0").unwrap()));

    let t: serde_json::Value = serde_json::from_slice(&result).unwrap();
    let v = t
        .get(VERSION_KEY)
        .expect("missing version entry in the serialized world");
    assert_eq!(v.as_str().unwrap(), "1.0.0");
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn version_mismatch_is_error_test() {
    let world0 = World::new(8);
    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_version(Version::parse("1.0.0").unwrap());

    let mut result = Vec::<u8>::new();
    let mut s = serde_json::Serializer::pretty(&mut result);

    p.save(&mut s, &world0).unwrap();

    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_version(Version::parse("2.0.0").unwrap());

    let err = p
        .load(&mut serde_json::Deserializer::from_slice(&result))
        .map(drop)
        .expect_err("Deserialization of incompatible versions should fail");

    assert_eq!(
        &err.to_string()[0..93],
        "Version mismatch. WorldPersister expected version `2.0.0` but the payload has version `1.0.0`",
    );
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn compatible_versions_are_deserialized_test() {
    let world0 = World::new(8);
    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_version(Version::parse("1.0.0").unwrap());

    let mut result = Vec::<u8>::new();
    let mut s = serde_json::Serializer::pretty(&mut result);

    p.save(&mut s, &world0).unwrap();

    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_component::<u32>()
        .with_version(Version::parse("1.1.0").unwrap());

    p.load(&mut serde_json::Deserializer::from_slice(&result))
        .map(drop)
        .unwrap();
}

fn test_version_fail(src: &str, dst: &str) {
    let world0 = World::new(8);
    let p = WorldPersister::new()
        .with_component::<i32>()
        .with_version(Version::parse(src).unwrap());

    let mut result = Vec::<u8>::new();
    let mut s = serde_json::Serializer::pretty(&mut result);

    p.save(&mut s, &world0).unwrap();

    let p = WorldPersister::new()
        .with_component::<u32>()
        .with_version(Version::parse(dst).unwrap());

    p.load(&mut serde_json::Deserializer::from_slice(&result))
        .map(drop)
        .unwrap_err();
}

#[test]
#[cfg_attr(feature = "tracing", tracing_test::traced_test)]
fn incompatible_versions_are_not_deserialized_test() {
    test_version_fail("1.0.0", "2.0.0");
    test_version_fail("1.1.0", "1.0.0");
    test_version_fail("1.0.1", "1.0.0");
}

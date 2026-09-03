//! Provides utilities to save and load Worlds via serde.
//!
use erased_serde::Serializer as _;
use rustc_hash::FxHashMap;
use serde::ser::Error as _;
use serde::{
    Serialize,
    de::{DeserializeOwned, Error, IgnoredAny, Visitor},
    ser::SerializeSeq,
};
use std::marker::PhantomData;

use crate::{Component, World, entity_id::EntityId, prelude::Query};

const VERSION_KEY: &str = "__version__";

pub use erased_serde;
pub use semver;
pub type Version = semver::Version;

trait ErasedColumnSerde {
    fn id(&self) -> String;

    fn save<'a>(&self, world: &'a World) -> Box<dyn erased_serde::Serialize + 'a>;
    fn load(
        &self,
        world: &mut World,
        d: &mut dyn erased_serde::Deserializer,
    ) -> Result<(), erased_serde::Error>;
}

struct ComponentEntry<U> {
    _m: PhantomData<U>,
}

struct ColumnSer<'a, U> {
    _m: PhantomData<U>,
    w: &'a World,
}

impl<'a, U: Component + serde::Serialize> serde::Serialize for ColumnSer<'a, U> {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let q = Query::<(EntityId, &U)>::new(self.w);
        let mut s = s.serialize_seq(Some(q.count()))?;
        for t in q.iter() {
            s.serialize_element(&t)?;
        }
        s.end()
    }
}

impl<U: Component + serde::Serialize + DeserializeOwned> ErasedColumnSerde for ComponentEntry<U> {
    fn id(&self) -> String {
        format!(
            "{:?}-{}",
            SerTy::Component,
            std::any::type_name::<U>().to_owned()
        )
    }

    fn save<'a>(&self, world: &'a World) -> Box<dyn erased_serde::Serialize + 'a> {
        let col = ColumnSer::<U> {
            _m: PhantomData,
            w: world,
        };
        Box::new(col)
    }

    fn load(
        &self,
        world: &mut World,
        d: &mut dyn erased_serde::Deserializer,
    ) -> Result<(), erased_serde::Error> {
        // TODO: would be nice if we could circumvent this intermediate vec similar to ColumnSeed
        let column: Vec<(EntityId, U)> = erased_serde::deserialize(d)?;

        for (id, value) in column {
            if !world.is_id_valid(id) {
                #[cfg(feature = "tracing")]
                tracing::trace!("Inserting id {id}");
                world.insert_id(id).unwrap();
            }
            world.set_component(id, value).unwrap();
        }

        Ok(())
    }
}

struct ResourceEntry<U> {
    _m: PhantomData<U>,
}

impl<U: Component + serde::Serialize + DeserializeOwned> ErasedColumnSerde for ResourceEntry<U> {
    fn id(&self) -> String {
        format!(
            "{:?}-{}",
            SerTy::Resource,
            std::any::type_name::<U>().to_owned()
        )
    }

    fn save<'a>(&self, world: &'a World) -> Box<dyn erased_serde::Serialize + 'a> {
        Box::new(world.get_resource::<U>())
    }

    fn load(
        &self,
        world: &mut World,
        d: &mut dyn erased_serde::Deserializer,
    ) -> Result<(), erased_serde::Error> {
        let value: Option<U> = erased_serde::deserialize(d)?;
        if let Some(value) = value {
            world.insert_resource(value);
        }
        Ok(())
    }
}

pub struct WorldPersister {
    registered_rows: FxHashMap<String, Box<dyn ErasedColumnSerde>>,
    version: Option<Version>,
}

impl Default for WorldPersister {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde_derive::Serialize, serde_derive::Deserialize,
)]
enum SerTy {
    Component,
    Resource,
}

impl WorldPersister {
    pub fn new() -> Self {
        WorldPersister {
            registered_rows: Default::default(),
            version: None,
        }
    }
    /// Add a version field to the serialized data.
    ///
    /// If the WorldPersister's major version is different than the serialized data version, then
    /// deserialization is rejected.
    /// Minor and patch version differences are accepted
    pub fn with_version(mut self, version: impl Into<Version>) -> Self {
        self.version = Some(version.into());
        self
    }

    fn register(&mut self, t: impl ErasedColumnSerde + 'static) {
        self.registered_rows.insert(t.id(), Box::new(t));
    }

    /// Component will be serialized
    ///
    /// Entities with no component in WorldPersister will not be serialized
    ///
    /// You can GC unserialized entities after deserialization by deleting entities with
    /// [[World::gc_empty_entities]]
    pub fn with_component<U: Component + Serialize + DeserializeOwned>(mut self) -> Self {
        self.register(ComponentEntry::<U> { _m: PhantomData });
        self
    }

    pub fn save<S: serde::Serializer>(&self, s: S, world: &World) -> Result<(), S::Error> {
        let mut s = <dyn erased_serde::Serializer>::erase(s);

        // outermost map, type -> list[id, values]
        // bincode requires a length be specified
        let mut len = self.registered_rows.len();
        if self.version.is_some() {
            len += 1;
        }

        let s = s
            .erased_serialize_map(Some(len))
            .map_err(S::Error::custom)?;

        if let Some(v) = self.version.as_ref() {
            s.erased_serialize_entry(&VERSION_KEY, v)
                .map_err(S::Error::custom)?;
        }

        for (k, v) in self.registered_rows.iter() {
            s.erased_serialize_entry(k, &v.save(world))
                .map_err(S::Error::custom)?;
        }

        s.erased_end();
        Ok(())
    }

    pub fn with_resource<U: Component + Serialize + DeserializeOwned>(mut self) -> WorldPersister {
        self.register(ResourceEntry::<U> { _m: PhantomData });
        self
    }

    pub fn load<'a, D: serde::Deserializer<'a>>(&self, d: D) -> Result<World, D::Error> {
        let world = World::new(0);
        let visitor = WorldVisitor {
            persist: self,
            world,
        };
        let world = d.deserialize_map(visitor)?;

        Ok(world)
    }

    pub fn load_version<'a, D: serde::Deserializer<'a>>(
        &self,
        d: D,
    ) -> Result<Option<Version>, D::Error> {
        let visitor = VersionVisitor;
        let version = d.deserialize_map(visitor)?;
        Ok(version)
    }
}

struct WorldVisitor<'a> {
    persist: &'a WorldPersister,
    world: World,
}

/// used to bypass the intermediate deserialization of columns into vectors
struct ColumnSeed<'a> {
    entry: &'a dyn ErasedColumnSerde,
    world: &'a mut World,
}

impl<'de> serde::de::DeserializeSeed<'de> for ColumnSeed<'_> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        let mut d = <dyn erased_serde::Deserializer>::erase(d);
        self.entry
            .load(self.world, &mut d)
            .map_err(D::Error::custom)
    }
}

impl<'a, 'de: 'a> Visitor<'de> for WorldVisitor<'a> {
    type Value = World;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(&format!(
            "Serialized World compatible with version {:?}",
            self.persist.version
        ))
    }

    fn visit_map<A>(mut self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
            if key == VERSION_KEY {
                if let Some(expected) = self.persist.version.as_ref() {
                    let req =
                        semver::VersionReq::parse(&format!("<= {}, ^{}", expected, expected.major))
                            .unwrap();
                    let version: Version = map.next_value()?;
                    if !req.matches(&version) {
                        return Err(A::Error::custom(format!(
                            "Version mismatch. WorldPersister expected version `{expected}` but the payload has version `{version}`"
                        )));
                    }
                }
            } else {
                let Some(row) = self.persist.registered_rows.get(key.as_ref()) else {
                    // missing row is not an error
                    map.next_value::<IgnoredAny>()?;
                    continue;
                };

                map.next_value_seed(ColumnSeed {
                    entry: row.as_ref(),
                    world: &mut self.world,
                })?;
            }
        }

        Ok(self.world)
    }
}

struct VersionVisitor;

impl<'de> Visitor<'de> for VersionVisitor {
    type Value = Option<Version>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Serialized World")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        // json deserializer complains if I don't consume the entire dict
        let mut result = None;
        while let Some(key) = map.next_key::<std::borrow::Cow<'de, str>>()? {
            if key == VERSION_KEY {
                result = map.next_value::<Version>().map(Some)?;
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
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

        type Q<'a> =
            Query<'a, (EntityId, ArchetypeHash, Has<u32>, Has<f32>), (With<i32>, With<Foo>)>;

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
}

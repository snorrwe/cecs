//! Provides utilities to save and load Worlds via serde.
//!
#[cfg(test)]
mod tests;

use rustc_hash::FxHashMap;
use serde::ser::{Error as _, SerializeMap as _};
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

    pub fn save<S: serde::Serializer>(&self, ser: S, world: &World) -> Result<S::Ok, S::Error> {
        // outermost map, type -> list[id, values]
        // bincode requires a length be specified
        let mut len = self.registered_rows.len();
        if self.version.is_some() {
            len += 1;
        }

        let mut s = ser.serialize_map(Some(len)).map_err(S::Error::custom)?;

        if let Some(v) = self.version.as_ref() {
            s.serialize_entry(&VERSION_KEY, v)
                .map_err(S::Error::custom)?;
        }

        for (k, v) in self.registered_rows.iter() {
            s.serialize_entry(k, &v.save(world))
                .map_err(S::Error::custom)?;
        }

        s.end()
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

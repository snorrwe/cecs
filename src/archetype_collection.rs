//! Archetype collection
//! Flat storage with ordered archetypes
//! Iteration order must be stable by the key
//!

use std::pin::Pin;

use crate::{TypeHash, table::EntityTable};

pub type Inner = Pin<Box<EntityTable>>;

#[derive(Debug, Default)]
#[cfg_attr(feature = "clone", derive(Clone))]
pub struct ArchetypeCollection {
    /// Ordered set of archetypes
    archetypes: Vec<(TypeHash, Inner)>,
}

impl ArchetypeCollection {
    pub fn extend(&mut self, it: impl Iterator<Item = (TypeHash, Inner)>) {
        self.archetypes.extend(it);
        self.archetypes.sort_unstable_by_key(|(k, _)| *k);

        self.assert_invariants();
    }

    pub fn insert(&mut self, k: TypeHash, v: Inner) {
        let i = self
            .archetypes
            .binary_search_by_key(&k, |(k, _)| *k)
            .unwrap_or_else(|i| i);

        self.archetypes.insert(i, (k, v));

        self.assert_invariants();
    }

    fn assert_invariants(&mut self) {
        #[cfg(debug_assertions)]
        {
            let l = self.archetypes.len();
            self.archetypes.dedup_by_key(|(k, _)| *k);
            assert_eq!(l, self.archetypes.len(), "Duplicate archetypes");
        }
    }

    pub fn get_or_insert_with(
        &mut self,
        k: TypeHash,
        f: impl FnOnce() -> Inner,
    ) -> &mut EntityTable {
        let i = match self.archetypes.binary_search_by_key(&k, |(k, _)| *k) {
            Ok(i) => i,
            Err(i) => {
                self.archetypes.insert(i, (k, f()));
                i
            }
        };
        &mut self.archetypes[i].1
    }

    pub fn contains_key(&self, k: &TypeHash) -> bool {
        self.archetypes.binary_search_by_key(k, |(k, _)| *k).is_ok()
    }

    #[expect(unused)]
    pub fn get<'a>(&'a self, k: &TypeHash) -> Option<&'a EntityTable> {
        self.archetypes
            .binary_search_by_key(k, |(k, _)| *k)
            .map(|i| Pin::as_ref(&self.archetypes[i].1).get_ref())
            .ok()
    }

    pub fn get_mut<'a>(&'a mut self, k: &TypeHash) -> Option<&'a mut EntityTable> {
        self.archetypes
            .binary_search_by_key(k, |(k, _)| *k)
            .map(|i| Pin::as_mut(&mut self.archetypes[i].1).get_mut())
            .ok()
    }

    pub fn remove(&mut self, k: &TypeHash) -> Option<Inner> {
        self.archetypes
            .binary_search_by_key(k, |(k, _)| *k)
            .ok()
            .map(|i| self.archetypes.remove(i).1)
    }

    pub fn iter(&self) -> impl Iterator<Item = &(TypeHash, Inner)> {
        self.archetypes.iter()
    }

    #[allow(unused)]
    pub fn keys(&self) -> impl Iterator<Item = &TypeHash> {
        self.archetypes.iter().map(|(k, _)| k)
    }

    #[allow(unused)]
    pub fn values(&self) -> impl Iterator<Item = &Inner> {
        self.archetypes.iter().map(|(_, v)| v)
    }

    pub fn as_slice(&self) -> &[(TypeHash, Pin<Box<EntityTable>>)] {
        self.archetypes.as_slice()
    }

    #[cfg(feature = "clone")]
    pub fn clone_with_alloc(
        &self,
        allocator: crate::component_allocator::ComponentAllocator,
    ) -> Self {
        Self {
            archetypes: self
                .archetypes
                .iter()
                .map(|(k, v)| {
                    let v: &EntityTable = v.as_ref().get_ref();
                    let c = v.clone_with_alloc(allocator.clone());
                    (*k, Box::pin(c))
                })
                .collect(),
        }
    }
}

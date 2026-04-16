use crate::{World, prelude::EntityId, query::WorldQuery};
use std::marker::PhantomData;

/// Iterate over EntityIds deleted in the last tick
/// Deleted entities are refreshed between ticks
///
/// ```
/// use cecs::prelude::*;
///
/// let mut world = World::new(4);
/// let id = world.insert_entity();
/// world.delete_entity(id).unwrap();
/// world.tick();
/// world.run_view_system(move |q: DeletedEntities| {
///     for deleted_id in q.iter() {
///       assert_eq!(deleted_id, id);
///       return;
///     }
///     unreachable!("There should be one deleted entity id");
/// });
/// ```
#[derive(Debug)]
pub struct DeletedEntities<'a> {
    world: std::ptr::NonNull<World>,
    _l: PhantomData<&'a ()>,
}

impl<'a> DeletedEntities<'a> {
    pub fn iter(&self) -> impl Iterator<Item = EntityId> {
        unsafe { self.world.as_ref().iter_deleted() }
    }

    pub fn count(&self) -> usize {
        unsafe { self.world.as_ref().deleted.len() }
    }

    pub fn is_empty(&self) -> bool {
        unsafe { self.world.as_ref().deleted.is_empty() }
    }
}

unsafe impl<'a> WorldQuery<'a> for DeletedEntities<'a> {
    fn new(db: &'a World, _system_idx: usize) -> Self {
        Self {
            world: std::ptr::NonNull::from(db),
            _l: PhantomData,
        }
    }

    fn read_only() -> bool {
        true
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_deleted_entities_are_cleared() {
        let mut world = World::new(4);
        let id = world.insert_entity();
        world.delete_entity(id).unwrap();
        world.tick();
        world.run_view_system(move |q: DeletedEntities| {
            for deleted_id in q.iter() {
                assert_eq!(deleted_id, id);
                return;
            }
            unreachable!("One id should have been deleted");
        });
        world.tick();
        world.run_view_system(move |q: DeletedEntities| {
            assert!(q.is_empty());
        });
    }

    #[test]
    fn test_update_deleted() {
        let mut world = World::new(4);
        let id = world.insert_entity();
        world.delete_entity(id).unwrap();
        world.run_view_system(move |q: DeletedEntities| {
            assert!(q.is_empty());
        });
        world.update_deleted();
        world.run_view_system(move |q: DeletedEntities| {
            for deleted_id in q.iter() {
                assert_eq!(deleted_id, id);
                return;
            }
            unreachable!("One id should have been deleted");
        });
        world.update_deleted();
        world.run_view_system(move |q: DeletedEntities| {
            assert!(q.is_empty());
        });
    }
}

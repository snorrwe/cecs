use std::{
    any::TypeId,
    collections::HashSet,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use super::WorldQuery;

pub struct Res<'a, T> {
    inner: &'a T,
    _m: PhantomData<T>,
}

unsafe impl<'a, T: 'static> WorldQuery<'a> for Res<'a, T> {
    fn new(db: &'a crate::World, _system_idx: usize) -> Self {
        Self::new(db)
    }

    fn resources_const(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }

    fn read_only() -> bool {
        true
    }
}

impl<'a, T: 'static> Res<'a, T> {
    pub fn new(world: &'a crate::World) -> Self {
        let inner = match world.resources.fetch() {
            Some(inner) => inner,
            None => {
                panic!(
                    "Res query on uninitialized type: {}",
                    std::any::type_name::<T>()
                );
            }
        };
        Self {
            inner,
            _m: PhantomData,
        }
    }

    pub fn cloned(&self) -> T
    where
        T: Clone,
    {
        self.inner.clone()
    }

    pub fn copied(&self) -> T
    where
        T: Copy,
    {
        *self.inner
    }
}

impl<'a, T: 'static> Deref for Res<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, T: 'static> AsRef<T> for Res<'a, T> {
    fn as_ref(&self) -> &T {
        self.inner
    }
}

pub struct ResMut<'a, T> {
    inner: &'a mut T,
    _m: PhantomData<fn() -> &'a mut T>,
}

impl<'a, T: 'static> ResMut<'a, T> {
    pub fn new(world: &'a crate::World) -> Self {
        let inner = match unsafe { world.resources.fetch_mut() } {
            Some(inner) => inner,
            None => {
                panic!(
                    "ResMut query on uninitialized type: {}",
                    std::any::type_name::<T>()
                );
            }
        };
        Self {
            inner,
            _m: PhantomData,
        }
    }

    pub fn cloned(&self) -> T
    where
        T: Clone,
    {
        self.inner.clone()
    }

    pub fn copied(&self) -> T
    where
        T: Copy,
    {
        *self.inner
    }
}

impl<'a, T: 'static> Deref for ResMut<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, T: 'static> DerefMut for ResMut<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
    }
}

impl<'a, T: 'static> AsRef<T> for ResMut<'a, T> {
    fn as_ref(&self) -> &T {
        self.inner
    }
}

impl<'a, T: 'static> AsMut<T> for ResMut<'a, T> {
    fn as_mut(&mut self) -> &mut T {
        self.inner
    }
}

unsafe impl<'a, T: 'static> WorldQuery<'a> for ResMut<'a, T> {
    fn new(db: &'a crate::World, _system_idx: usize) -> Self {
        Self::new(db)
    }

    fn resources_mut(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }

    fn resources_const(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }
}

unsafe impl<'a, T: 'static> WorldQuery<'a> for Option<Res<'a, T>> {
    fn new(world: &'a crate::World, _system_idx: usize) -> Self {
        let inner = world.resources.fetch();
        inner.map(|inner| Res {
            inner,
            _m: PhantomData,
        })
    }

    fn resources_const(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }

    fn read_only() -> bool {
        true
    }
}

unsafe impl<'a, T: 'static> WorldQuery<'a> for Option<ResMut<'a, T>> {
    fn new(world: &'a crate::World, _system_idx: usize) -> Self {
        let inner = unsafe { world.resources.fetch_mut() };
        inner.map(|inner| ResMut {
            inner,
            _m: PhantomData,
        })
    }

    fn resources_mut(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }

    fn resources_const(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }
}

#[cfg(test)]
mod tests {
    use crate::World;

    use super::*;

    #[test]
    fn test_tuple_resource_query() {
        let mut world = World::new(4);

        world.insert_resource(1i32);
        world.insert_resource(2u32);

        world.run_view_system(|(i, u): (Res<i32>, Res<u32>)| {
            assert_eq!(*i, 1);
            assert_eq!(*u, 2);
        });
    }
}

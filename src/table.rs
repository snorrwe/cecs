use crate::{
    Component, RowIndex, TypeHash, component_allocator::ComponentAllocator, entity_id::EntityId,
    hash_ty, hash_type_id,
};
use rustc_hash::FxHashMap;
use std::{any::TypeId, cell::UnsafeCell, ptr::NonNull};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArchetypeHash(pub TypeHash);

// TODO: component allocator

/// A table for entities with the same shape.
/// Each column of the table stores a specific component, and each row is an entity.
///
/// Sometimes called an 'archetype'
///
/// Components are stored in a column-major format.
pub struct EntityTable {
    pub(crate) ty: TypeHash,
    pub(crate) rows: u32,
    pub(crate) entities: Vec<EntityId>,
    pub(crate) components: FxHashMap<TypeId, UnsafeCell<Column>>,
    pub(crate) allocator: ComponentAllocator,
}

unsafe impl Send for EntityTable {}
unsafe impl Sync for EntityTable {}

#[cfg(feature = "clone")]
impl Clone for EntityTable {
    fn clone(&self) -> Self {
        Self {
            ty: self.ty,
            rows: self.rows,
            entities: self.entities.clone(),
            components: self
                .components
                .iter()
                .map(|(ty, col)| unsafe { (*ty, UnsafeCell::new((*col.get()).clone())) })
                .collect(),
            allocator: self.allocator.clone(),
        }
    }
}

impl std::fmt::Debug for EntityTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityTable")
            .field("rows", &self.rows)
            .field(
                "entities",
                &self
                    .entities
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>(),
            )
            .field(
                "components",
                &self
                    .components
                    .values()
                    .map(|c| unsafe { &*c.get() }.ty_name)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl EntityTable {
    #[cfg(feature = "clone")]
    pub fn clone_with_alloc(&self, allocator: ComponentAllocator) -> Self {
        Self {
            ty: self.ty,
            rows: self.rows,
            entities: self.entities.clone(),
            components: self
                .components
                .iter()
                .map(|(ty, col)| unsafe {
                    (
                        *ty,
                        UnsafeCell::new((*col.get()).clone_with_alloc(allocator.clone())),
                    )
                })
                .collect(),
            allocator,
        }
    }

    pub fn empty(allocator: ComponentAllocator) -> Self {
        let ty = hash_ty::<()>();
        let mut components = FxHashMap::default();
        components.insert(
            TypeId::of::<()>(),
            UnsafeCell::new(Column::new::<()>(0, allocator.clone())),
        );
        Self {
            ty,
            rows: 0,
            entities: Vec::default(),
            components,
            allocator,
        }
    }

    /// Get the archetype storage's ty.
    pub fn ty(&self) -> TypeHash {
        self.ty
    }

    pub fn len(&self) -> usize {
        self.rows as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// return the updated entityid, if any
    #[must_use]
    pub fn remove(&mut self, row_index: RowIndex) -> Option<EntityId> {
        for storage in self.components.values_mut() {
            storage.get_mut().remove(row_index);
        }
        self.entities.swap_remove(row_index as usize);
        self.rows -= 1;
        // if we have remaining entities, and the removed entity was not the last
        (row_index < self.rows).then(|| self.entities[row_index as usize])
    }

    pub fn insert_entity(&mut self, id: EntityId) -> RowIndex {
        let res = self.rows;
        self.entities.push(id);
        self.rows += 1;
        debug_assert!(self.rows as usize == self.entities.len());
        res
    }

    /// return the new index in `dst` and the entity that has been moved to this one's position, if
    /// any
    #[must_use]
    pub fn move_entity(&mut self, dst: &mut Self, index: RowIndex) -> (RowIndex, Option<EntityId>) {
        debug_assert_eq!(self.rows as usize, self.entities.len());
        debug_assert!(self.rows > 0, "rows={}", self.rows);
        debug_assert!(index < self.rows, "index={} rows={}", index, self.rows);
        debug_assert_ne!(self as *mut _, dst as *mut _); // sanity check, the World must not allow
        // this to happen

        let entity_id = self.entities.swap_remove(index as usize);
        let res = dst.insert_entity(entity_id);
        self.rows -= 1;
        let mut moved = None;
        if index < self.rows {
            // the removed row was not the last
            moved = Some(self.entities[index as usize]);
        }
        for (ty, src) in self.components.iter_mut() {
            if let Some(dst) = dst.components.get_mut(ty) {
                (src.get_mut().move_row)(src.get_mut(), dst.get_mut(), index);
            } else {
                // destination does not have self column
                src.get_mut().remove(index);
            }
        }
        (res, moved)
    }

    /// return the moved entity in `self`, if any
    #[must_use]
    pub fn move_entity_into(
        &mut self,
        src_index: RowIndex,
        dst: &mut Self,
        dst_index: RowIndex,
    ) -> Option<EntityId> {
        self.entities.swap_remove(src_index as usize);
        self.rows -= 1;
        let mut moved = None;
        if src_index < self.rows {
            // the removed row was not the last
            moved = Some(self.entities[src_index as usize]);
        }
        for (ty, col) in self.components.iter_mut() {
            if let Some(dst) = dst.components.get_mut(ty) {
                (col.get_mut().move_row_into)(col.get_mut(), src_index, dst.get_mut(), dst_index);
            } else {
                // destination does not have this column
                col.get_mut().remove(src_index);
            }
        }
        moved
    }

    pub fn set_component<T: 'static>(&mut self, row_index: RowIndex, val: T) {
        unsafe {
            let table = self
                .components
                .get_mut(&TypeId::of::<T>())
                .expect("set_component called on bad archetype")
                .get_mut();

            let v = table.as_slice_mut();
            let row_index = row_index as usize;
            assert!(row_index <= v.len());
            if row_index == v.len() {
                table.push(val);
            } else {
                v[row_index] = val;
            }
        }
    }

    pub fn contains_column<T: 'static>(&self) -> bool {
        let hash = TypeId::of::<T>();
        self.contains_column_ty(hash)
    }

    #[inline]
    pub fn contains_column_ty(&self, ty: TypeId) -> bool {
        self.components.contains_key(&ty)
    }

    pub fn extended_hash<T: Component>(&self) -> TypeHash {
        self.extended_hash_ty(hash_ty::<T>())
    }

    pub fn extended_hash_ty(&self, ty: TypeHash) -> TypeHash {
        self.ty ^ ty
    }

    pub fn extend_with_column<T: Component>(mut self) -> Self {
        if !self.contains_column::<T>() {
            let new_ty = self.extended_hash::<T>();
            self.ty = new_ty;
            self.components.insert(
                TypeId::of::<T>(),
                UnsafeCell::new(Column::new::<T>(2, self.allocator.clone())),
            );
        }
        self
    }

    /// Creates a new archetype that holds tables with both `self` and `rhs` columns
    pub fn merged(&self, rhs: &Self) -> Self {
        let mut result = self.clone_empty();
        for (col, table) in rhs.components.iter() {
            if !self.contains_column_ty(*col) {
                let table = unsafe { &*table.get() };
                result.ty = result.extended_hash_ty(hash_type_id(*col));
                result.components.insert(
                    *col,
                    UnsafeCell::new((table.clone_empty)(table.allocator.clone())),
                );
            }
        }
        result
    }

    /// Swap all components of two entities
    pub fn swap_components(&mut self, a: RowIndex, b: RowIndex) {
        for table in self.components.values_mut() {
            let table = table.get_mut();
            (table.swap_rows)(table, a, b);
        }
    }

    pub fn reduce_with_column<T: Component>(mut self) -> Self {
        if self.contains_column::<T>() {
            let new_ty = self.extended_hash::<T>();
            self.ty = new_ty;
            self.components.remove(&TypeId::of::<T>()).unwrap();
        }
        self
    }

    pub fn clone_empty(&self) -> Self {
        self.clone_empty_into(self.allocator.clone())
    }

    pub fn clone_empty_into(&self, allocator: ComponentAllocator) -> Self {
        Self {
            ty: self.ty,
            rows: 0,
            entities: Vec::with_capacity(self.entities.len()),
            components: FxHashMap::from_iter(
                self.components
                    .iter()
                    .map(|(id, col)| {
                        (
                            *id,
                            (unsafe {
                                let col = &*col.get();
                                (col.clone_empty)(allocator.clone())
                            }),
                        )
                    })
                    .map(|(id, col)| (id, UnsafeCell::new(col))),
            ),
            allocator,
        }
    }

    #[inline]
    pub fn get_component<T: 'static>(&self, row: RowIndex) -> Option<&T> {
        self.components.get(&TypeId::of::<T>()).map(|rows| unsafe {
            let col = &*rows.get();
            debug_assert!(row < col.end);
            &*col.data.cast::<T>().add(row as usize)
        })
    }

    /// # SAFETY caller must ensure that no mutable aliasing happens to the row
    #[inline]
    #[allow(clippy::mut_from_ref)]
    pub unsafe fn get_component_mut<T: 'static>(&self, row: RowIndex) -> Option<&mut T> {
        self.components.get(&TypeId::of::<T>()).map(|rows| unsafe {
            let col = &mut *rows.get();
            debug_assert!(row < col.end);
            &mut *col.data.cast::<T>().add(row as usize)
        })
    }

    pub fn components(&self) -> impl Iterator<Item = (TypeId, &Column)> {
        self.components
            .iter()
            .map(|(ty, e)| (*ty, unsafe { &*e.get() }))
    }
}

/// Type erased storage for an Archetype column
pub struct Column {
    allocator: ComponentAllocator,
    // Vec //
    data: *mut u8,
    end: u32,
    capacity: u32,
    // Type Erased Methods //
    pub(crate) finalize: fn(&mut Column),
    pub(crate) swap_remove: fn(RowIndex, &mut Column),
    #[cfg(feature = "clone")]
    pub(crate) clone: fn(&Column) -> Column,
    #[cfg(feature = "clone")]
    pub(crate) clone_with_alloc: fn(&Column, ComponentAllocator) -> Column,
    pub(crate) clone_empty: fn(ComponentAllocator) -> Column,
    /// src, dst
    ///
    /// if component is not in `src` then this is a noop
    /// Caller must ensure that both tables have the same underlying type
    pub(crate) move_row: fn(&mut Column, &mut Column, RowIndex),
    /// src, dst
    /// Move the row from src to the specified slow in dst
    ///
    /// Caller must ensure that dst is initialized and both tables have the same underlying type
    pub(crate) move_row_into: fn(&mut Column, RowIndex, &mut Column, RowIndex),
    /// Swap rows in an entity
    pub(crate) swap_rows: fn(&mut Column, RowIndex, RowIndex),
    pub ty_name: &'static str,
}

impl Drop for Column {
    fn drop(&mut self) {
        (self.finalize)(self);
    }
}

#[cfg(feature = "clone")]
impl Clone for Column {
    fn clone(&self) -> Self {
        (self.clone)(self)
    }
}

impl std::fmt::Debug for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedVec")
            .field("ty", &self.ty_name)
            .finish()
    }
}

#[cfg(feature = "clone")]
fn perform_clone<T: Clone>(dst: &mut Column, src: &Column) {
    dst.end = src.end;
    for i in 0..src.end {
        unsafe {
            let val = (&*src.data.cast::<T>().add(i as usize)).clone();
            std::ptr::write(dst.data.cast::<T>().add(i as usize), val);
        }
    }
}

impl Column {
    pub fn new<T: crate::Component>(capacity: usize, allocator: ComponentAllocator) -> Self {
        let (data, capacity) = allocator.alloc::<T>(capacity);
        Self {
            allocator,
            ty_name: std::any::type_name::<T>(),
            capacity: capacity as u32,
            end: 0,
            data: data.as_ptr().cast(),
            finalize: |erased_table: &mut Column| {
                // drop the inner table
                unsafe {
                    let data: *mut T = erased_table.data.cast();
                    for i in 0..erased_table.end {
                        std::ptr::drop_in_place(data.add(i as usize));
                    }
                    erased_table.allocator.dealloc(
                        NonNull::new_unchecked(erased_table.data.cast::<T>()),
                        erased_table.capacity as usize,
                    );
                }
            },
            swap_remove: |entity_id, erased_table: &mut Column| unsafe {
                erased_table.swap_remove::<T>(entity_id as usize);
            },
            #[cfg(feature = "clone")]
            clone: |table: &Column| {
                let mut res = Column::new::<T>(table.capacity as usize, table.allocator.clone());
                perform_clone::<T>(&mut res, table);
                res
            },
            #[cfg(feature = "clone")]
            clone_with_alloc: |table: &Column, allocator| {
                let mut res = Column::new::<T>(table.capacity as usize, allocator);
                perform_clone::<T>(&mut res, table);
                res
            },
            clone_empty: |allocator: ComponentAllocator| Column::new::<T>(1, allocator),
            move_row: |src, dst, index| unsafe {
                let src = src.swap_remove::<T>(index as usize);
                dst.push::<T>(src);
            },
            move_row_into: |src_t, src, dst_t, dst| unsafe {
                let src = src_t.swap_remove::<T>(src as usize);
                dst_t.as_slice_mut::<T>()[dst as usize] = src;
            },
            swap_rows: |this, src, dst| unsafe {
                this.as_slice_mut::<T>().swap(src as usize, dst as usize);
            },
        }
    }

    /// # SAFETY
    /// Must be called with the same type as `new`
    pub unsafe fn as_slice<T>(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.data.cast::<T>(), self.end as usize) }
    }

    /// # SAFETY
    /// Must be called with the same type as `new`
    pub unsafe fn as_slice_mut<T>(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.data.cast::<T>(), self.end as usize) }
    }

    /// # SAFETY
    /// Must be called with the same type as `new`
    pub unsafe fn push<T: 'static>(&mut self, val: T) {
        unsafe {
            debug_assert!(self.end <= self.capacity);
            if self.end == self.capacity {
                // full, have to reallocate
                let new_cap = (self.capacity * 3 / 2).max(2);
                let (new_data, new_cap) = self.allocator.alloc::<T>(new_cap as usize);
                let new_data = new_data.as_ptr();
                for i in 0..self.end {
                    let t: T = std::ptr::read(self.data.cast::<T>().add(i as usize));
                    std::ptr::write(new_data.cast::<T>().add(i as usize), t);
                }
                self.allocator.dealloc::<T>(
                    NonNull::new_unchecked(self.data.cast::<T>()),
                    self.capacity as usize,
                );
                self.capacity = new_cap as u32;
                self.data = new_data.cast();
            }
            std::ptr::write(self.data.cast::<T>().add(self.end as usize), val);
            self.end += 1;
        }
    }

    /// # SAFETY
    /// Must be called with the same type as `new`
    pub unsafe fn swap_remove<T>(&mut self, i: usize) -> T {
        unsafe {
            debug_assert!(i < self.end as usize);
            let res;
            if i + 1 == self.end as usize {
                // last item
                res = std::ptr::read(self.data.cast::<T>().add(i));
            } else {
                res = std::ptr::read(self.data.cast::<T>().add(i));
                let last: T = std::ptr::read(self.data.cast::<T>().add(self.end as usize - 1));
                std::ptr::write(self.data.cast::<T>().add(i), last);
            }
            self.end -= 1;
            res
        }
    }

    pub fn remove(&mut self, id: RowIndex) {
        (self.swap_remove)(id, self);
    }

    #[cfg(feature = "clone")]
    pub fn clone_with_alloc(&self, allocator: ComponentAllocator) -> Self {
        (self.clone_with_alloc)(self, allocator)
    }
}

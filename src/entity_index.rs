use std::{
    alloc::{Layout, alloc, dealloc},
    mem::{align_of, size_of},
    ptr::{self, NonNull},
};

use crate::{
    EntityTable, RowIndex,
    entity_id::{ENTITY_GEN_MASK, ENTITY_INDEX_MASK, EntityId},
};

#[derive(Debug, Clone, thiserror::Error)]
pub enum HandleTableError {
    #[error("EntityIndex has no free capacity")]
    OutOfCapacity,
    #[error("Entity not found")]
    NotFound,
    #[error("Handle was not initialized")]
    Uninitialized,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum InsertError {
    #[error("Id {0} has already been allocated by a different entity")]
    Taken(EntityId),
    #[error("Id {0} has already been inserted")]
    AlreadyInserted(EntityId),
}

pub(crate) struct EntityIndex {
    entries: *mut Entry,
    cap: u32,
    /// Currently allocated entries
    count: u32,
    /// deallocated entities
    /// if empty allocate the next entity in the list
    free_list: u32,
}

#[cfg(feature = "clone")]
impl Clone for EntityIndex {
    fn clone(&self) -> Self {
        let mut result = Self::new(self.cap);
        result.entries_mut().copy_from_slice(self.entries());
        result.free_list = self.free_list;
        result.count = self.count;
        result
    }
}

unsafe impl Send for EntityIndex {}
unsafe impl Sync for EntityIndex {}

const SENTINEL: u32 = !0;

impl EntityIndex {
    pub fn new(initial_capacity: u32) -> Self {
        assert!(initial_capacity < ENTITY_INDEX_MASK);
        let entries;
        let cap = initial_capacity.max(1); // allocate at least 1 entry
        unsafe {
            entries = alloc(Layout::from_size_align_unchecked(
                size_of::<Entry>() * cap as usize,
                align_of::<Entry>(),
            )) as *mut Entry;
            assert!(!entries.is_null());
            for i in 0..cap {
                ptr::write(
                    entries.add(i as usize),
                    Entry {
                        generation: 0,
                        arch: std::ptr::null_mut(),
                        row_index: i + 1,
                    },
                );
            }
            (&mut *entries.add(cap as usize - 1)).row_index = SENTINEL;
        };
        Self {
            entries,
            cap,
            free_list: 0,
            count: 0,
        }
    }

    fn free_list_push(&mut self, index: u32) {
        let next = self.free_list;
        self.entries_mut()[index as usize].row_index = next;
        self.free_list = index;
    }

    fn free_list_pop(&mut self) -> Option<u32> {
        if self.free_list == SENTINEL {
            return None;
        }
        let result = self.free_list;
        self.free_list = self.entries()[result as usize].row_index;
        Some(result)
    }

    fn grow(&mut self, new_cap: u32) {
        #[cfg(feature = "tracing")]
        tracing::trace!("Growing from {} to {new_cap}", self.cap);
        let cap = self.cap;
        assert!(new_cap < ENTITY_INDEX_MASK);
        assert!(new_cap > cap);
        assert!(new_cap >= 2);
        let new_entries: *mut Entry;
        unsafe {
            new_entries = alloc(Layout::from_size_align_unchecked(
                size_of::<Entry>() * new_cap as usize,
                align_of::<Entry>(),
            ))
            .cast();

            ptr::copy_nonoverlapping(self.entries, new_entries, cap as usize);
            for i in cap..new_cap {
                ptr::write(
                    new_entries.add(i as usize),
                    Entry {
                        generation: 0,
                        arch: std::ptr::null_mut(),
                        row_index: i + 1,
                    },
                );
            }
            (&mut *new_entries.add(new_cap as usize - 1)).row_index = self.free_list;
            self.free_list = cap;

            dealloc(
                self.entries.cast(),
                Layout::from_size_align_unchecked(
                    size_of::<Entry>() * cap as usize,
                    align_of::<Entry>(),
                ),
            );
        }
        self.entries = new_entries;
        self.cap = new_cap;
    }

    pub fn len(&self) -> usize {
        self.count as usize
    }

    pub fn capacity(&self) -> usize {
        self.cap as usize
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Can resize the buffer, if out of capacity
    pub fn allocate_with_resize(&mut self) -> EntityId {
        if self.free_list == SENTINEL && self.count == self.cap {
            self.grow((self.cap as f32 * 3.0 / 2.0).ceil() as u32);
        }
        self.allocate().unwrap()
    }

    /// Insert is an O(n) operation that walks the free-list.
    /// Definitely avoid
    pub(crate) fn insert_id(&mut self, id: EntityId) -> Result<(), InsertError> {
        let needle = id.index();
        if self
            .entries()
            .get(needle as usize)
            .map(|entry| !entry.arch.is_null())
            .unwrap_or(false)
        {
            if self.entries()[needle as usize].generation == id.generation() {
                return Err(InsertError::AlreadyInserted(id));
            } else {
                return Err(InsertError::Taken(id));
            }
        }
        if needle as usize >= self.capacity() {
            self.grow(needle + 1);
        }
        unsafe {
            let mut free_list: *mut u32 = &mut self.free_list;
            while *free_list != SENTINEL {
                if *free_list == needle {
                    // unlink from the free list
                    let row = self.entries()[needle as usize].row_index;
                    *free_list = row;
                    self.init_allocated_id(needle);
                    self.entries_mut()[needle as usize].generation = id.generation();
                    #[cfg(feature = "tracing")]
                    tracing::trace!(%id, "Inserted");
                    return Ok(());
                }
                free_list = &mut self.entries_mut()[*free_list as usize].row_index;
            }
        }
        // not found
        if self.entries()[needle as usize].generation == id.generation() {
            return Err(InsertError::AlreadyInserted(id));
        } else {
            return Err(InsertError::Taken(id));
        }
    }

    /// Allocate will not grow the buffer, caller must ensure that sufficient capacity is reserved
    pub fn allocate(&mut self) -> Result<EntityId, HandleTableError> {
        // pop element off the free list
        //
        let index = match self.free_list_pop() {
            Some(i) => i,
            None => {
                if self.count == self.cap {
                    return Err(HandleTableError::OutOfCapacity);
                }
                self.count
            }
        };
        Ok(self.init_allocated_id(index))
    }

    fn init_allocated_id(&mut self, index: u32) -> EntityId {
        let entries = self.entries;
        self.count += 1;
        let entry;
        unsafe {
            entry = &mut *entries.add(index as usize);
            entry.arch = std::ptr::null_mut();
            entry.row_index = SENTINEL;
        }
        let id = EntityId::new(index as u32, entry.generation);
        #[cfg(feature = "tracing")]
        tracing::trace!(%id, "Initialized id");
        id
    }

    pub fn reserve(&mut self, additional: u32) {
        let new_cap = self.count + additional;
        if new_cap > self.cap {
            self.grow(new_cap);
        }
    }

    /// # Safety
    ///
    /// Caller must ensure that the id is valid
    pub(crate) unsafe fn update(&mut self, id: EntityId, arch: *mut EntityTable, row: RowIndex) {
        #[cfg(feature = "tracing")]
        tracing::trace!(%id, ?arch, %row, "Updating");
        unsafe {
            let index = id.index();
            debug_assert!(index < self.cap);
            let entry = &mut *self.entries.add(index as usize);
            debug_assert_eq!(id.generation(), entry.generation);
            entry.arch = arch;
            entry.row_index = row;
        }
    }

    /// # Safety
    ///
    /// Caller must ensure that the id is valid
    pub(crate) unsafe fn update_row_index(&mut self, id: EntityId, row: RowIndex) {
        #[cfg(feature = "tracing")]
        tracing::trace!(%id, %row, "Updating row index");
        unsafe {
            let index = id.index();
            debug_assert!(index < self.cap);
            let entry = &mut *self.entries.add(index as usize);
            debug_assert_eq!(id.generation(), entry.generation);
            entry.row_index = row;
        }
    }

    fn get(&self, id: EntityId) -> Option<&Entry> {
        let index = id.index();
        self.is_valid(id)
            .then(|| unsafe { &*self.entries.add(index as usize) })
    }

    pub fn read(&self, id: EntityId) -> Result<(NonNull<EntityTable>, RowIndex), HandleTableError> {
        let res = self.get(id).ok_or(HandleTableError::NotFound)?;
        if res.arch.is_null() {
            return Err(HandleTableError::Uninitialized);
        }

        Ok((unsafe { NonNull::new_unchecked(res.arch) }, res.row_index))
    }

    pub fn free(&mut self, id: EntityId) {
        #[cfg(feature = "tracing")]
        tracing::trace!(%id, "Freeing");
        self.count -= 1;
        let index = id.index();
        let entry: &mut Entry;
        unsafe {
            let entries = self.entries;
            entry = &mut *entries.add(index as usize);
        }
        debug_assert_eq!(id.generation(), entry.generation);
        entry.arch = std::ptr::null_mut();
        entry.row_index = SENTINEL;
        entry.generation = (entry.generation + 1) & ENTITY_GEN_MASK;
        self.free_list_push(index);
    }

    pub fn is_valid(&self, id: EntityId) -> bool {
        let index = id.index() as usize;
        let generation = id.generation();
        match self.entries().get(index) {
            Some(entry) => entry.generation == generation && entry.arch != std::ptr::null_mut(),
            None => return false,
        }
    }

    fn entries(&self) -> &[Entry] {
        unsafe { std::slice::from_raw_parts(self.entries, self.cap as usize) }
    }

    fn entries_mut(&mut self) -> &mut [Entry] {
        unsafe { std::slice::from_raw_parts_mut(self.entries, self.cap as usize) }
    }
}

impl Drop for EntityIndex {
    fn drop(&mut self) {
        unsafe {
            dealloc(
                self.entries.cast(),
                Layout::from_size_align_unchecked(
                    size_of::<Entry>() * (self.cap as usize),
                    align_of::<Entry>(),
                ),
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Entry {
    pub generation: u32,
    pub arch: *mut EntityTable,
    /// Store the index of the entity in the EntityTable
    /// When entity is invalid the row_index stores the position of the next Entry in the free-list
    pub row_index: RowIndex,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alloc() {
        let mut table = EntityIndex::new(512);

        for _ in 0..4 {
            let e = table.allocate().unwrap();
            assert_eq!(e.generation(), 0); // assert for the next step in the test
        }
        for i in 0..4 {
            let e = EntityId::new(i, 0);
            table.free(e);
            assert!(!table.is_valid(e));
        }
        for _ in 0..512 {
            let _e = table.allocate();
        }
    }

    #[test]
    fn dealloc_test() {
        let mut table = EntityIndex::new(512);

        let a = table.allocate().unwrap();

        table.free(a);

        assert!(!table.is_valid(a));
    }

    #[test]
    fn can_grow_handles_test() {
        let mut table = EntityIndex::new(0);

        table.reserve(128);
        for _ in 0..128 {
            table.allocate().unwrap();
        }
    }

    #[test]
    fn reuses_entity_ids_test() {
        let mut table = EntityIndex::new(0);

        table.reserve(128);

        let mut a = table.allocate_with_resize();
        for _ in 0..10 {
            table.free(a);
            let b = table.allocate_with_resize();
            assert_eq!(a.index(), b.index());
            assert_ne!(a.generation(), b.generation());
            a = b;
        }
    }

    #[test]
    fn walking_the_free_list_terminates_test() {
        // see if the sentinel value is in the chain after resizing
        let mut table = EntityIndex::new(0);

        let ids = (0..256)
            .map(|_| table.allocate_with_resize())
            .collect::<Vec<_>>();
        for id in ids {
            table.free(id);
        }

        let mut next = table.free_list;
        let mut cnt = 0;
        let cap = table.capacity();
        while next != SENTINEL {
            cnt += 1;
            let entry = &table.entries()[next as usize];
            next = entry.row_index;
            assert!(entry.arch.is_null());
            assert!(cnt <= cap);
        }
        assert_eq!(cnt, cap);
    }
}

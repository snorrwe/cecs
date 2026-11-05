use std::{any::TypeId, mem::size_of, ptr::NonNull, sync::Arc};

use parking_lot::Mutex;
use rustc_hash::FxHashMap;

#[derive(Clone)]
pub struct ComponentAllocator {
    columns: Arc<Mutex<FxHashMap<TypeId, Vec<ColumnAllocator>>>>,
}

unsafe impl Send for ComponentAllocator {}
unsafe impl Sync for ComponentAllocator {}

const BLOCK_SIZE: usize = 1048576; // 1MiB

impl ComponentAllocator {
    pub fn new() -> Self {
        Self {
            columns: Default::default(),
        }
    }

    /// Return the pointer to the allocation and the actual size of the allocation, which is at
    /// least num_items
    pub fn alloc<T: 'static>(&self, num_items: usize) -> (NonNull<T>, usize) {
        let ty = TypeId::of::<T>();

        let mut cols = self.columns.lock();
        let allocators = cols.entry(ty).or_insert_with(|| {
            let first = ColumnAllocator::new::<T>(num_items);
            vec![first]
        });

        for alloc in allocators.iter_mut() {
            unsafe {
                if let Some((ptr, size)) = alloc.try_alloc::<T>(num_items) {
                    return (NonNull::new_unchecked(ptr), size);
                }
            }
        }

        let mut next = ColumnAllocator::new::<T>(num_items);
        let (ptr, size) = unsafe { next.try_alloc(num_items).unwrap() };
        allocators.push(next);
        unsafe { (NonNull::new_unchecked(ptr), size) }
    }

    /// # SAFETY must be called with the same type as alloc
    pub unsafe fn dealloc<T: 'static>(&self, start: NonNull<T>, size: usize) {
        if size_of::<T>() == 0 {
            return;
        }
        let mut cols = self.columns.lock();
        let allocators = cols
            .get_mut(&TypeId::of::<T>())
            .expect("Allocator collection not found for type");

        for alloc in allocators.iter_mut() {
            if alloc.contains(start.as_ptr().cast()) {
                unsafe { alloc.dealloc::<T>(start.as_ptr(), size) };
                return;
            }
        }
        panic!("Pointer {start:?} was not allocated by this allocator",);
    }
}

impl Default for ComponentAllocator {
    fn default() -> Self {
        Self::new()
    }
}

struct ColumnAllocator {
    pub layout: std::alloc::Layout,
    pub start: *mut u8,
    pub head: *mut u8,
    pub end: *mut u8,
    /// Sorted by the start ptrs
    /// Blocks must never overlap
    pub free_stack: Vec<FreeBlock>,
}

struct FreeBlock {
    pub start: *mut u8,
    pub end: *mut u8,
}

impl FreeBlock {
    pub fn size_bytes(&self) -> usize {
        unsafe { self.end.offset_from(self.start).try_into().unwrap() }
    }
}

impl Drop for ColumnAllocator {
    fn drop(&mut self) {
        unsafe {
            std::alloc::dealloc(self.start, self.layout);
        }
    }
}

impl ColumnAllocator {
    /// num_items = number of maximum elements of T to store in this allocation
    pub fn new<T>(mut num_items: usize) -> Self {
        if num_items * size_of::<T>() < BLOCK_SIZE && size_of::<T>() > 0 {
            num_items = BLOCK_SIZE / size_of::<T>() + 1;
        }

        let layout = std::alloc::Layout::from_size_align(
            (num_items * size_of::<T>()).max(1),
            align_of::<T>().max(8),
        )
        .unwrap();

        let ptr = unsafe { std::alloc::alloc(layout) };

        assert!(!ptr.is_null(), "Memory allocation failed");

        Self {
            start: ptr,
            layout,
            head: ptr,
            end: unsafe { ptr.add(layout.size()) },
            free_stack: Default::default(),
        }
    }

    fn remaining_bytes(&self) -> usize {
        unsafe { self.end.offset_from(self.head).try_into().unwrap() }
    }

    /// # SAFETY
    /// Must be called with the same type as new
    ///
    /// return: On success Some start of allocation and actual size in items
    pub unsafe fn try_alloc<T>(&mut self, num_items: usize) -> Option<(*mut T, usize)> {
        let byte_size = size_of::<T>() * num_items;

        // TODO: maybe try to find a tighter bound instead of the first?
        for i in (0..self.free_stack.len()).rev() {
            match self.free_stack[i].size_bytes().cmp(&byte_size) {
                std::cmp::Ordering::Equal => {
                    let alloc = self.free_stack.remove(i);
                    return Some((alloc.start.cast(), num_items));
                }
                std::cmp::Ordering::Greater => {
                    if self.free_stack[i].size_bytes() / 2 > byte_size {
                        let alloc = &mut self.free_stack[i];
                        // the free alloc is substantially higher, split
                        unsafe {
                            let end: *mut T = alloc.start.cast::<T>().add(num_items);
                            let start = std::mem::replace(&mut alloc.start, end.cast());
                            return Some((start.cast(), num_items));
                        }
                    } else {
                        // the free alloc is a bit larger, give the caller all of the block

                        let alloc = self.free_stack.remove(i);
                        return Some((alloc.start.cast(), alloc.size_bytes() / size_of::<T>()));
                    }
                }
                std::cmp::Ordering::Less => {
                    continue;
                }
            }
        }

        // no freed allocation try allocating from the open block

        let start: *mut u8 = self.head;
        if self.remaining_bytes() < byte_size {
            return None;
        }

        self.head = unsafe { self.head.add(byte_size) };
        Some((start.cast(), num_items))
    }

    /// # SAFETY
    /// Must be called with the same type as new
    /// Must be called with a valid alloc obtained from try_alloc of this instance
    pub unsafe fn dealloc<T>(&mut self, start: *mut T, num_items: usize) {
        let block = FreeBlock {
            start: start.cast(),
            end: unsafe { start.add(num_items).cast() },
        };

        let i = self
            .free_stack
            .binary_search_by(|b| b.start.cmp(&block.start))
            .expect_err("Double free; found the start position as a free block");

        if i < self.free_stack.len() {
            if i > 0 && self.free_stack[i - 1].end == block.start {
                // merge with back candidate
                self.free_stack[i - 1].end = block.end;
                // check if we can merge with next
                if self.free_stack[i - 1].end == self.free_stack[i].start {
                    self.free_stack[i - 1].end = self.free_stack[i].end;
                    self.free_stack.remove(i);
                }
            } else if block.end == self.free_stack[i].start {
                // merge with front candidate
                self.free_stack[i].start = block.start;
            }
        } else if i > 0 && self.free_stack[i - 1].end == block.start {
            // merge with back candidate
            self.free_stack[i - 1].end = block.end;
        } else {
            self.free_stack.insert(i, block);
        }
    }

    pub fn contains(&self, ptr: *mut u8) -> bool {
        unsafe {
            let o = ptr.offset_from(self.start);
            if o < 0 {
                return false;
            }
            ptr.offset_from(self.end) < 0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocs_do_not_overlap() {
        let mut allocator = ColumnAllocator::new::<i32>(1_000_000);

        let allocs = (0..1000)
            .map(|_| unsafe { allocator.try_alloc::<i32>(1000).unwrap() })
            .collect::<Vec<_>>();

        let mut last = allocs[0];

        for alloc in allocs[1..].iter() {
            unsafe {
                let end = last.0.add(last.1);
                assert!(end <= alloc.0);
            }
            last = *alloc;
        }
    }

    #[expect(dead_code)]
    struct NeverConstructed(i32);
    impl Drop for NeverConstructed {
        fn drop(&mut self) {
            panic!("This object should not have been constructed");
        }
    }

    #[test]
    fn test_allocation_does_not_construct_objects() {
        let mut allocator = ColumnAllocator::new::<NeverConstructed>(1024);

        unsafe {
            let (start, size) = allocator.try_alloc::<NeverConstructed>(1024).unwrap();
            allocator.dealloc(start, size);
        }
    }

    #[test]
    fn test_reuses_allocation() {
        let mut allocator = ColumnAllocator::new::<i32>(1024);

        unsafe {
            let (start, size) = allocator.try_alloc::<i32>(1).unwrap();
            allocator.dealloc(start, size);
            let (start_2, size_2) = allocator.try_alloc::<i32>(1).unwrap();

            assert_eq!(size, 1);
            assert_eq!(size, size_2);
            assert_eq!(start, start_2);
        }
    }
}

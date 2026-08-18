use std::{any::TypeId, cell::UnsafeCell};

use rustc_hash::FxHashMap;

use crate::Component;

pub struct ResourceStorage {
    pub(crate) resources: FxHashMap<TypeId, UnsafeCell<ErasedResource>>,
}

#[cfg(feature = "clone")]
impl Clone for ResourceStorage {
    fn clone(&self) -> Self {
        Self {
            resources: self
                .resources
                .iter()
                .map(|(id, table)| (*id, UnsafeCell::new(unsafe { &*table.get() }.clone())))
                .collect(),
        }
    }
}

impl Default for ResourceStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl ResourceStorage {
    pub fn new() -> Self {
        Self {
            resources: Default::default(),
        }
    }

    pub fn insert<T: Component>(&mut self, value: T) {
        match self.resources.entry(TypeId::of::<T>()) {
            std::collections::hash_map::Entry::Occupied(mut x) => {
                x.insert(UnsafeCell::new(ErasedResource::new(value)));
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(UnsafeCell::new(ErasedResource::new(value)));
            }
        }
    }

    pub fn fetch<T: 'static>(&self) -> Option<&T> {
        self.resources
            .get(&TypeId::of::<T>())
            .map(|table| unsafe { (*table.get()).as_inner::<T>() })
    }

    /// # SAFETY caller must ensure that no mutable aliasing happens to the value
    pub unsafe fn fetch_mut<T: 'static>(&self) -> Option<&mut T> {
        self.resources
            .get(&TypeId::of::<T>())
            .map(|table| unsafe { (*table.get()).as_inner_mut::<T>() })
    }

    pub fn fetch_or_default<T: Default + Component>(&mut self) -> &mut T {
        let res = self
            .resources
            .entry(TypeId::of::<T>())
            .or_insert_with(|| UnsafeCell::new(ErasedResource::new(T::default())));
        unsafe { res.get_mut().as_inner_mut::<T>() }
    }

    pub fn remove<T: 'static>(&mut self) -> Option<Box<T>> {
        self.resources
            .remove(&TypeId::of::<T>())
            .map(|table| unsafe { table.into_inner().into_inner() })
    }

    pub fn contains<T: 'static>(&self) -> bool {
        self.resources.contains_key(&TypeId::of::<T>())
    }

    pub fn len(&self) -> usize {
        self.resources.len()
    }
}

pub(crate) struct ErasedResource {
    inner: *mut u8,
    finalize: fn(&mut ErasedResource),
    #[cfg(feature = "clone")]
    clone: fn(&ErasedResource) -> ErasedResource,
}

impl Drop for ErasedResource {
    fn drop(&mut self) {
        (self.finalize)(self);
    }
}

#[cfg(feature = "clone")]
impl Clone for ErasedResource {
    fn clone(&self) -> Self {
        (self.clone)(self)
    }
}

impl ErasedResource {
    pub fn new<T: Component>(value: T) -> Self {
        let inner = Box::leak(Box::new(value));
        Self {
            inner: (inner as *mut T).cast(),
            finalize: |resource| unsafe {
                if !resource.inner.is_null() {
                    let _inner: Box<T> = Box::from_raw(resource.inner.cast::<T>());
                    resource.inner = std::ptr::null_mut();
                }
            },
            #[cfg(feature = "clone")]
            clone: |resource| unsafe {
                let val = resource.as_inner::<T>().clone();
                ErasedResource::new(val)
            },
        }
    }

    /// # SAFETY
    /// Must be called with the same type as `new`
    pub unsafe fn as_inner<T>(&self) -> &T {
        unsafe { &*self.inner.cast() }
    }

    /// # SAFETY
    /// Must be called with the same type as `new`
    pub unsafe fn as_inner_mut<T>(&mut self) -> &mut T {
        unsafe { &mut *self.inner.cast() }
    }

    pub unsafe fn into_inner<T>(mut self) -> Box<T> {
        unsafe {
            let inner = self.inner;
            self.inner = std::ptr::null_mut();
            Box::from_raw(inner.cast())
        }
    }
}

use std::sync::atomic::{AtomicBool, Ordering};

use crate::query::{SystemProperties, WorldQuery};

/// Gives unique access to the world
///
/// Systems that take `WorldAccess` can not have other queries and will run in their own schedule
/// group.
///
///
/// ```
/// use cecs::prelude::*;
///
/// fn sys(mut access: WorldAccess) {
///     let w = access.world_mut();
///     // you can use the world as you would normally
///     w.run_system(||{}).unwrap();
/// }
///
/// let mut world = World::new(0);
/// ```
///
/// ## Limitation
///
/// You must not stack WorldAccess queries! Do not run a system that needs WorldAccess inside
/// another system with WorldAccess as they might violate Rust's borrow rules.
///
/// WorldAccess is unstable and the author would recommend avoiding using it for now
pub struct WorldAccess<'a> {
    world: std::ptr::NonNull<crate::World>,
    _guard: WorldLockGuard<'a>,
}

// this has nothing World specific in it, I just suck at naming
//
// this lock's job is to panic if users try to stack WorldAccesses
pub(crate) struct WorldLock {
    locked: AtomicBool,
}

pub(crate) struct WorldLockGuard<'a> {
    lock: &'a WorldLock,
}

impl<'a> Drop for WorldLockGuard<'a> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

impl WorldLock {
    pub fn new() -> Self {
        Self {
            locked: AtomicBool::new(false),
        }
    }

    pub fn lock(&self) -> WorldLockGuard<'_> {
        self.locked
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .expect("Can not be locked twice");
        WorldLockGuard { lock: self }
    }

    pub fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }
}

impl WorldAccess<'_> {
    pub fn world(&self) -> &crate::World {
        unsafe { self.world.as_ref() }
    }

    pub fn world_mut(&mut self) -> &mut crate::World {
        unsafe { self.world.as_mut() }
    }
}

unsafe impl Send for WorldAccess<'_> {}
unsafe impl Sync for WorldAccess<'_> {}

unsafe impl<'a> WorldQuery<'a> for WorldAccess<'a> {
    fn new(db: &'a crate::World, _props: SystemProperties) -> Self {
        let _guard = db.this_lock.lock();
        WorldAccess {
            world: db.into(),
            _guard,
        }
    }

    fn exclusive() -> bool {
        true
    }

    fn read_only() -> bool {
        false
    }
}

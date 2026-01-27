use std::ptr::NonNull;

use crate::{
    Component, UnsafeBuffer, World, WorldError, entity_id::EntityId, prelude::Bundle,
    query::WorldQuery,
};

pub struct Commands<'a> {
    world: &'a World,
    cmd: &'a UnsafeBuffer<CommandPayload>,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum CommandError {
    #[error("Failed to delete entity: {id}")]
    ErrDelete { id: EntityId },
    #[error("Inserted id ({0}) is invalid and can not be inserted")]
    InsertInvalidId(EntityId),
    #[error("Failed to merge entities ({src}, {dst}): {err:?}")]
    MergeFail {
        src: EntityId,
        dst: EntityId,
        err: WorldError,
    },
    #[error("Failed to insert component (id={id}) (ty={ty}): {err:?}")]
    ComponentInsertFailed {
        id: EntityId,
        ty: &'static str,
        err: WorldError,
    },
    #[error("Failed to remove component (id={id}) (ty={ty}): {err:?}")]
    ComponentRemoveFailed {
        id: EntityId,
        ty: &'static str,
        err: WorldError,
    },
}

unsafe impl<'a> Send for Commands<'a> {}
unsafe impl<'a> Sync for Commands<'a> {}

// used to ensure no duplicate commands are present on a system
struct CommandSentinel;

unsafe impl<'a> WorldQuery<'a> for Commands<'a> {
    fn new(w: &'a World, system_idx: usize) -> Self {
        Self::new(w, system_idx)
    }

    fn resources_mut(set: &mut std::collections::HashSet<std::any::TypeId>) {
        set.insert(std::any::TypeId::of::<CommandSentinel>());
    }
}

impl<'a> Commands<'a> {
    pub(crate) fn new(w: &'a World, system_idx: usize) -> Self {
        Self {
            world: &w,
            cmd: &w.commands[system_idx],
        }
    }

    /// Reserve storage for  `additional` number of additional entities.
    ///
    /// Reserve happens lazily, this can be used in conjunction with `spawn` but not with `id()`
    ///
    ///
    /// ```
    /// # use cecs::prelude::*;
    /// fn sys(mut cmd: Commands) {
    ///     cmd.reserve_entities(128);
    ///     for i in 0..128 {
    ///         // this is fine
    ///         cmd.spawn().insert(i);
    ///     }
    /// }
    /// # let mut world = World::new(0);
    /// # world.run_system(sys).unwrap();
    /// ```
    ///
    /// ```should_panic
    /// # use cecs::prelude::*;
    /// fn sys(mut cmd: Commands) {
    ///     cmd.reserve_entities(128);
    ///     for i in 0..128 {
    ///         // id will return an error, since the reserve has not happened yet
    ///         cmd.spawn().id().unwrap();
    ///     }
    /// }
    /// # let mut world = World::new(0);
    /// # world.run_system(sys).unwrap();
    /// ```
    pub fn reserve_entities(&mut self, additional: u32) {
        unsafe {
            let cmd = &mut *self.cmd.get();
            cmd.push(CommandPayload::World(WorldCommands::Reserve { additional }));
        }
    }

    pub fn entity(&mut self, id: EntityId) -> &mut EntityCommands {
        unsafe {
            let cmd = &mut *self.cmd.get();
            cmd.push(CommandPayload::Entity(EntityCommands {
                world: self.world,
                action: EntityAction::Fetch(id),
                payload: Vec::default(),
            }));
            cmd.last_mut().unwrap().entity_mut()
        }
    }

    /// Take a pre-made id and insert into the world
    /// Will cause an error if the id slot is taken
    pub fn insert_id(&mut self, id: EntityId) -> &mut EntityCommands {
        unsafe {
            let cmd = &mut *self.cmd.get();
            cmd.push(CommandPayload::Entity(EntityCommands {
                world: self.world,
                action: EntityAction::InsertId(id),
                payload: Vec::default(),
            }));
            cmd.last_mut().unwrap().entity_mut()
        }
    }

    pub fn spawn(&mut self) -> &mut EntityCommands {
        unsafe {
            let cmd = &mut *self.cmd.get();
            cmd.push(CommandPayload::Entity(EntityCommands {
                world: self.world,
                action: EntityAction::Insert,
                payload: Vec::default(),
            }));
            cmd.last_mut().unwrap().entity_mut()
        }
    }

    pub fn delete(&mut self, id: EntityId) {
        unsafe {
            let cmd = &mut *self.cmd.get();
            cmd.push(CommandPayload::Entity(EntityCommands {
                world: self.world,
                action: EntityAction::Delete(id),
                payload: Vec::default(),
            }));
        }
    }

    pub fn insert_resource<T: Component>(&mut self, resource: T) {
        unsafe {
            let cmd = &mut *self.cmd.get();
            cmd.push(CommandPayload::Resource(ErasedResourceCommand::new(
                ResourceCommand::Insert(resource),
            )));
        }
    }

    pub fn remove_resource<T: Component>(&mut self) {
        unsafe {
            let cmd = &mut *self.cmd.get();
            cmd.push(CommandPayload::Resource(ErasedResourceCommand::new(
                ResourceCommand::<T>::Delete,
            )));
        }
    }

    pub fn merge_entities(&mut self, src: EntityId, dst: EntityId) {
        unsafe {
            let cmd = &mut *self.cmd.get();
            cmd.push(CommandPayload::Entity(EntityCommands {
                world: self.world,
                action: EntityAction::Merge { src, dst },
                payload: Vec::default(),
            }));
        }
    }
}

#[derive(Default)]
pub(crate) enum CommandPayload {
    #[default]
    Noop,
    Entity(EntityCommands),
    Resource(ErasedResourceCommand),
    World(WorldCommands),
}

fn action_ty(c: &EntityAction) -> (u32, u32) {
    match c {
        EntityAction::Init(id) => (id.index(), 0),
        EntityAction::InsertId(id) => (id.index(), 0),
        EntityAction::Insert => (!0, 0),
        EntityAction::Fetch(id) => (id.index(), 1),
        EntityAction::Merge { src, .. } => (src.index(), 1),
        EntityAction::Delete(id) => (id.index(), 2),
    }
}

fn cmd_ty(c: &CommandPayload) -> (u32, u32) {
    match c {
        CommandPayload::Entity(e) => action_ty(&e.action),
        CommandPayload::Resource(_) => (!0, 3),
        CommandPayload::World(_) => (!0, 4),
        CommandPayload::Noop => (!0, 5),
    }
}

/// Sort actions by type, then entity commands as well.
///
/// Entity Commands ordering:
/// - first sort by their entity id, then sort by:
///     1. insert actions
///     2. update actions
///     3. delete actions
///
/// This way deleting and updating in the same tick is handled as one might expect
///
/// TODO: possible improvements:
/// - perform insert actions
/// - sort by entity ids
/// - merge updates
/// - if theres a delete action, then discard the other updates
pub(crate) fn prepare_commands(cmd: &mut Vec<CommandPayload>) {
    cmd.sort_by_key(cmd_ty);

    // deduplicate entity commands

    if let Some(entity_commands) = cmd
        .chunk_by_mut(|a, b| cmd_ty(a) == cmd_ty(b))
        .filter(|g| matches!(&g[0], &CommandPayload::Entity(_)))
        .next()
    {
        for g in entity_commands.chunk_by_mut(|a, b| match (a, b) {
            (CommandPayload::Entity(a), CommandPayload::Entity(b)) => a.can_merge(b),
            _ => false,
        }) {
            if let Some((mut merged, rest)) = g.split_first_mut() {
                let CommandPayload::Entity(m) = &mut merged else {
                    unreachable!();
                };
                for pl in rest {
                    let CommandPayload::Entity(pl) = std::mem::take(pl) else {
                        unreachable!();
                    };
                    m.merge(pl);
                }
            }
        }
    }
}

impl CommandPayload {
    pub(crate) fn apply(self, world: &mut World) -> Result<(), CommandError> {
        match self {
            CommandPayload::Entity(c) => c.apply(world),
            CommandPayload::Resource(c) => c.apply(world),
            CommandPayload::World(c) => c.apply(world),
            CommandPayload::Noop => Ok(()),
        }
    }

    fn entity_mut(&mut self) -> &mut EntityCommands {
        match self {
            CommandPayload::Entity(cmd) => cmd,
            _ => panic!("Command is not entity command"),
        }
    }
}

pub struct EntityCommands {
    /// if the action is delete or merge, then `payload` is ignored
    action: EntityAction,
    world: *const World,
    payload: Vec<ErasedComponentCommand>,
}

#[derive(Debug, Clone, Copy, Eq)]
pub enum EntityAction {
    Fetch(EntityId),
    /// Like fetch, but initialize the id first
    /// Insert actions can become Init actions if the id is requested
    Init(EntityId),
    InsertId(EntityId),
    Insert,
    Delete(EntityId),
    Merge {
        src: EntityId,
        dst: EntityId,
    },
}

impl PartialEq for EntityAction {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Fetch(l), Self::Fetch(r)) => l == r,
            (Self::Init(l), Self::Init(r)) => l == r,
            (Self::InsertId(l), Self::InsertId(r)) => l == r,
            (Self::Delete(l), Self::Delete(r)) => l == r,
            (
                Self::Merge {
                    src: l_src,
                    dst: l_dst,
                },
                Self::Merge {
                    src: r_src,
                    dst: r_dst,
                },
            ) => l_src == r_src && l_dst == r_dst,
            // insert insert are never equal
            _ => false,
        }
    }
}

impl EntityCommands {
    pub fn can_merge(&self, other: &EntityCommands) -> bool {
        self.action == other.action
    }

    pub fn try_merge(&mut self, other: EntityCommands) -> Result<(), EntityCommands> {
        if !self.can_merge(&other) {
            return Err(other);
        }
        self.payload.extend(other.payload.into_iter());
        Ok(())
    }

    pub fn merge(&mut self, other: EntityCommands) {
        self.try_merge(other).map_err(drop).unwrap();
    }

    /// Note: fetching the `id` will force entity allocation, which can trigger out of capacity
    /// error, even if you have reserved in this system stage.
    ///
    /// Ensure that you allocate enough capacity in a previous stage (or tick) before calling
    /// `id()`
    pub fn id(&mut self) -> Result<EntityId, crate::entity_index::HandleTableError> {
        match self.action {
            EntityAction::InsertId(id)
            | EntityAction::Init(id)
            | EntityAction::Fetch(id)
            | EntityAction::Delete(id) => Ok(id),
            EntityAction::Insert => unsafe {
                let world = &*self.world;
                let _guard = world.this_lock.lock();
                let index = world.entity_ids.get();
                let id = (*index).allocate()?;
                self.action = EntityAction::Init(id);
                Ok(id)
            },
            EntityAction::Merge { src: _, dst } => Ok(dst),
        }
    }

    pub(crate) fn apply(self, world: &mut World) -> Result<(), CommandError> {
        let id = match self.action {
            EntityAction::Fetch(id) => id,
            EntityAction::Init(id) => {
                unsafe {
                    world.init_id(id);
                }
                // ensure entity buffer growth
                world.reserve_entities(1);
                id
            }
            EntityAction::InsertId(id) => {
                if let Err(err) = world.insert_id(id) {
                    match err {
                        crate::entity_index::InsertError::Taken(_) => {
                            return Err(CommandError::InsertInvalidId(id));
                        }
                        crate::entity_index::InsertError::AlreadyInserted(_) => { /*ignore*/ }
                    }
                }
                id
            }
            EntityAction::Insert => world.insert_entity(),
            EntityAction::Delete(id) => {
                if let Err(_err) = world.delete_entity(id) {
                    #[cfg(feature = "tracing")]
                    tracing::debug!(
                        id = tracing::field::display(id),
                        error = tracing::field::display(_err),
                        "Entity can't be deleted"
                    );
                }
                return Ok(());
            }
            EntityAction::Merge { src, dst } => {
                return world
                    .merge_entities(src, dst)
                    .map_err(|err| CommandError::MergeFail { src, dst, err });
            }
        };
        for cmd in self.payload {
            cmd.apply(id, world)?;
        }
        Ok(())
    }

    pub fn insert<T: Component>(&mut self, component: T) -> &mut Self {
        self.payload.push(ErasedComponentCommand::from_component(
            ComponentCommand::Insert(component),
        ));
        self
    }

    pub fn insert_bundle<T: Bundle>(&mut self, bundle: T) -> &mut Self {
        self.payload
            .push(ErasedComponentCommand::from_bundle(BundleCommand::Insert(
                bundle,
            )));
        self
    }

    pub fn remove<T: Component>(&mut self) -> &mut Self {
        self.payload.push(ErasedComponentCommand::from_component(
            ComponentCommand::<T>::Delete,
        ));
        self
    }
}

pub(crate) struct ErasedComponentCommand {
    inner: *mut (),
    apply: fn(NonNull<()>, EntityId, &mut World) -> Result<(), CommandError>,
    drop: fn(NonNull<()>),
}

unsafe impl Send for ErasedComponentCommand {}
unsafe impl Sync for ErasedComponentCommand {}

impl Drop for ErasedComponentCommand {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            (self.drop)(NonNull::new(self.inner).unwrap());
        }
    }
}

impl ErasedComponentCommand {
    pub fn apply(mut self, id: EntityId, world: &mut World) -> Result<(), CommandError> {
        let ptr = NonNull::new(self.inner).unwrap();
        self.inner = std::ptr::null_mut();
        (self.apply)(ptr, id, world)
    }

    pub fn from_component<T: Component>(inner: ComponentCommand<T>) -> Self {
        let inner = (Box::leak(Box::new(inner)) as *mut ComponentCommand<T>).cast();
        Self {
            inner,
            drop: |ptr| {
                let mut ptr = ptr.cast();
                let _ptr: Box<ComponentCommand<T>> = unsafe { Box::from_raw(ptr.as_mut()) };
            },
            apply: |ptr, id, world| {
                let mut ptr = ptr.cast();
                let ptr: Box<ComponentCommand<T>> = unsafe { Box::from_raw(ptr.as_mut()) };
                ptr.apply(id, world)?;
                Ok(())
            },
        }
    }

    pub fn from_bundle<T: Bundle>(inner: BundleCommand<T>) -> Self {
        let inner = (Box::leak(Box::new(inner)) as *mut BundleCommand<T>).cast();
        Self {
            inner,
            drop: |ptr| {
                let mut ptr = ptr.cast();
                let _ptr: Box<BundleCommand<T>> = unsafe { Box::from_raw(ptr.as_mut()) };
            },
            apply: |ptr, id, world| {
                let mut ptr = ptr.cast();
                let ptr: Box<BundleCommand<T>> = unsafe { Box::from_raw(ptr.as_mut()) };
                ptr.apply(id, world)?;
                Ok(())
            },
        }
    }
}

pub(crate) enum BundleCommand<T> {
    Insert(T),
}

impl<T: Bundle> BundleCommand<T> {
    fn apply(self, entity_id: EntityId, world: &mut World) -> Result<(), CommandError> {
        match self {
            BundleCommand::Insert(bundle) => {
                world.set_bundle(entity_id, bundle).map_err(|err| {
                    CommandError::ComponentInsertFailed {
                        id: entity_id,
                        ty: std::any::type_name::<T>(),
                        err,
                    }
                })?;
            }
        }
        Ok(())
    }
}

pub(crate) enum ComponentCommand<T> {
    Insert(T),
    Delete,
}

impl<T: Component> ComponentCommand<T> {
    fn apply(self, entity_id: EntityId, world: &mut World) -> Result<(), CommandError> {
        match self {
            ComponentCommand::Insert(comp) => {
                world.set_component(entity_id, comp).map_err(|err| {
                    CommandError::ComponentInsertFailed {
                        id: entity_id,
                        ty: std::any::type_name::<T>(),
                        err,
                    }
                })?;
            }
            ComponentCommand::Delete => {
                if let Err(err) = world.remove_component::<T>(entity_id) {
                    match err {
                        WorldError::ComponentNotFound => { /*ignore*/ }
                        WorldError::InsertInvalidId(_)
                        | WorldError::OutOfCapacity
                        | WorldError::EntityNotFound => {
                            return Err(CommandError::ComponentRemoveFailed {
                                id: entity_id,
                                ty: std::any::type_name::<T>(),
                                err,
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

pub(crate) struct ErasedResourceCommand {
    inner: *mut (),
    apply: fn(NonNull<()>, &mut World) -> Result<(), CommandError>,
    drop: fn(NonNull<()>),
}

unsafe impl Send for ErasedResourceCommand {}
unsafe impl Sync for ErasedResourceCommand {}

impl Drop for ErasedResourceCommand {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            (self.drop)(NonNull::new(self.inner).unwrap());
        }
    }
}

impl ErasedResourceCommand {
    pub fn new<T: Component>(inner: ResourceCommand<T>) -> Self {
        let inner = (Box::leak(Box::new(inner)) as *mut ResourceCommand<T>).cast();
        Self {
            inner,
            drop: |ptr| {
                let mut ptr = ptr.cast();
                let _ptr: Box<ResourceCommand<T>> = unsafe { Box::from_raw(ptr.as_mut()) };
            },
            apply: |ptr, world| {
                let mut ptr = ptr.cast();
                let cmd: Box<ResourceCommand<T>> = unsafe { Box::from_raw(ptr.as_mut()) };
                cmd.apply(world)?;
                Ok(())
            },
        }
    }

    pub fn apply(mut self, world: &mut World) -> Result<(), CommandError> {
        // self.inner will be dropped twice unless we clear it now
        let ptr = self.inner;
        self.inner = std::ptr::null_mut();
        (self.apply)(NonNull::new(ptr).unwrap(), world)
    }
}

pub(crate) enum ResourceCommand<T> {
    Insert(T),
    Delete,
}

impl<T: Component> ResourceCommand<T> {
    fn apply(self, world: &mut World) -> Result<(), CommandError> {
        match self {
            ResourceCommand::Insert(comp) => {
                world.insert_resource::<T>(comp);
            }
            ResourceCommand::Delete => {
                let _ = world.remove_resource::<T>();
            }
        }
        Ok(())
    }
}

pub(crate) enum WorldCommands {
    Reserve { additional: u32 },
}

impl WorldCommands {
    pub fn apply(self, world: &mut World) -> Result<(), CommandError> {
        match self {
            WorldCommands::Reserve { additional } => world.reserve_entities(additional),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::query::Query;

    use super::*;

    #[test]
    fn can_add_entity_via_cmd_test() {
        let mut world = World::new(100);

        let mut cmd = world.ensure_commands();
        cmd.spawn().insert(69i32);
        cmd.spawn().insert(69i32);
        cmd.spawn().insert(69i32);

        drop(cmd);

        world.apply_commands().unwrap();

        let mut cnt = 0;
        for i in Query::<&i32>::new(&world).iter() {
            cnt += 1;
            assert_eq!(i, &69);
        }

        assert_eq!(cnt, 3);
    }

    #[test]
    fn can_remove_component_test() {
        let mut world = World::new(100);

        let id = world.insert_entity();
        world.set_component(id, 69i32).unwrap();

        let _c = Query::<&i32>::new(&world).fetch(id).unwrap();

        let mut cmd = world.ensure_commands();
        cmd.entity(id).remove::<i32>();
        drop(cmd);
        world.apply_commands().unwrap();

        let q = Query::<&i32>::new(&world);
        let c = q.fetch(id);
        assert!(c.is_none());

        // entity still exists
        let _c = Query::<&()>::new(&world).fetch(id).unwrap();
    }

    #[test]
    fn can_delete_entity_test() {
        let mut world = World::new(100);

        let id = world.insert_entity();
        world.set_component(id, 69i32).unwrap();

        let _c = Query::<&i32>::new(&world).fetch(id).unwrap();

        let mut cmd = world.ensure_commands();
        cmd.delete(id);
        drop(cmd);
        world.apply_commands().unwrap();

        let q = Query::<&i32>::new(&world);
        let c = q.fetch(id);
        assert!(c.is_none());

        // entity should not exists
        let q = Query::<&()>::new(&world);
        let c = q.fetch(id);
        assert!(c.is_none());
    }

    #[test]
    fn double_insert_test() {
        let mut world = World::new(0);

        {
            let mut cmd = world.ensure_commands();
            cmd.insert_resource(0i32);

            drop(cmd);
            world.apply_commands().unwrap();
        }
        {
            let mut cmd = world.ensure_commands();
            cmd.insert_resource(42i32);

            drop(cmd);
            world.apply_commands().unwrap();
        }

        let i = world.get_resource::<i32>().unwrap();

        assert_eq!(*i, 42);
    }

    #[test]
    fn merge_entities_test() {
        let mut world = World::new(16);

        let a = world.insert_entity();
        let b = world.insert_entity();
        // control
        let c = world.insert_entity();

        world.set_component(a, 1u64).unwrap();
        world.set_component(a, 1u32).unwrap();
        world.set_component(b, 2i32).unwrap();
        world.set_component(b, 2i64).unwrap();

        world.set_component(c, 3u64).unwrap();
        world.set_component(c, 3u32).unwrap();
        world.set_component(c, 3i64).unwrap();
        world.set_component(c, 3i32).unwrap();

        {
            let mut cmd = world.ensure_commands();
            cmd.merge_entities(a, b);

            drop(cmd);
            world.apply_commands().unwrap();
        }

        assert!(!world.is_id_valid(a), "Entity a should have been deleted");

        // test if c entity is intact
        let comp = world.get_component::<u64>(c).unwrap();
        assert_eq!(comp, &3);
        let comp = world.get_component::<u32>(c).unwrap();
        assert_eq!(comp, &3);
        let comp = world.get_component::<i64>(c).unwrap();
        assert_eq!(comp, &3);
        let comp = world.get_component::<i32>(c).unwrap();
        assert_eq!(comp, &3);

        let c = world.get_component::<u64>(b).unwrap();
        assert_eq!(c, &1);
        let c = world.get_component::<u32>(b).unwrap();
        assert_eq!(c, &1);
        let c = world.get_component::<i64>(b).unwrap();
        assert_eq!(c, &2);
        let c = world.get_component::<i32>(b).unwrap();
        assert_eq!(c, &2);
    }

    #[test]
    #[should_panic]
    fn using_multiple_commands_is_a_panic_test() {
        // TODO: would be nice if the ECS could support this use-case
        fn sys(_a: Commands, _b: Commands) {}

        let mut w = World::new(1);
        w.run_system(sys).unwrap_or_default();
    }

    /// regression test. this used to panic
    #[test]
    fn resource_commands_with_no_entiy() {
        let mut w = World::new(1);

        w.run_system(|mut cmd: Commands| {
            cmd.insert_resource(4i32);
            cmd.insert_resource(1i64);
        })
        .unwrap();
    }
}

#![feature(slice_range)]

use std::{
    any::TypeId, cell::UnsafeCell, collections::BTreeMap, mem::transmute, pin::Pin, ptr::NonNull,
};

use cfg_if::cfg_if;
use commands::CommandPayload;
use entity_id::EntityId;
use entity_index::EntityIndex;
use prelude::Bundle;
use resources::ResourceStorage;
use systems::SystemStage;
use table::EntityTable;

pub mod bundle;
pub mod commands;
pub mod entity_id;
pub mod entity_index;
pub mod prelude;
pub mod query;
pub mod query_set;
pub mod resources;
pub mod systems;
pub mod world_access;

#[cfg(feature = "serde")]
pub mod serde;

mod table;

#[cfg(feature = "parallel")]
pub mod job_system;
#[cfg(feature = "parallel")]
mod scheduler;

use world_access::WorldLock;

use crate::{commands::sort_commands, systems::ShouldRunFlags};

#[cfg(test)]
mod world_tests;

type UnsafeBuffer<T> = std::cell::UnsafeCell<Vec<T>>;

pub struct World {
    pub(crate) this_lock: WorldLock,
    pub(crate) entity_ids: UnsafeCell<EntityIndex>,
    pub(crate) archetypes: BTreeMap<TypeHash, Pin<Box<EntityTable>>>,
    // empty archetypes reserved for buffers
    // enables us to reuse temporary archetypes without affecting query performance
    archetypes_staging: BTreeMap<TypeHash, Pin<Box<EntityTable>>>,
    pub(crate) resources: ResourceStorage,
    pub(crate) commands: Vec<UnsafeBuffer<CommandPayload>>,
    pub(crate) commands_buffer: Vec<CommandPayload>,
    pub(crate) system_stages: Vec<SystemStage<'static>>,

    /// 1 schedule for each system_stage
    #[cfg(feature = "parallel")]
    pub(crate) schedule: Vec<scheduler::Schedule>,

    #[cfg(feature = "parallel")]
    pub job_system: job_system::JobPool,
}

unsafe impl Send for World {}
unsafe impl Sync for World {}

#[cfg(feature = "clone")]
impl Clone for World {
    fn clone(&self) -> Self {
        // we don't actually mutate archetypes here, we just need mutable references to cast to
        // pointers
        let mut archetypes = self.archetypes.clone();
        let commands = Vec::default();
        let commands_buffer = Vec::default();

        let mut entity_ids = self.entity_ids().clone();
        for id in prelude::Query::<EntityId>::new(self).iter() {
            let (ptr, row_index) = self.entity_ids().read(id).unwrap();
            let ty = unsafe { ptr.as_ref() }.ty();
            let new_arch = archetypes.get_mut(&ty).unwrap();
            unsafe {
                entity_ids.update(id, new_arch.as_mut().get_mut() as *mut _, row_index);
            }
        }

        let resources = self.resources.clone();

        let systems = self.system_stages.clone();

        #[cfg(feature = "parallel")]
        let schedule = self.schedule.clone();
        #[cfg(feature = "parallel")]
        let job_system = self.job_system.clone();

        Self {
            this_lock: WorldLock::new(),
            entity_ids: UnsafeCell::new(entity_ids),
            archetypes,
            archetypes_staging: Default::default(),
            commands,
            commands_buffer,
            resources,
            system_stages: systems,
            #[cfg(feature = "parallel")]
            schedule,
            #[cfg(feature = "parallel")]
            job_system,
        }
    }
}

type TypeHash = u128;

const fn hash_ty<T: 'static>() -> TypeHash {
    let ty = TypeId::of::<T>();
    hash_type_id(ty)
}

const fn hash_type_id(ty: TypeId) -> TypeHash {
    let hash: TypeHash = unsafe { transmute(ty) };
    if hash == unsafe { transmute::<_, TypeHash>(TypeId::of::<()>()) } {
        // ensure that unit type has hash=0
        0
    } else {
        hash
    }
}

pub const VOID_TY: TypeHash = 0;

#[derive(Clone, Debug, thiserror::Error)]
pub enum WorldError {
    #[error("World is full and can not take more entities")]
    OutOfCapacity,
    #[error("Entity was not found")]
    EntityNotFound,
    #[error("Entity doesn't have specified component")]
    ComponentNotFound,
    #[error("Inserted id ({0}) is invalid and can not be inserted")]
    InsertInvalidId(EntityId),
}

pub type WorldResult<T> = Result<T, WorldError>;
pub type RowIndex = u32;

#[cfg(feature = "parallel")]
pub trait ParallelComponent: Send + Sync {}
#[cfg(not(feature = "parallel"))]
pub trait ParallelComponent {}

#[cfg(feature = "parallel")]
impl<T: Send + Sync> ParallelComponent for T {}
#[cfg(not(feature = "parallel"))]
impl<T> ParallelComponent for T {}

/// The end goal is to have a clonable ECS, that's why we have the Clone restriction.
#[cfg(feature = "clone")]
pub trait Component: 'static + Clone + ParallelComponent {}
#[cfg(feature = "clone")]
impl<T: 'static + Clone + ParallelComponent> Component for T {}

#[cfg(not(feature = "clone"))]
pub trait Component: 'static + ParallelComponent {}
#[cfg(not(feature = "clone"))]
impl<T: 'static + ParallelComponent> Component for T {}

impl World {
    pub fn new(initial_capacity: u32) -> Self {
        let entity_ids = EntityIndex::new(initial_capacity);

        #[cfg(feature = "parallel")]
        let job_system: job_system::JobPool = job_system::JOB_POOL.clone();
        let mut result = Self {
            this_lock: WorldLock::new(),
            entity_ids: UnsafeCell::new(entity_ids),
            archetypes: BTreeMap::new(),
            archetypes_staging: Default::default(),
            resources: ResourceStorage::new(),
            commands: Vec::default(),
            commands_buffer: Vec::default(),
            system_stages: Default::default(),
            #[cfg(feature = "parallel")]
            schedule: Default::default(),
            #[cfg(feature = "parallel")]
            job_system: job_system.clone(),
        };
        let void_store = Box::pin(EntityTable::empty());
        result.archetypes.insert(VOID_TY, void_store);
        #[cfg(feature = "parallel")]
        result.insert_resource(job_system);
        result
    }

    pub fn reserve_entities(&mut self, additional: u32) {
        self.entity_ids.get_mut().reserve(additional);
    }

    pub fn entity_capacity(&self) -> usize {
        self.entity_ids().capacity()
    }

    /// Writes entity ids and their archetype hash
    pub fn write_entities(&self, mut w: impl std::io::Write) -> std::io::Result<()> {
        for id in prelude::Query::<EntityId>::new(self).iter() {
            let (arch, _row_index) = self.entity_ids().read(id).unwrap();
            let ty = unsafe { arch.as_ref().ty() };
            write!(w, "{id}: {ty}, ")?;
        }
        Ok(())
    }

    pub fn num_entities(&self) -> usize {
        self.entity_ids().len()
    }

    pub fn is_id_valid(&self, id: EntityId) -> bool {
        self.entity_ids().is_valid(id)
    }

    pub fn apply_commands(&mut self) -> WorldResult<()> {
        #[cfg(feature = "tracing")]
        tracing::trace!("Running commands");

        self.commands_buffer.clear();
        let mut commands = std::mem::take(&mut self.commands_buffer);
        commands.extend(
            self.commands
                .iter_mut()
                .flat_map(|cmd| cmd.get_mut().drain(..)),
        );
        sort_commands(&mut commands);
        for cmd in commands.drain(..) {
            cmd.apply(self)?;
        }
        self.commands_buffer = commands;

        // move archetypes based on content
        // empty archetypes go to staging
        // non-empty staging archetypes go to the archetypes proper
        // except for the empty archetype which is treated special
        //
        let promotion_queue = self
            .archetypes_staging
            .iter()
            .filter_map(|(k, v)| (!v.is_empty()).then_some(*k))
            .collect::<Vec<_>>();

        for id in promotion_queue {
            let t = self.archetypes_staging.remove(&id).unwrap();
            self.archetypes.insert(id, t);
        }

        let demotion_queue = self
            .archetypes
            .iter()
            .filter_map(|(k, v)| (k != &VOID_TY && v.is_empty()).then_some(*k))
            .collect::<Vec<_>>();

        for id in demotion_queue {
            let t = self.archetypes.remove(&id).unwrap();
            self.archetypes_staging.insert(id, t);
        }

        #[cfg(feature = "tracing")]
        tracing::trace!("Running commands done");
        Ok(())
    }

    pub fn insert_entity(&mut self) -> EntityId {
        let id = self.entity_ids.get_mut().allocate_with_resize();
        unsafe {
            self.init_id(id);
        }
        #[cfg(feature = "tracing")]
        tracing::trace!(id = tracing::field::display(id), "Inserted entity");
        id
    }

    /// # Safety
    /// Id must be an allocated but uninitialized entity
    pub(crate) unsafe fn init_id(&mut self, id: EntityId) {
        let void_store = self.archetypes.get_mut(&VOID_TY).unwrap();

        let index = void_store.as_mut().insert_entity(id);
        void_store.as_mut().set_component(index, ());
        unsafe {
            self.entity_ids
                .get_mut()
                .update(id, void_store.as_mut().get_mut() as *mut _, index)
        };
    }

    pub fn delete_entity(&mut self, id: EntityId) -> WorldResult<()> {
        #[cfg(feature = "tracing")]
        tracing::trace!(id = tracing::field::display(id), "Delete entity");
        if !self.entity_ids().is_valid(id) {
            return Err(WorldError::EntityNotFound);
        }

        let (mut archetype, index) = self
            .entity_ids()
            .read(id)
            .map_err(|_| WorldError::EntityNotFound)?;
        unsafe {
            if let Some(updated_entity) = archetype.as_mut().remove(index) {
                #[cfg(feature = "tracing")]
                tracing::trace!(?updated_entity, index, "Update moved entity index");
                debug_assert_ne!(id, updated_entity);
                self.entity_ids
                    .get_mut()
                    .update_row_index(updated_entity, index);
            }
            self.entity_ids.get_mut().free(id);
        }
        Ok(())
    }

    fn get_archetype_by_key_mut(&mut self, key: &TypeHash) -> Option<NonNull<EntityTable>> {
        self.archetypes
            .get_mut(key)
            .or_else(|| self.archetypes_staging.get_mut(key))
            .map(|t| t.as_mut().get_mut().into())
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self, bundle)))]
    fn set_bundle<T: Bundle>(&mut self, entity_id: EntityId, bundle: T) -> WorldResult<()> {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            entity_id = tracing::field::display(entity_id),
            ty = std::any::type_name::<T>(),
            "Set bundle"
        );

        let (mut archetype, mut index) = self
            .entity_ids()
            .read(entity_id)
            .map_err(|_| WorldError::EntityNotFound)?;
        let mut archetype = unsafe { archetype.as_mut() };

        #[cfg(feature = "tracing")]
        tracing::trace!(index, "Read entity");

        if !bundle.can_insert(archetype) {
            #[cfg(feature = "tracing")]
            tracing::trace!(?archetype.ty,"Bundle can not be inserted into the current archetype");

            let new_hash = T::compute_hash_from_table(archetype);
            match self.get_archetype_by_key_mut(&new_hash) {
                Some(mut new_arch) => {
                    #[cfg(feature = "tracing")]
                    tracing::trace!(new_hash, "Moving entity into existing archetype");

                    unsafe {
                        let (i, updated_entity) = archetype.move_entity(new_arch.as_mut(), index);
                        if let Some(updated_entity) = updated_entity {
                            #[cfg(feature = "tracing")]
                            tracing::trace!(?updated_entity, index, "Update moved entity index");
                            self.entity_ids
                                .get_mut()
                                .update_row_index(updated_entity, index);
                        }
                        index = i;
                        archetype = new_arch.as_mut();
                    }
                }
                None => {
                    #[cfg(feature = "tracing")]
                    tracing::trace!(new_hash, "Inserting new archetype");

                    let new_arch = T::extend(archetype);
                    debug_assert_eq!(new_hash, new_arch.ty());
                    let mut res = self.insert_archetype(archetype, index, new_arch);

                    archetype = unsafe { res.as_mut() };
                    index = 0;
                }
            }
        }
        bundle.insert_into(archetype, index)?;
        unsafe {
            #[cfg(feature = "tracing")]
            tracing::trace!(index, ?entity_id, "Update entity index");
            self.entity_ids
                .get_mut()
                .update(entity_id, archetype, index);
        }
        Ok(())
    }

    fn set_component<T: Component>(
        &mut self,
        entity_id: EntityId,
        component: T,
    ) -> WorldResult<()> {
        self.set_bundle(entity_id, (component,))
    }

    pub fn get_component<T: Component>(&self, entity_id: EntityId) -> Option<&T> {
        let (arch, idx) = self.entity_ids().read(entity_id).ok()?;
        unsafe { arch.as_ref().get_component(idx) }
    }

    #[allow(clippy::mut_from_ref)]
    pub fn get_component_mut<T: Component>(&self, entity_id: EntityId) -> Option<&mut T> {
        let (mut arch, idx) = self.entity_ids().read(entity_id).ok()?;
        unsafe { arch.as_mut().get_component_mut(idx) }
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip(self)))]
    fn remove_component<T: Component>(&mut self, entity_id: EntityId) -> WorldResult<()> {
        #[cfg(feature = "tracing")]
        tracing::trace!(
            entity_id = tracing::field::display(entity_id),
            ty = std::any::type_name::<T>(),
            "Remove component"
        );

        let (mut archetype, mut index) = self
            .entity_ids()
            .read(entity_id)
            .map_err(|_| WorldError::EntityNotFound)?;
        let archetype = unsafe { archetype.as_mut() };
        let arch_ptr;
        if !archetype.contains_column::<T>() {
            return Err(WorldError::ComponentNotFound);
        }
        let new_ty = archetype.extended_hash::<T>();
        if self.archetypes.contains_key(&new_ty) {
            let new_arch = self.archetypes.get_mut(&new_ty).unwrap();
            let (i, updated_entity) = archetype.move_entity(new_arch, index);
            if let Some(updated_entity) = updated_entity {
                #[cfg(feature = "tracing")]
                tracing::trace!(?updated_entity, index, "Update moved entity index");
                debug_assert_ne!(updated_entity, entity_id);
                unsafe {
                    self.entity_ids
                        .get_mut()
                        .update_row_index(updated_entity, index);
                }
            }
            index = i;
            arch_ptr = new_arch.as_mut().get_mut() as *mut _;
        } else {
            let arch = archetype.clone_empty();
            let mut res = self.insert_archetype(archetype, index, arch.reduce_with_column::<T>());

            arch_ptr = unsafe { res.as_mut() as *mut _ };
            index = 0;
        }
        unsafe {
            self.entity_ids.get_mut().update(entity_id, arch_ptr, index);
        }
        Ok(())
    }

    #[inline(never)]
    #[must_use]
    fn insert_archetype(
        &mut self,
        archetype: &mut EntityTable,
        row_index: RowIndex,
        new_arch: EntityTable,
    ) -> NonNull<EntityTable> {
        let mut new_arch = Box::pin(new_arch);
        let (index, moved_entity) = archetype.move_entity(&mut new_arch, row_index);
        debug_assert_eq!(index, 0);
        let res = unsafe { NonNull::new_unchecked(new_arch.as_mut().get_mut() as *mut _) };
        debug_assert!(
            !self.archetypes.contains_key(&new_arch.ty()),
            "Musn't insert the same archetype twice"
        );
        self.archetypes.insert(new_arch.ty(), new_arch);

        if let Some(updated_entity) = moved_entity {
            #[cfg(feature = "tracing")]
            tracing::trace!(?updated_entity, index, "Update moved entity index");
            unsafe {
                self.entity_ids
                    .get_mut()
                    .update_row_index(updated_entity, row_index);
            }
        }
        res
    }

    pub fn insert_resource<T: Component>(&mut self, value: T) {
        self.resources.insert(value);
    }

    pub fn remove_resource<T: 'static>(&mut self) -> Option<Box<T>> {
        self.resources.remove::<T>()
    }

    pub fn get_resource<T: 'static>(&self) -> Option<&T> {
        self.resources.fetch::<T>()
    }

    pub fn get_resource_mut<T: 'static>(&mut self) -> Option<&mut T> {
        unsafe { self.resources.fetch_mut::<T>() }
    }

    pub fn get_resource_or_default<T: Default + Component>(&mut self) -> &mut T {
        self.resources.fetch_or_default()
    }

    pub fn get_or_insert_resource<T: Component>(&mut self, init: impl FnOnce() -> T) -> &mut T {
        if !self.resources.contains::<T>() {
            self.resources.insert(init());
        }
        unsafe { self.resources.fetch_mut().unwrap() }
    }

    /// System stages are executed in the order they were added to the World
    pub fn add_stage<'a>(&mut self, stage: impl Into<SystemStage<'a>>) {
        fn _add_stage(this: &mut World, stage: SystemStage<'_>) {
            // # SAFETY
            // lifetimes are managed by the World instance from now
            let stage: SystemStage = unsafe { transmute(stage) };
            cfg_if!(
                if #[cfg(feature = "parallel")] {
                    this.schedule
                        .push(scheduler::Schedule::from_stage(&stage));
                }
            );

            this.system_stages.push(stage);
        }
        _add_stage(self, stage.into())
    }

    /// Run a single stage withouth adding it to the World
    ///
    /// Return the command result and the stage that was ran, so the stage can be reused
    pub fn run_stage<'a>(&mut self, stage: impl Into<SystemStage<'a>>) -> RunStageReturn<'a> {
        fn _run_stage<'a>(this: &mut World, stage: SystemStage<'a>) -> RunStageReturn<'a> {
            #[cfg(feature = "tracing")]
            tracing::trace!(stage_name = stage.name.as_str(), "Update stage");

            // # SAFETY
            // lifetimes are managed by the World instance from now
            let stage = unsafe { transmute::<SystemStage, SystemStage>(stage) };

            // move stage into the world
            cfg_if!(
                if #[cfg(feature = "parallel")] {
                    this.schedule
                        .push(scheduler::Schedule::from_stage(&stage));
                }
            );

            let i = this.system_stages.len();
            this.system_stages.push(stage);
            this.execute_stage(i);

            // pop the stage after execution, one-shot stages are not stored
            // systems with WorldAccess may add further systems to the World, so use remove instead of
            // pop()
            let stage = this.system_stages.remove(i);

            // # SAFETY
            // revert the lifetime change, give back control to the caller
            let stage = unsafe { transmute::<SystemStage, SystemStage>(stage) };

            #[cfg(feature = "parallel")]
            this.schedule.remove(i);

            let res = this.apply_commands();
            RunStageReturn { stage, result: res }
        }
        _run_stage(self, stage.into())
    }

    /// Run a system and apply any commands before returning
    ///
    /// Returns the commands result
    pub fn run_system<'a, S, P, R>(&mut self, system: S) -> WorldResult<R>
    where
        S: systems::IntoOnceSystem<'a, P, R>,
    {
        self.resize_commands(1);

        // assert invariants
        #[cfg(debug_assertions)]
        let _desc = S::descriptor();

        let func = system.into_once_system();
        let result = (func)(self, 0);
        // apply commands immediately
        self.apply_commands().map(move |_| result)
    }

    /// Run a system that only gets a read-only view of the world.
    /// All mutable access is forbidden, including Commands
    ///
    /// Panics if the system does not conform to the requirements
    pub fn run_view_system<'a, S, P, R>(&self, system: S) -> R
    where
        S: systems::IntoOnceSystem<'a, P, R>,
    {
        let desc = S::descriptor();
        assert!((desc.read_only)());
        let sys = system.into_once_system();
        (sys)(self, 0)
    }

    pub fn tick(&mut self) {
        #[cfg(feature = "parallel")]
        debug_assert_eq!(self.system_stages.len(), self.schedule.len());
        for i in 0..self.system_stages.len() {
            self.execute_stage(i);
            // apply commands after each stage
            self.apply_commands().unwrap();
        }
    }

    fn execute_stage(&mut self, i: usize) {
        self.resize_commands(self.system_stages[i].systems.len());
        let stage = &self.system_stages[i];

        #[cfg(feature = "tracing")]
        let stage_name = stage.name.clone();
        #[cfg(feature = "tracing")]
        tracing::trace!(stage_name = stage_name.as_str(), "Run stage");

        let mut should_run_flags: ShouldRunFlags = !0;
        for (i, condition) in stage.should_run.iter().enumerate() {
            let should_run = unsafe { run_system(self, condition) };
            if !should_run {
                should_run_flags ^= 1 << i;
                #[cfg(feature = "tracing")]
                tracing::trace!(
                    stage_name = stage.name.to_string(),
                    "Stage should_run was false"
                );
            }
        }

        let systems = &stage.systems;

        cfg_if!(
            if #[cfg(feature = "parallel")] {
                let schedule = &self.schedule[i];
                let graph = schedule.jobs(systems, should_run_flags, self);

                #[cfg(feature = "tracing")]
                tracing::debug!(graph = tracing::field::debug(&graph), "Running job graph");

                self.job_system.run_graph(&graph);
            } else {
                for system in systems.iter().filter(|sys| {
                    sys.should_run_mask & should_run_flags == sys.should_run_mask
                }) {
                    unsafe {
                        run_system(self, system);
                    }
                }
            }
        );
        #[cfg(feature = "tracing")]
        tracing::trace!(stage_name = stage_name.as_str(), "Run stage done");
    }

    fn resize_commands(&mut self, len: usize) {
        // do not shrink
        if self.commands.len() < len {
            self.commands
                .resize_with(len, std::cell::UnsafeCell::default);
        }
    }

    /// Constructs a new [crate::commands::Commands] instance with initialized buffers in this world
    pub fn ensure_commands(&mut self) -> prelude::Commands<'_> {
        self.resize_commands(1);
        commands::Commands::new(self, 0)
    }

    /// Return the "type" of the Entity.
    /// Type itself is an opaque hash.
    ///
    /// This function is meant to be used to test successful saving/loading of entities
    pub fn get_entity_ty(&self, id: EntityId) -> Option<TypeHash> {
        let (arch, _) = self.entity_ids().read(id).ok()?;
        Some(unsafe { arch.as_ref().ty() })
    }

    pub fn get_entity_component_types(&self, id: EntityId) -> Option<Vec<TypeId>> {
        let (arch, _) = self.entity_ids().read(id).ok()?;
        Some(unsafe {
            arch.as_ref()
                .components
                .keys()
                .filter(|k| k != &&TypeId::of::<()>())
                .copied()
                .collect()
        })
    }

    pub fn get_entity_component_names(&self, id: EntityId) -> Option<Vec<&'static str>> {
        let (arch, _) = self.entity_ids().read(id).ok()?;
        Some(unsafe {
            arch.as_ref()
                .components
                .iter()
                .filter_map(|(k, v)| (k != &TypeId::of::<()>()).then_some(v))
                .map(|t| (&*t.get()).ty_name)
                .collect()
        })
    }

    /// Compute a checksum of the World
    ///
    /// Note that only entities and their archetypes are considered, the contents of the components
    /// themselves are ignored
    pub fn checksum(&self) -> u64 {
        use std::hash::Hasher;

        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        prelude::Query::<EntityId>::new(self).iter().for_each(|id| {
            let (arch, row_index) = self.entity_ids().read(id).unwrap();
            let ty = unsafe { arch.as_ref().ty() };
            hasher.write_u32(id.into());
            hasher.write_u128(ty);
            hasher.write_u32(row_index);
        });

        hasher.finish()
    }

    pub(crate) fn entity_ids(&self) -> &EntityIndex {
        unsafe { &*self.entity_ids.get() }
    }

    /// Move all components from `lhs` to `rhs`, overrinding components `rhs` already has. Then
    /// delete the `lhs` entity
    pub fn merge_entities(&mut self, lhs: EntityId, rhs: EntityId) -> WorldResult<()> {
        if lhs == rhs {
            return Ok(());
        }
        let (mut lhs_archetype, lhs_index) = self
            .entity_ids()
            .read(lhs)
            .map_err(|_| WorldError::EntityNotFound)?;
        let lhs_archetype = unsafe { lhs_archetype.as_mut() };

        let (mut rhs_archetype, rhs_index) = self
            .entity_ids()
            .read(rhs)
            .map_err(|_| WorldError::EntityNotFound)?;
        let rhs_archetype = unsafe { rhs_archetype.as_mut() };

        if lhs_archetype.ty() == rhs_archetype.ty() {
            // if they're the same archetype just swap and remove
            rhs_archetype.swap_components(lhs_index, rhs_index);
            unsafe {
                let entity_ids = self.entity_ids.get_mut();
                entity_ids.update_row_index(lhs, rhs_index);
                entity_ids.update_row_index(rhs, lhs_index);
            }
            return self.delete_entity(lhs);
        }

        // figure out the new archetype hash
        let mut dst_hash = rhs_archetype.ty();
        if dst_hash != lhs_archetype.ty() {
            for col in lhs_archetype.components.keys() {
                if !rhs_archetype.components.contains_key(col) {
                    dst_hash ^= hash_type_id(*col);
                }
            }
        }
        if dst_hash == rhs_archetype.ty() {
            // rhs is already in the correct archetype
            // this means that lhs is a subset of rhs
            let moved = lhs_archetype.move_entity_into(lhs_index, rhs_archetype, rhs_index);
            if let Some(moved) = moved {
                unsafe {
                    self.entity_ids.get_mut().update_row_index(moved, lhs_index);
                }
            }
            self.entity_ids.get_mut().free(lhs);
            return Ok(());
        }
        if dst_hash == lhs_archetype.ty() {
            // rhs is a subset of lhs
            // delete rhs components, update the lhs id to rhs and delete the lhs id
            //
            let moved = rhs_archetype.remove(rhs_index);
            if let Some(moved) = moved {
                unsafe {
                    self.entity_ids.get_mut().update_row_index(moved, rhs_index);
                }
            }
            // update the entity id in lhs
            lhs_archetype.entities[lhs_index as usize] = rhs;
            // entity id update
            let entity_ids = self.entity_ids.get_mut();
            unsafe {
                entity_ids.update(rhs, lhs_archetype, lhs_index);
            }
            entity_ids.free(lhs);
            return Ok(());
        }

        // dst_arch is disjoint from both lhs and rhs
        //
        let dst_arch = self
            .archetypes
            .entry(dst_hash)
            .or_insert_with(|| Box::pin(rhs_archetype.merged(lhs_archetype)));
        // move rhs components to the dst
        //
        let (dst_index, moved) = rhs_archetype.move_entity(dst_arch, rhs_index);
        if let Some(id) = moved {
            unsafe {
                self.entity_ids.get_mut().update_row_index(id, rhs_index);
            }
        }
        // for lhs columns, if both rhs and lhs have them, then update the dst value
        // else move
        for (ty, col) in lhs_archetype.components.iter_mut() {
            let dst_col = dst_arch
                .components
                .get_mut(ty)
                .expect("dst should have all lhs components")
                .get_mut();
            if rhs_archetype.contains_column_ty(*ty) {
                (col.get_mut().move_row_into)(col.get_mut(), lhs_index, dst_col, dst_index);
            } else {
                (col.get_mut().move_row)(col.get_mut(), dst_col, lhs_index);
            }
        }
        // all lhs components have been moved
        // swap_remove the id
        //
        lhs_archetype.entities.swap_remove(lhs_index as usize);
        lhs_archetype.rows -= 1;
        if lhs_archetype.rows > 0 && lhs_index < lhs_archetype.rows {
            unsafe {
                self.entity_ids
                    .get_mut()
                    .update_row_index(lhs_archetype.entities[lhs_index as usize], lhs_index);
            }
        }

        // entity bookkeeping
        unsafe {
            let entity_ids = self.entity_ids.get_mut();
            entity_ids.update(rhs, Pin::as_mut(dst_arch).get_mut() as *mut _, dst_index);
            entity_ids.free(lhs);
        }
        Ok(())
    }

    /// Insert a pre-allocated entity id into the world.
    ///
    /// Insert is an O(n) operation that walks a linked-list.
    /// Prefer [[insert_entity]]
    pub fn insert_id(&mut self, id: EntityId) -> Result<(), entity_index::InsertError> {
        self.entity_ids.get_mut().insert_id(id)?;
        unsafe {
            self.init_id(id);
        }
        #[cfg(feature = "tracing")]
        tracing::trace!(
            id = tracing::field::display(id),
            "Inserted existing entity id"
        );
        Ok(())
    }

    pub fn archetypes(&self) -> &BTreeMap<TypeHash, Pin<Box<EntityTable>>> {
        &self.archetypes
    }

    pub fn entity_table(&self, id: EntityId) -> Option<&EntityTable> {
        let (arch, _idx) = self.entity_ids().read(id).ok()?;
        unsafe { Some(arch.as_ref()) }
    }

    /// Remove empty archetypes
    pub fn vacuum(&mut self) {
        self.archetypes_staging.clear();
    }
}

// # SAFETY
// this World instance must be borrowed as mutable by the caller, so no other thread should have
// access to the internals
//
// The system's queries must be disjoint to any other concurrently running system's
unsafe fn run_system<'a, R>(world: &'a World, sys: &'a systems::ErasedSystem<'_, R>) -> R {
    let index = sys.system_idx;
    let execute: &systems::InnerSystem<'_, R> = { unsafe { transmute(sys.execute.as_ref()) } };

    (execute)(world, index)
}

pub struct RunStageReturn<'a> {
    pub stage: SystemStage<'a>,
    pub result: WorldResult<()>,
}

impl<'a> RunStageReturn<'a> {
    pub fn unwrap(self) -> SystemStage<'a> {
        self.result.unwrap();
        self.stage
    }

    pub fn expect(self, msg: &str) -> SystemStage<'a> {
        self.result.expect(msg);
        self.stage
    }
}

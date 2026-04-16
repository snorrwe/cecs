// FIXME: yeet the bloody Box<dyn Iterator> iterators please
//
pub mod deleted_query;
pub mod filters;
pub mod resource_query;

#[cfg(test)]
mod query_tests;

use crate::{
    Component, RowIndex, World,
    entity_id::EntityId,
    systems::SystemDescriptor,
    table::{ArchetypeHash, EntityTable},
};
use filters::Filter;
use std::{any::TypeId, collections::HashSet, marker::PhantomData, ops::RangeBounds, slice};

/// # SAFETY
///
/// Implementors must ensure that the appropriate methods are implemented!
pub unsafe trait WorldQuery<'a> {
    fn new(db: &'a World, system_idx: usize) -> Self;

    /// List of component types this query needs exclusive access to
    fn components_mut(_set: &mut HashSet<TypeId>) {}
    /// List of component types this query needs
    fn components_const(_set: &mut HashSet<TypeId>) {}
    /// List of resource types this query needs exclusive access to
    fn resources_mut(_set: &mut HashSet<TypeId>) {}
    /// List of resource types this query needs
    fn resources_const(_set: &mut HashSet<TypeId>) {}
    /// Return wether this system should run in isolation
    fn exclusive() -> bool {
        false
    }

    fn read_only() -> bool {
        false
    }
}

unsafe impl<'a> WorldQuery<'a> for () {
    fn new(_db: &'a World, _system_idx: usize) -> Self {}
    fn read_only() -> bool {
        true
    }
}

#[derive(Default)]
pub struct QueryProperties {
    pub exclusive: bool,
    pub comp_mut: HashSet<TypeId>,
    pub comp_const: HashSet<TypeId>,
    pub res_mut: HashSet<TypeId>,
    pub res_const: HashSet<TypeId>,
}

impl QueryProperties {
    pub fn is_disjoint(&self, other: &QueryProperties) -> bool {
        !self.exclusive
            && !other.exclusive
            && self.comp_mut.is_disjoint(&other.comp_const)
            && self.res_mut.is_disjoint(&other.res_const)
            && self.comp_mut.is_disjoint(&other.comp_mut)
            && self.res_mut.is_disjoint(&other.res_mut)
            && self.comp_const.is_disjoint(&other.comp_mut)
            && self.res_const.is_disjoint(&other.res_mut)
    }

    pub fn extend(&mut self, props: QueryProperties) {
        self.exclusive = self.exclusive || props.exclusive;
        self.comp_mut.extend(props.comp_mut.into_iter());
        self.res_mut.extend(props.res_mut.into_iter());
        self.comp_const.extend(props.comp_const.into_iter());
        self.res_const.extend(props.res_const.into_iter());
    }

    pub fn is_empty(&self) -> bool {
        !self.exclusive
            && self.comp_mut.is_empty()
            && self.res_mut.is_empty()
            && self.res_const.is_empty()
            && self.comp_const.is_empty()
    }

    pub fn from_system<T>(desc: &SystemDescriptor<T>) -> Self {
        Self {
            exclusive: (desc.exclusive)(),
            comp_mut: (desc.components_mut)(),
            res_mut: (desc.resources_mut)(),
            comp_const: (desc.components_const)(),
            res_const: (desc.resources_const)(),
        }
    }
}

/// Test if this query is valid and return its properties
#[inline]
#[allow(unused)]
pub(crate) fn ensure_query_valid<'a, T: WorldQuery<'a>>() -> QueryProperties {
    let mut comp_mut = HashSet::new();
    let mut comp_const = HashSet::new();

    T::components_mut(&mut comp_mut);
    T::components_const(&mut comp_const);

    assert!(
        comp_mut.is_disjoint(&comp_const),
        "A query may not borrow the same type as both mutable and immutable,
{}",
        std::any::type_name::<T>()
    );

    // resources do not need asserts here
    let mut res_mut = HashSet::new();
    let mut res_const = HashSet::new();
    T::resources_mut(&mut res_mut);
    T::resources_const(&mut res_const);
    QueryProperties {
        comp_mut,
        comp_const,
        res_mut,
        res_const,
        exclusive: T::exclusive(),
    }
}

pub struct Query<'a, T, F = ()> {
    world: std::ptr::NonNull<crate::World>,
    _m: PhantomData<(T, F)>,
    _l: PhantomData<&'a ()>,
}

unsafe impl<T, F> Send for Query<'_, T, F> {}
unsafe impl<T, F> Sync for Query<'_, T, F> {}

unsafe impl<'a, T, F> WorldQuery<'a> for Query<'a, T, F>
where
    T: QueryFragment,
    F: Filter,
{
    fn new(db: &'a World, _system_idx: usize) -> Self {
        Self::new(db)
    }

    fn components_mut(set: &mut HashSet<TypeId>) {
        <T as QueryFragment>::types_mut(set);
    }

    fn components_const(set: &mut HashSet<TypeId>) {
        <T as QueryFragment>::types_const(set);
    }

    fn read_only() -> bool {
        <T as QueryFragment>::read_only()
    }
}

impl<'a, T, F> Query<'a, T, F>
where
    T: QueryFragment,
    F: Filter,
{
    pub fn new(world: &'a crate::World) -> Self {
        Query {
            world: std::ptr::NonNull::from(world),
            _m: PhantomData,
            _l: PhantomData,
        }
    }

    /// Return a query that is a subset of this query
    ///
    /// Subset means that the child query may not have more components than the original query.
    ///
    /// Mutable references may be demoted to const references, but const references may not be
    /// promoted to mutable references.
    ///
    /// The query has to be uniquely borrowed, because the subquery _may_ mutably borrow the same data as the parent query
    ///
    /// # Panics
    ///
    /// Panics if an invariant no longer holds.
    ///
    /// TODO: Can we avoid the unique borrow of the parent Query?
    ///
    ///
    /// ```
    /// use cecs::prelude::*;
    /// # #[derive(Clone, Copy)]
    /// # struct A;
    /// # #[derive(Clone, Copy)]
    /// # struct B;
    /// # #[derive(Clone, Copy)]
    /// # struct C;
    ///
    /// let mut world = World::new(4);
    ///
    /// let e = world.insert_entity();
    /// world.run_system(move |mut cmd: Commands| {
    ///     cmd.entity(e).insert_bundle((A, B, C));
    /// });
    ///
    /// let mut q = Query::<(EntityId, &mut A, &mut B, &C)>::new(&world);
    ///
    /// let sub: Query<(&A, &C, EntityId)> = q.subset();
    ///
    /// let mut count = 0;
    /// for (_a, _c, id) in sub.iter() {
    ///     assert_eq!(id, e);
    ///     count += 1;
    /// }
    /// assert_eq!(count, 1);
    /// ```
    pub fn subset<'b, T1>(&'b mut self) -> Query<'b, T1, F>
    where
        T1: QueryFragment,
        'a: 'b,
    {
        #[cfg(debug_assertions)]
        {
            let p = ensure_query_valid::<Query<T1, F>>();

            let mut rhs = HashSet::new();
            Self::components_mut(&mut rhs);
            let lhs = p.comp_mut;

            assert!(lhs.is_subset(&rhs));

            // T1 const components must be a subset of rhs const+mut
            Self::components_const(&mut rhs);

            let lhs = p.comp_const;
            assert!(lhs.is_subset(&rhs));
        }
        unsafe { Query::<'b, T1, F>::new(self.world.as_ref()) }
    }

    /// Restrict this Query with an additional filter
    /// ```
    /// use cecs::prelude::*;
    /// # #[derive(Clone, Copy)]
    /// # struct A;
    /// # #[derive(Clone, Copy)]
    /// # struct B;
    ///
    /// let mut world = World::new(4);
    ///
    /// let e = world.insert_entity();
    /// world.run_system(move |mut cmd: Commands| {
    ///     cmd.entity(e).insert_bundle((A, B));
    /// });
    ///
    /// let mut q = Query::<(EntityId, &mut A)>::new(&world);
    ///
    /// let sub = q.with_filter::<WithOut<B>>();
    ///
    /// let mut count = 0;
    /// for _ in sub.iter() {
    ///     count += 1;
    /// }
    /// assert_eq!(count, 0);
    /// ```
    pub fn with_filter<'b, F1>(&'b mut self) -> Query<'b, T, (F, F1)>
    where
        F1: Filter,
        'a: 'b,
    {
        unsafe { Query::<'b, T, (F, F1)>::new(self.world.as_ref()) }
    }

    /// Count the number of entities this query spans
    pub fn count(&self) -> usize {
        unsafe {
            self.world
                .as_ref()
                .archetypes
                .iter()
                .filter(|(_, arch)| F::filter(arch) && T::contains(arch))
                .map(|(_, arch)| arch.len())
                .sum::<usize>()
        }
    }

    pub fn is_empty(&self) -> bool {
        unsafe {
            self.world
                .as_ref()
                .archetypes
                .iter()
                .filter(|(_, arch)| F::filter(arch) && T::contains(arch))
                .all(|(_, arch)| arch.is_empty())
        }
    }

    pub fn any(&self) -> bool {
        unsafe {
            self.world
                .as_ref()
                .archetypes
                .iter()
                .filter(|(_, arch)| F::filter(arch) && T::contains(arch))
                .any(|(_, arch)| !arch.is_empty())
        }
    }

    pub fn single<'b>(&'b self) -> Option<<T as QueryFragment>::Item<'a>>
    where
        'a: 'b,
    {
        self.iter().next()
    }

    pub fn single_mut<'b>(&'b mut self) -> Option<<T as QueryFragment>::ItemMut<'a>>
    where
        'a: 'b,
    {
        self.iter_mut().next()
    }

    pub fn iter<'b>(&'b self) -> impl Iterator<Item = <T as QueryFragment>::Item<'a>> + 'b {
        unsafe {
            self.world
                .as_ref()
                .archetypes
                .iter()
                .filter(|(_, arch)| F::filter(arch))
                .flat_map(|(_, arch)| T::iter(arch))
        }
    }

    pub fn iter_mut<'b>(
        &'b mut self,
    ) -> impl Iterator<Item = <T as QueryFragment>::ItemMut<'a>> + 'b {
        unsafe {
            self.world
                .as_ref()
                .archetypes
                .iter()
                .filter(|(_, arch)| F::filter(arch))
                .flat_map(|(_, arch)| T::iter_mut(arch))
        }
    }

    /// Unsafe functions let you pass the same query to sub-systems recursively splitting workload
    /// on multiple threads.
    ///
    /// The top-level system still needs &mut access to the components.
    pub unsafe fn iter_unsafe(
        &'a self,
    ) -> impl Iterator<Item = <T as QueryFragment>::ItemUnsafe<'a>> {
        unsafe {
            self.world
                .as_ref()
                .archetypes
                .iter()
                .filter(|(_, arch)| F::filter(arch))
                .flat_map(|(_, arch)| T::iter_unsafe(arch))
        }
    }

    pub fn fetch<'b>(&'b self, id: EntityId) -> Option<<T as QueryFragment>::Item<'b>>
    where
        'a: 'b,
    {
        unsafe {
            let (arch, index) = self.world.as_ref().entity_ids().read(id).ok()?;
            if !F::filter(arch.as_ref()) {
                return None;
            }

            T::fetch(arch.as_ref(), index)
        }
    }

    pub fn fetch_mut<'b>(&'b mut self, id: EntityId) -> Option<<T as QueryFragment>::ItemMut<'b>>
    where
        'a: 'b,
    {
        unsafe {
            let (arch, index) = self.world.as_ref().entity_ids().read(id).ok()?;
            if !F::filter(arch.as_ref()) {
                return None;
            }

            T::fetch_mut(arch.as_ref(), index)
        }
    }

    pub unsafe fn fetch_unsafe(
        &'a self,
        id: EntityId,
    ) -> Option<<T as QueryFragment>::ItemUnsafe<'a>> {
        unsafe {
            let (arch, index) = self.world.as_ref().entity_ids().read(id).ok()?;
            if !F::filter(arch.as_ref()) {
                return None;
            }

            T::fetch_unsafe(arch.as_ref(), index)
        }
    }

    pub fn contains(&self, id: EntityId) -> bool {
        unsafe {
            let (arch, _index) = match self.world.as_ref().entity_ids().read(id).ok() {
                None => return false,
                Some(x) => x,
            };
            if !F::filter(arch.as_ref()) {
                return false;
            }

            T::contains(arch.as_ref())
        }
    }

    /// fetch the first row of the query
    /// panic if no row was found
    pub fn one(&'a self) -> <T as QueryFragment>::Item<'a> {
        self.iter().next().unwrap()
    }

    /// fetch the first row of the query
    /// panic if no row was found
    pub fn one_mut<'b>(&'b mut self) -> <T as QueryFragment>::ItemMut<'a>
    where
        'a: 'b,
    {
        self.single_mut().unwrap()
    }

    #[cfg(feature = "parallel")]
    pub fn par_for_each<'b>(&'b self, f: impl Fn(<T as QueryFragment>::Item<'a>) + Sync + 'b)
    where
        T: Send + Sync,
    {
        unsafe {
            let world = self.world.as_ref();
            let pool = &world.job_system;
            pool.scope(|s| {
                let f = &f;
                world
                    .archetypes
                    .iter()
                    // TODO: should filters run inside the jobs instead?
                    // currently I anticipate that filters are inexpensive, so it seems cheaper to
                    // filter ahead of job creation
                    .filter(|(_, arch)| !arch.is_empty() && F::filter(arch))
                    .for_each(|(_, arch)| {
                        let batch_size = arch.len() / pool.parallelism() + 1;
                        // TODO: the job allocator could probably help greatly with these jobs
                        for range in batches(arch.len(), batch_size) {
                            s.spawn(move |_s| {
                                for t in T::iter_range(arch, range) {
                                    f(t);
                                }
                            })
                        }
                    });
            });
        }
    }

    #[cfg(feature = "parallel")]
    pub fn par_for_each_mut<'b>(
        &'b mut self,
        f: impl Fn(<T as QueryFragment>::ItemMut<'a>) + Sync + 'b,
    ) where
        T: Send + Sync,
    {
        unsafe {
            let world = self.world.as_ref();
            let pool = &world.job_system;
            pool.scope(|s| {
                let f = &f;
                world
                    .archetypes
                    .iter()
                    // TODO: should filters run inside the jobs instead?
                    // currently I anticipate that filters are inexpensive, so it seems cheaper to
                    // filter ahead of job creation
                    .filter(|(_, arch)| !arch.is_empty() && F::filter(arch))
                    .for_each(|(_, arch)| {
                        // TODO: should take size of queried types into account?
                        let batch_size = arch.len() / pool.parallelism() + 1;

                        // TODO: the job allocator could probably help greatly with these jobs
                        for range in batches(arch.len(), batch_size) {
                            s.spawn(move |_s| {
                                for t in T::iter_range_mut(arch, range) {
                                    f(t);
                                }
                            })
                        }
                    });
            });
        }
    }

    #[cfg(not(feature = "parallel"))]
    pub fn par_for_each<'b>(&'b self, f: impl Fn(<T as QueryFragment>::Item<'a>) + 'b) {
        self.iter().for_each(f);
    }

    #[cfg(not(feature = "parallel"))]
    pub fn par_for_each_mut<'b>(&'b mut self, f: impl Fn(<T as QueryFragment>::ItemMut<'a>) + 'b) {
        self.iter_mut().for_each(f);
    }
}

#[allow(unused)]
fn batches(len: usize, batch_size: usize) -> impl Iterator<Item = impl RangeBounds<usize> + Clone> {
    (0..len / batch_size)
        .map(move |i| {
            let s = i * batch_size;
            s..s + batch_size
        })
        // last batch if len is not divisible by batch_size
        // otherwise it's an empty range
        .chain(std::iter::once((len - (len % batch_size))..len))
}

pub trait QueryFragment {
    type ItemUnsafe<'a>;
    type ItUnsafe<'a>: Iterator<Item = Self::ItemUnsafe<'a>>;
    type Item<'a>;
    type It<'a>: Iterator<Item = Self::Item<'a>>;
    type ItemMut<'a>;
    type ItMut<'a>: Iterator<Item = Self::ItemMut<'a>>;

    unsafe fn iter_unsafe(archetype: &EntityTable) -> Self::ItUnsafe<'_>;
    unsafe fn fetch_unsafe(
        archetype: &EntityTable,
        index: RowIndex,
    ) -> Option<Self::ItemUnsafe<'_>>;
    fn iter(archetype: &EntityTable) -> Self::It<'_>;
    fn iter_mut(archetype: &EntityTable) -> Self::ItMut<'_>;
    fn fetch(archetype: &EntityTable, index: RowIndex) -> Option<Self::Item<'_>>;
    fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>>;
    fn types_mut(set: &mut HashSet<TypeId>);
    fn types_const(set: &mut HashSet<TypeId>);
    fn contains(archetype: &EntityTable) -> bool;
    fn read_only() -> bool;
    fn iter_range(archetype: &EntityTable, range: impl RangeBounds<usize> + Clone) -> Self::It<'_>;
    fn iter_range_mut(
        archetype: &EntityTable,
        range: impl RangeBounds<usize> + Clone,
    ) -> Self::ItMut<'_>;
}

/// Query wether an entity has a component
///
/// Shortcut for `Option<&T>::is_some()`
///
/// In general one should prefer the use of [[With]] and [[WithOut]] filters. But sometimes it can
/// be convenient to use the Has query.
/// ```
/// # use cecs::prelude::*;
/// # let mut world = World::new(4);
/// # #[derive(Debug, Clone, Copy)]
/// # struct Foo;
/// #
/// # world.run_system(|mut cmd: Commands| {
/// #   for i in 0..4 {
/// #     cmd.spawn().insert(Foo);
/// #   }
/// # });
///
/// fn system(q: Query<(Option<&Foo>, Has<Foo>)>) {
/// #   assert_eq!(q.count(), 4);
///     for (opt, has) in q.iter() {
///         assert_eq!(opt.is_some(), has);
///     }
/// }
///
/// # world.run_system(system).unwrap();
/// ```
pub struct Has<T>(PhantomData<T>);

impl<T: Component> QueryFragment for Has<T> {
    type ItemUnsafe<'a> = bool;
    type ItUnsafe<'a> = Self::It<'a>;
    type Item<'a> = bool;
    type It<'a> = std::iter::Take<std::iter::Repeat<bool>>;
    type ItemMut<'a> = bool;
    type ItMut<'a> = Self::It<'a>;

    unsafe fn iter_unsafe<'a>(archetype: &'a EntityTable) -> Self::ItUnsafe<'a> {
        Self::iter(archetype)
    }

    unsafe fn fetch_unsafe(
        archetype: &EntityTable,
        index: RowIndex,
    ) -> Option<Self::ItemUnsafe<'_>> {
        Self::fetch(archetype, index)
    }

    fn iter<'a>(archetype: &'a EntityTable) -> Self::It<'a> {
        std::iter::repeat(archetype.contains_column::<T>()).take(archetype.len())
    }

    fn iter_mut<'a>(archetype: &'a EntityTable) -> Self::ItMut<'a> {
        Self::iter(archetype)
    }

    fn fetch(archetype: &EntityTable, _index: RowIndex) -> Option<Self::Item<'_>> {
        Some(archetype.contains_column::<T>())
    }

    fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>> {
        Self::fetch(archetype, index)
    }

    fn contains(_archetype: &EntityTable) -> bool {
        true
    }

    fn types_mut(_set: &mut HashSet<TypeId>) {
        // noop
    }

    fn types_const(_set: &mut HashSet<TypeId>) {
        // noop
        // Do not care about mutations to component T, only wether it is present in the archetype
    }

    fn read_only() -> bool {
        true
    }

    fn iter_range(archetype: &EntityTable, range: impl RangeBounds<usize>) -> Self::It<'_> {
        let len = archetype.entities.len();
        let range = slice::range(range, ..len);
        let len = range.len();
        std::iter::repeat(archetype.contains_column::<T>()).take(len)
    }

    fn iter_range_mut(
        archetype: &EntityTable,
        range: impl RangeBounds<usize> + Clone,
    ) -> Self::ItMut<'_> {
        Self::iter_range(archetype, range)
    }
}

impl QueryFragment for EntityId {
    type ItemUnsafe<'a> = EntityId;
    type ItUnsafe<'a> = std::iter::Copied<std::slice::Iter<'a, EntityId>>;
    type Item<'a> = EntityId;
    type It<'a> = std::iter::Copied<std::slice::Iter<'a, EntityId>>;
    type ItemMut<'a> = EntityId;
    type ItMut<'a> = std::iter::Copied<std::slice::Iter<'a, EntityId>>;

    unsafe fn iter_unsafe<'a>(archetype: &'a EntityTable) -> Self::ItUnsafe<'a> {
        archetype.entities.iter().copied()
    }

    unsafe fn fetch_unsafe(
        archetype: &EntityTable,
        index: RowIndex,
    ) -> Option<Self::ItemUnsafe<'_>> {
        archetype.entities.get(index as usize).copied()
    }

    fn iter<'a>(archetype: &'a EntityTable) -> Self::It<'a> {
        archetype.entities.iter().copied()
    }

    fn iter_mut<'a>(archetype: &'a EntityTable) -> Self::ItMut<'a> {
        Self::iter(archetype)
    }

    fn fetch(archetype: &EntityTable, index: RowIndex) -> Option<Self::Item<'_>> {
        archetype.entities.get(index as usize).copied()
    }

    fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>> {
        Self::fetch(archetype, index)
    }

    fn contains(_archetype: &EntityTable) -> bool {
        true
    }

    fn types_mut(_set: &mut HashSet<TypeId>) {
        // noop
    }

    fn types_const(_set: &mut HashSet<TypeId>) {
        // noop
        // entity_id is not considered while scheduling
    }

    fn read_only() -> bool {
        true
    }

    fn iter_range(archetype: &EntityTable, range: impl RangeBounds<usize>) -> Self::It<'_> {
        let len = archetype.entities.len();
        let range = slice::range(range, ..len);
        archetype.entities[range].iter().copied()
    }

    fn iter_range_mut(
        archetype: &EntityTable,
        range: impl RangeBounds<usize> + Clone,
    ) -> Self::ItMut<'_> {
        Self::iter_range(archetype, range)
    }
}

impl QueryFragment for ArchetypeHash {
    type ItemUnsafe<'a> = ArchetypeHash;
    type Item<'a> = ArchetypeHash;
    type ItemMut<'a> = ArchetypeHash;
    type It<'a> = Box<dyn Iterator<Item = Self::Item<'a>> + 'a>;
    type ItUnsafe<'a> = Self::It<'a>;
    type ItMut<'a> = Self::It<'a>;

    unsafe fn iter_unsafe<'a>(archetype: &'a EntityTable) -> Self::ItUnsafe<'a> {
        Self::iter(archetype)
    }

    unsafe fn fetch_unsafe(
        archetype: &EntityTable,
        index: RowIndex,
    ) -> Option<Self::ItemUnsafe<'_>> {
        Self::fetch(archetype, index)
    }

    fn iter<'a>(archetype: &'a EntityTable) -> Self::It<'a> {
        let hash = archetype.ty;
        Box::new((0..archetype.rows).map(move |_| ArchetypeHash(hash)))
    }

    fn iter_mut<'a>(archetype: &'a EntityTable) -> Self::ItMut<'a> {
        Self::iter(archetype)
    }

    fn fetch(archetype: &EntityTable, index: RowIndex) -> Option<Self::Item<'_>> {
        archetype
            .entities
            .get(index as usize)
            .map(|_| ArchetypeHash(archetype.ty))
    }

    fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>> {
        Self::fetch(archetype, index)
    }

    fn contains(_archetype: &EntityTable) -> bool {
        true
    }

    fn types_mut(_set: &mut HashSet<TypeId>) {
        // noop
    }

    fn types_const(_set: &mut HashSet<TypeId>) {
        // noop
    }

    fn read_only() -> bool {
        true
    }

    fn iter_range(archetype: &EntityTable, range: impl RangeBounds<usize>) -> Self::It<'_> {
        let len = archetype.entities.len();
        let range = slice::range(range, ..len);
        let hash = archetype.ty;
        Box::new(
            archetype.entities[range]
                .iter()
                .map(move |_| ArchetypeHash(hash)),
        )
    }

    fn iter_range_mut(
        archetype: &EntityTable,
        range: impl RangeBounds<usize> + Clone,
    ) -> Self::ItMut<'_> {
        Self::iter_range(archetype, range)
    }
}

// Optional query fetch functions return Option<Option<T>> where the outer optional is always Some.
// This awkward interface is there because of combined queries
//
impl<'a, T: Component> QueryFragment for Option<&'a T> {
    type ItemUnsafe<'b> = Option<*mut T>;
    type ItUnsafe<'b> = Box<dyn Iterator<Item = Self::ItemUnsafe<'b>> + 'b>;
    type Item<'b> = Option<&'b T>;
    type It<'b> = Box<dyn Iterator<Item = Self::Item<'b>> + 'b>;
    type ItemMut<'b> = Option<&'b T>;
    type ItMut<'b> = Box<dyn Iterator<Item = Self::ItemMut<'b>> + 'b>;

    fn iter(archetype: &EntityTable) -> Self::It<'_> {
        match archetype.components.get(&TypeId::of::<T>()) {
            Some(columns) => {
                Box::new(unsafe { (&*columns.get()).as_slice::<T>().iter() }.map(Some))
            }
            None => Box::new((0..archetype.rows).map(|_| None)),
        }
    }

    fn iter_mut(archetype: &EntityTable) -> Self::ItMut<'_> {
        <Self as QueryFragment>::iter(archetype)
    }

    unsafe fn iter_unsafe(archetype: &EntityTable) -> Self::ItUnsafe<'_> {
        match archetype.components.get(&TypeId::of::<T>()) {
            Some(columns) => Box::new(
                unsafe { (&mut *columns.get()).as_slice_mut::<T>().iter_mut() }
                    .map(|x| x as *mut _)
                    .map(Some),
            ),
            None => Box::new((0..archetype.rows).map(|_| None)),
        }
    }

    fn fetch(archetype: &EntityTable, index: RowIndex) -> Option<Self::Item<'_>> {
        Some(archetype.get_component::<T>(index))
    }

    fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>> {
        Self::fetch(archetype, index)
    }

    unsafe fn fetch_unsafe(
        archetype: &EntityTable,
        index: RowIndex,
    ) -> Option<Self::ItemUnsafe<'_>> {
        unsafe { Some(archetype.get_component_mut::<T>(index).map(|x| x as *mut _)) }
    }

    fn types_mut(_set: &mut HashSet<TypeId>) {
        // noop
    }

    fn types_const(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }

    fn contains(_archetype: &EntityTable) -> bool {
        true
    }

    fn read_only() -> bool {
        true
    }

    fn iter_range(archetype: &EntityTable, range: impl RangeBounds<usize>) -> Self::It<'_> {
        let len = archetype.len();
        let range = slice::range(range, ..len);
        match archetype.components.get(&TypeId::of::<T>()) {
            Some(columns) => unsafe {
                let col = (&*columns.get()).as_slice::<T>();
                Box::new(col[range].iter().map(Some))
            },
            None => Box::new(range.into_iter().map(|_| None)),
        }
    }

    fn iter_range_mut(
        archetype: &EntityTable,
        range: impl RangeBounds<usize> + Clone,
    ) -> Self::ItMut<'_> {
        Self::iter_range(archetype, range)
    }
}

impl<'a, T: Component> QueryFragment for Option<&'a mut T> {
    type ItemUnsafe<'b> = Option<*mut T>;
    type ItUnsafe<'b> = Box<dyn Iterator<Item = Self::ItemUnsafe<'b>> + 'b>;
    type Item<'b> = Option<&'b T>;
    type It<'b> = Box<dyn Iterator<Item = Self::Item<'b>> + 'b>;
    type ItemMut<'b> = Option<&'b mut T>;
    type ItMut<'b> = Box<dyn Iterator<Item = Self::ItemMut<'b>> + 'b>;

    fn iter(archetype: &EntityTable) -> Self::It<'_> {
        match archetype.components.get(&TypeId::of::<T>()) {
            Some(columns) => {
                Box::new(unsafe { (&*columns.get()).as_slice::<T>().iter() }.map(Some))
            }
            None => Box::new((0..archetype.rows).map(|_| None)),
        }
    }

    fn iter_mut(archetype: &EntityTable) -> Self::ItMut<'_> {
        match archetype.components.get(&TypeId::of::<T>()) {
            Some(columns) => {
                Box::new(unsafe { (&mut *columns.get()).as_slice_mut::<T>().iter_mut() }.map(Some))
            }
            None => Box::new((0..archetype.rows).map(|_| None)),
        }
    }

    unsafe fn iter_unsafe(archetype: &EntityTable) -> Self::ItUnsafe<'_> {
        match archetype.components.get(&TypeId::of::<T>()) {
            Some(columns) => Box::new(
                unsafe { (&mut *columns.get()).as_slice_mut::<T>().iter_mut() }
                    .map(|x| x as *mut _)
                    .map(Some),
            ),
            None => Box::new((0..archetype.rows).map(|_| None)),
        }
    }

    fn fetch(archetype: &EntityTable, index: RowIndex) -> Option<Self::Item<'_>> {
        Some(archetype.get_component::<T>(index))
    }

    fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>> {
        Some(unsafe { archetype.get_component_mut::<T>(index) })
    }

    unsafe fn fetch_unsafe(
        archetype: &EntityTable,
        index: RowIndex,
    ) -> Option<Self::ItemUnsafe<'_>> {
        unsafe { Some(archetype.get_component_mut::<T>(index).map(|x| x as *mut _)) }
    }

    fn types_mut(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }

    fn types_const(_set: &mut HashSet<TypeId>) {
        // noop
    }

    fn contains(_archetype: &EntityTable) -> bool {
        true
    }

    fn read_only() -> bool {
        false
    }

    fn iter_range(archetype: &EntityTable, range: impl RangeBounds<usize>) -> Self::It<'_> {
        let len = archetype.len();
        let range = slice::range(range, ..len);
        match archetype.components.get(&TypeId::of::<T>()) {
            Some(columns) => unsafe {
                let col = (&*columns.get()).as_slice::<T>();
                Box::new(col[range].iter().map(Some))
            },
            None => Box::new(range.into_iter().map(|_| None)),
        }
    }

    fn iter_range_mut(
        archetype: &EntityTable,
        range: impl RangeBounds<usize> + Clone,
    ) -> Self::ItMut<'_> {
        let len = archetype.len();
        let range = slice::range(range, ..len);
        match archetype.components.get(&TypeId::of::<T>()) {
            Some(columns) => unsafe {
                let col = (&mut *columns.get()).as_slice_mut::<T>();
                Box::new(col[range].iter_mut().map(Some))
            },
            None => Box::new(range.into_iter().map(|_| None)),
        }
    }
}

impl<'a, T: Component> QueryFragment for &'a T {
    type ItemUnsafe<'b> = *mut T;
    type ItUnsafe<'b> = Box<dyn Iterator<Item = Self::ItemUnsafe<'b>>>;
    type Item<'b> = &'b T;
    type It<'b> = std::iter::Flatten<std::option::IntoIter<std::slice::Iter<'b, T>>>;
    type ItemMut<'b> = &'b T;
    type ItMut<'b> = std::iter::Flatten<std::option::IntoIter<std::slice::Iter<'b, T>>>;

    fn fetch(archetype: &EntityTable, index: RowIndex) -> Option<Self::Item<'_>> {
        archetype.get_component::<T>(index)
    }

    fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>> {
        Self::fetch(archetype, index)
    }

    unsafe fn fetch_unsafe(
        archetype: &EntityTable,
        index: RowIndex,
    ) -> Option<Self::ItemUnsafe<'_>> {
        unsafe { archetype.get_component_mut::<T>(index).map(|x| x as *mut _) }
    }

    fn contains(archetype: &EntityTable) -> bool {
        archetype.contains_column::<T>()
    }

    fn types_mut(_set: &mut HashSet<TypeId>) {
        // noop
    }

    fn types_const(set: &mut HashSet<TypeId>) {
        set.insert(TypeId::of::<T>());
    }

    fn iter(archetype: &EntityTable) -> Self::It<'_> {
        archetype
            .components
            .get(&TypeId::of::<T>())
            .map(|columns| unsafe { (&*columns.get()).as_slice::<T>().iter() })
            .into_iter()
            .flatten()
    }

    fn iter_mut(archetype: &EntityTable) -> Self::ItMut<'_> {
        Self::iter(archetype)
    }

    unsafe fn iter_unsafe(archetype: &EntityTable) -> Self::ItUnsafe<'_> {
        Box::new(
            archetype
                .components
                .get(&TypeId::of::<T>())
                .map(|columns| unsafe {
                    let slice = (&mut *columns.get()).as_slice_mut::<T>();
                    let ptr = slice.as_mut_ptr();
                    let len = slice.len();
                    (0..len).map(move |i| ptr.add(i))
                })
                .into_iter()
                .flatten(),
        )
    }

    fn read_only() -> bool {
        true
    }

    fn iter_range(archetype: &EntityTable, range: impl RangeBounds<usize>) -> Self::ItMut<'_> {
        archetype
            .components
            .get(&TypeId::of::<T>())
            .map(|columns| unsafe {
                let col = (&*columns.get()).as_slice::<T>();
                let len = col.len();
                let range = slice::range(range, ..len);
                col[range].iter()
            })
            .into_iter()
            .flatten()
    }

    fn iter_range_mut(
        archetype: &EntityTable,
        range: impl RangeBounds<usize> + Clone,
    ) -> Self::ItMut<'_> {
        Self::iter_range(archetype, range)
    }
}

impl<'a, T: Component> QueryFragment for &'a mut T {
    type ItemUnsafe<'b> = *mut T;
    type ItUnsafe<'b> = Box<dyn Iterator<Item = *mut T>>;
    type Item<'b> = &'b T;
    type It<'b> = std::iter::Flatten<std::option::IntoIter<std::slice::Iter<'b, T>>>;
    type ItemMut<'b> = &'b mut T;
    type ItMut<'b> = std::iter::Flatten<std::option::IntoIter<std::slice::IterMut<'b, T>>>;

    fn iter(archetype: &EntityTable) -> Self::It<'_> {
        archetype
            .components
            .get(&TypeId::of::<T>())
            .map(|columns| unsafe { (&*columns.get()).as_slice::<T>().iter() })
            .into_iter()
            .flatten()
    }

    fn iter_mut(archetype: &EntityTable) -> Self::ItMut<'_> {
        archetype
            .components
            .get(&TypeId::of::<T>())
            .map(|columns| unsafe { (&mut *columns.get()).as_slice_mut::<T>().iter_mut() })
            .into_iter()
            .flatten()
    }

    unsafe fn iter_unsafe(archetype: &EntityTable) -> Self::ItUnsafe<'_> {
        Box::new(
            archetype
                .components
                .get(&TypeId::of::<T>())
                .map(|columns| unsafe {
                    let slice = (&mut *columns.get()).as_slice_mut::<T>();
                    let ptr = slice.as_mut_ptr();
                    let len = slice.len();
                    (0..len).map(move |i| ptr.add(i))
                })
                .into_iter()
                .flatten(),
        )
    }

    fn fetch(archetype: &EntityTable, index: RowIndex) -> Option<Self::Item<'_>> {
        archetype.get_component::<T>(index)
    }

    fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>> {
        unsafe { archetype.get_component_mut::<T>(index) }
    }

    unsafe fn fetch_unsafe(
        archetype: &EntityTable,
        index: RowIndex,
    ) -> Option<Self::ItemUnsafe<'_>> {
        unsafe { archetype.get_component_mut::<T>(index).map(|x| x as *mut _) }
    }

    fn contains(archetype: &EntityTable) -> bool {
        archetype.contains_column::<T>()
    }

    fn types_mut(set: &mut HashSet<TypeId>) {
        let ty = TypeId::of::<T>();
        debug_assert!(!set.contains(&ty), "A query may only borrow a type once");
        set.insert(ty);
    }

    fn types_const(_set: &mut HashSet<TypeId>) {
        // noop
    }

    fn read_only() -> bool {
        false
    }

    fn iter_range(archetype: &EntityTable, range: impl RangeBounds<usize>) -> Self::It<'_> {
        archetype
            .components
            .get(&TypeId::of::<T>())
            .map(|columns| unsafe {
                let col = (&mut *columns.get()).as_slice::<T>();
                let len = col.len();
                let range = slice::range(range, ..len);
                col[range].iter()
            })
            .into_iter()
            .flatten()
    }

    fn iter_range_mut(
        archetype: &EntityTable,
        range: impl RangeBounds<usize> + Clone,
    ) -> Self::ItMut<'_> {
        archetype
            .components
            .get(&TypeId::of::<T>())
            .map(|columns| unsafe {
                let col = (&mut *columns.get()).as_slice_mut::<T>();
                let len = col.len();
                let range = slice::range(range, ..len);
                col[range].iter_mut()
            })
            .into_iter()
            .flatten()
    }
}

// macro implementing more combinations
//

pub struct TupleIterator<Inner>(Inner);

unsafe impl<Inner> Send for TupleIterator<Inner> {}
unsafe impl<Inner> Sync for TupleIterator<Inner> {}

macro_rules! impl_tuple {
    ($($idx: tt : $t: ident),+ $(,)?) => {
        impl<$($t,)+> Iterator for TupleIterator<
            ($($t),+)
        >
        where
            $( $t: Iterator ),+
        {
            type Item = ( $( $t::Item ),* );

            fn next(&mut self) -> Option<Self::Item> {
                Some((
                    $(
                        // TODO: optimization opportunity: only call next() on the first iterator
                        // and call next_unchecked() on the rest
                        self.0.$idx.next()?
                    ),+
                ))
            }
        }

        impl<$($t,)+> QueryFragment for ($($t,)+)
        where
        $(
            $t: QueryFragment,
        )+
        {
            type ItemUnsafe<'a> = ($(<$t as QueryFragment>::ItemUnsafe<'a>),+);
            type ItUnsafe<'a> = TupleIterator<($(<$t as QueryFragment>::ItUnsafe<'a>,)+)>;
            type Item<'a> = ($(<$t as QueryFragment>::Item<'a>),+);
            type It<'a> = TupleIterator<($(<$t as QueryFragment>::It<'a>,)+)>;
            type ItemMut<'a> = ($(<$t as QueryFragment>::ItemMut<'a>),+);
            type ItMut<'a> = TupleIterator<($(<$t as QueryFragment>::ItMut<'a>,)+)>;

            fn iter(archetype: &EntityTable) -> Self::It<'_>
            {
                TupleIterator(($( $t::iter(archetype) ),+))
            }

            fn iter_mut(archetype: &EntityTable) -> Self::ItMut<'_>
            {
                TupleIterator(($( $t::iter_mut(archetype) ),+))
            }

            fn iter_range(
                archetype: &EntityTable,
                range: impl RangeBounds<usize> + Clone,
            ) -> Self::It<'_> {
                TupleIterator(($( $t::iter_range(archetype, range.clone()) ),+))
            }

            fn iter_range_mut(
                archetype: &EntityTable,
                range: impl RangeBounds<usize> + Clone,
            ) -> Self::ItMut<'_> {
                TupleIterator(($( $t::iter_range_mut(archetype, range.clone()) ),+))
            }

            unsafe fn iter_unsafe(archetype: &EntityTable) -> Self::ItUnsafe<'_>
            {
                unsafe {
                    TupleIterator(($( $t::iter_unsafe(archetype) ),+))
                }
            }

            fn fetch(archetype: &EntityTable, index: RowIndex) -> Option<Self::Item<'_>> {
                Some((
                    $(
                        $t::fetch(archetype, index)?,
                    )*
                ))
            }

            fn fetch_mut(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemMut<'_>> {
                Some((
                    $(
                        $t::fetch_mut(archetype, index)?,
                    )*
                ))
            }

            unsafe fn fetch_unsafe(archetype: &EntityTable, index: RowIndex) -> Option<Self::ItemUnsafe<'_>> {
                unsafe {
                    Some((
                        $(
                            $t::fetch_unsafe(archetype, index)?,
                        )*
                    ))
                }
            }

            fn contains(archetype: &EntityTable) -> bool {
                    $(
                        $t::contains(archetype)
                    )&&*
            }

            fn types_mut(set: &mut HashSet<TypeId>) {
                $(<$t as QueryFragment>::types_mut(set));+
            }

            fn types_const(set: &mut HashSet<TypeId>) {
                $(<$t as QueryFragment>::types_const(set));+
            }

            fn read_only() -> bool {
                $(<$t as QueryFragment>::read_only())&&+
            }
        }

        unsafe impl<'a, $($t,)+> WorldQuery<'a> for ($($t,)+)
        where
            $(
                $t: WorldQuery<'a>,
            )+
        {
            fn new(db: &'a World, system_idx: usize) -> Self {
                (
                    $(
                    <$t as WorldQuery>::new(db, system_idx),
                    )+
                )
            }

            fn exclusive() -> bool {
                $(<$t as WorldQuery>::exclusive())||+
            }

            fn read_only() -> bool {
                $(<$t as WorldQuery>::read_only())&&+
            }

            fn resources_const(_set: &mut HashSet<TypeId>) {
                $(<$t as WorldQuery>::resources_const(_set));+
            }
            fn resources_mut(_set: &mut HashSet<TypeId>) {
                $(<$t as WorldQuery>::resources_mut(_set));+
            }
            fn components_const(_set: &mut HashSet<TypeId>) {
                $(<$t as WorldQuery>::components_const(_set));+
            }
            fn components_mut(_set: &mut HashSet<TypeId>) {
                $(<$t as WorldQuery>::components_mut(_set));+
            }
        }
    };
}

impl_tuple!(0: T0, 1: T1);
impl_tuple!(0: T0, 1: T1, 2: T2);
impl_tuple!(0: T0, 1: T1, 2: T2, 3: T3);
impl_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4);
impl_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5);
impl_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6);
impl_tuple!(0: T0, 1: T1, 2: T2, 3: T3, 4: T4, 5: T5, 6: T6, 7: T7);
impl_tuple!(
    0: T0,
    1: T1,
    2: T2,
    3: T3,
    4: T4,
    5: T5,
    6: T6,
    7: T7,
    8: T8
);
impl_tuple!(
    0: T0,
    1: T1,
    2: T2,
    3: T3,
    4: T4,
    5: T5,
    6: T6,
    7: T7,
    8: T8,
    9: T9
);
impl_tuple!(
    0: T0,
    1: T1,
    2: T2,
    3: T3,
    4: T4,
    5: T5,
    6: T6,
    7: T7,
    8: T8,
    9: T9,
    10: T10
);
impl_tuple!(
    0: T0,
    1: T1,
    2: T2,
    3: T3,
    4: T4,
    5: T5,
    6: T6,
    7: T7,
    8: T8,
    9: T9,
    10: T10,
    11: T11
);
impl_tuple!(
    0: T0,
    1: T1,
    2: T2,
    3: T3,
    4: T4,
    5: T5,
    6: T6,
    7: T7,
    8: T8,
    9: T9,
    10: T10,
    11: T11,
    12: T12
);
impl_tuple!(
    0: T0,
    1: T1,
    2: T2,
    3: T3,
    4: T4,
    5: T5,
    6: T6,
    7: T7,
    8: T8,
    9: T9,
    10: T10,
    11: T11,
    12: T12,
    13: T13
);
impl_tuple!(
    0: T0,
    1: T1,
    2: T2,
    3: T3,
    4: T4,
    5: T5,
    6: T6,
    7: T7,
    8: T8,
    9: T9,
    10: T10,
    11: T11,
    12: T12,
    13: T13,
    14: T14
);
impl_tuple!(
    0: T0,
    1: T1,
    2: T2,
    3: T3,
    4: T4,
    5: T5,
    6: T6,
    7: T7,
    8: T8,
    9: T9,
    10: T10,
    11: T11,
    12: T12,
    13: T13,
    14: T14,
    15: T15
);

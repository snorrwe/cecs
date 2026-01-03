#[cfg(test)]
mod test;

use std::{any::TypeId, collections::HashSet, ptr::NonNull, sync::Arc, u128};

use cfg_if::cfg_if;
use rustc_hash::FxHashMap;

#[cfg(feature = "parallel")]
use crate::job_system::{AsJob, ExecutionState};

use crate::{World, query::WorldQuery};

pub type InnerSystem<'a, R> = dyn Fn(&'a World, usize) -> R + 'a;
pub type ShouldRunSystem<'a> = InnerSystem<'a, bool>;

type SystemStorage<T> = Vec<T>;

pub fn sorted_systems<'a, T>(
    sys: impl IntoIterator<Item = ErasedSystem<'a, T>>,
) -> Vec<ErasedSystem<'a, T>> {
    // TODO: allow the same system id to appear multiple times?
    let mut systems = FxHashMap::default();
    // preserve the original order if no other ordering requirement is present
    let mut systemids = Vec::new();
    for s in sys.into_iter() {
        // ensure unique keys even if there are duplicate systems
        systemids.push(s.descriptor.id);
        let _r = systems.insert(s.descriptor.id, s);
        assert!(
            _r.is_none(),
            "Duplicate systems are not allowed in a single stage"
        );
    }
    let mut res = Vec::with_capacity(systems.len());
    let mut pending = Vec::default();

    for id in systemids {
        topo_sort_systems(id, &mut systems, &mut res, &mut pending);
    }
    res
}

fn topo_sort_systems<'a, T>(
    id: TypeId,
    systems: &mut FxHashMap<TypeId, ErasedSystem<'a, T>>,
    out: &mut Vec<ErasedSystem<'a, T>>,
    pending: &mut Vec<TypeId>,
) {
    assert!(!pending.contains(&id), "Circular dependencies detected");
    let Some(sys) = systems.remove(&id) else {
        return;
    };
    pending.push(id);
    for id in sys.descriptor.after.iter().copied() {
        topo_sort_systems(id, systems, out, pending);
    }
    pending.pop();
    out.push(sys);
}

#[derive(Clone, Default)]
pub struct SystemStage<'a> {
    pub name: String,
    pub(crate) should_run: SystemStorage<ErasedSystem<'a, bool>>,
    pub(crate) systems: SystemStorage<ErasedSystem<'a, ()>>,
}

#[derive(Clone, Default)]
pub struct SystemStageBuilder<'a> {
    pub name: String,
    pub should_run: SystemStorage<ErasedSystem<'a, bool>>,
    pub systems: SystemStorage<ErasedSystem<'a, ()>>,
    /// Nested stages inherit the parent `should_run` conditions
    /// By default, nested systems are ordered after the parent systems
    ///
    /// Nested stages share their Commands apply point with the parent stage
    pub nested: Vec<SystemStageBuilder<'a>>,
}

impl<'a> From<SystemStageBuilder<'a>> for SystemStage<'a> {
    fn from(value: SystemStageBuilder<'a>) -> Self {
        value.build()
    }
}

fn collapse_stages<'a>(
    should_run: &mut SystemStorage<ErasedSystem<'a, bool>>,
    systems: &mut SystemStorage<ErasedSystem<'a, ()>>,
    mut stage: SystemStageBuilder<'a>,
    mut flags: usize,
    mut mask: ShouldRunFlags,
) {
    let nflags = stage.should_run.len();
    assert!(
        nflags + flags < 128,
        "Up to 128 should_run systems are supported in a stage. Including child stages"
    );
    for i in flags..flags + nflags {
        mask |= 1 << i;
    }
    flags += nflags;
    for sys in stage.systems.iter_mut() {
        sys.should_run_mask = mask;
    }
    systems.extend(stage.systems.into_iter());
    should_run.extend(stage.should_run.into_iter());
    for child in stage.nested {
        collapse_stages(should_run, systems, child, flags, mask);
    }
}

impl<'a> SystemStageBuilder<'a> {
    pub fn build(self) -> SystemStage<'a> {
        let mut systems = Default::default();
        let mut should_run = Default::default();
        let name = self.name.clone();

        collapse_stages(&mut should_run, &mut systems, self, 0, 0);

        SystemStage {
            name: name,
            systems: sorted_systems(systems),
            should_run: sorted_systems(should_run),
        }
    }

    pub fn new<N: Into<String>>(name: N) -> Self {
        Self {
            name: name.into(),
            should_run: SystemStorage::with_capacity(1),
            systems: SystemStorage::with_capacity(4),
            ..Default::default()
        }
    }

    pub fn with_nested_stage(mut self, stage: SystemStageBuilder<'a>) -> Self {
        self.add_nested_stage(stage);
        self
    }

    pub fn add_nested_stage(&mut self, stage: SystemStageBuilder<'a>) {
        self.nested.push(stage);
    }

    /// Multiple should_runs will be executed serially, and "and'ed" together in the same order as
    /// they were registered.
    ///
    /// Nested stages inherit their parent should_run systems
    pub fn with_should_run<S, P>(mut self, system: S) -> Self
    where
        S: IntoSystem<'a, P, bool>,
    {
        self.add_should_run(system);
        self
    }

    pub fn add_should_run<S, P>(&mut self, system: S) -> &mut Self
    where
        S: IntoSystem<'a, P, bool>,
    {
        let system = system.descriptor().into();
        self.should_run.push(system);
        self
    }

    pub fn add_system<S, P>(&mut self, system: S) -> &mut Self
    where
        S: IntoSystem<'a, P, ()>,
    {
        let system_idx;

        cfg_if!(
            if #[cfg(feature = "parallel")] {
                system_idx = self.systems.len();
            }
            else {
                system_idx = 0;
            }
        );

        let descriptor = Arc::new(system.descriptor());
        let system = ErasedSystem {
            execute: (descriptor.factory)(),
            system_idx,
            descriptor,
            should_run_mask: 0,
        };
        self.systems.push(system);
        self
    }

    pub fn with_system<S, P>(mut self, system: S) -> Self
    where
        S: IntoSystem<'a, P, ()>,
    {
        self.add_system(system);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// return the number of systems in total in this stage
    pub fn len(&self) -> usize {
        self.systems.len() + self.should_run.len()
    }
}

impl<'a> SystemStage<'a> {
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// return the number of systems in total in this stage
    pub fn len(&self) -> usize {
        self.systems.len() + self.should_run.len()
    }

    pub fn new<N: Into<String>>(name: N) -> SystemStageBuilder<'a> {
        SystemStageBuilder::new(name)
    }
}

#[allow(unused)] // with no feature=parallel most of this struct is unused
pub struct SystemDescriptor<'a, R> {
    pub name: String,
    pub id: TypeId,
    pub components_mut: Box<dyn 'a + Fn() -> HashSet<TypeId>>,
    pub resources_mut: Box<dyn 'a + Fn() -> HashSet<TypeId>>,
    pub components_const: Box<dyn 'a + Fn() -> HashSet<TypeId>>,
    pub resources_const: Box<dyn 'a + Fn() -> HashSet<TypeId>>,
    pub exclusive: Box<dyn 'a + Fn() -> bool>,
    /// produce a system
    pub factory: Box<dyn 'a + Fn() -> Box<InnerSystem<'a, R>>>,
    pub read_only: Box<dyn 'a + Fn() -> bool>,
    pub after: HashSet<TypeId>,
}

unsafe impl<'a, R> Send for SystemDescriptor<'a, R> {}
unsafe impl<'a, R> Sync for SystemDescriptor<'a, R> {}

pub type ShouldRunFlags = u128;

pub struct ErasedSystem<'a, R> {
    pub(crate) system_idx: usize,
    pub(crate) execute: Box<InnerSystem<'a, R>>,
    pub(crate) descriptor: Arc<SystemDescriptor<'a, R>>,
    /// System is enabled iff `should_run_flags & should_run_mask == should_run_mask`
    pub(crate) should_run_mask: ShouldRunFlags,
}

impl<'a, R> From<SystemDescriptor<'a, R>> for ErasedSystem<'a, R> {
    fn from(system: SystemDescriptor<'a, R>) -> Self {
        let descriptor = Arc::new(system);
        ErasedSystem {
            execute: (descriptor.factory)(),
            system_idx: 0,
            descriptor,
            should_run_mask: 0,
        }
    }
}

unsafe impl<R> Send for ErasedSystem<'_, R> {}
unsafe impl<R> Sync for ErasedSystem<'_, R> {}

impl<'a, R> Clone for ErasedSystem<'a, R> {
    fn clone(&self) -> Self {
        Self {
            system_idx: self.system_idx,
            execute: (self.descriptor.factory)(),
            descriptor: self.descriptor.clone(),
            should_run_mask: self.should_run_mask,
        }
    }
}

pub trait IntoSystem<'a, Param, R> {
    fn descriptor(self) -> SystemDescriptor<'a, R>;
    /// Order this system after another system
    /// Note that if the systems have no conflicting queries, then they may still execute in
    /// parallel
    fn after<'b, P, R2>(self, rhs: impl IntoSystem<'b, P, R2>) -> SystemDescriptor<'a, R>
    where
        Self: Sized,
    {
        let mut res = self.descriptor();
        let desc = rhs.descriptor();
        let id = desc.id;
        res.after.insert(id);
        res
    }
}

pub trait IntoOnceSystem<'a, Param, R> {
    fn into_once_system(self) -> impl FnOnce(&World, usize) -> R;
    fn descriptor() -> SystemDescriptor<'a, R>;
}

// helps chaining methods
impl<'a, R> IntoSystem<'a, (), R> for SystemDescriptor<'a, R> {
    fn descriptor(self) -> SystemDescriptor<'a, R> {
        self
    }
}

pub struct SystemJob<'a, R> {
    pub world: NonNull<World>,
    pub sys: NonNull<ErasedSystem<'a, R>>,
}

impl<'a, R> std::fmt::Debug for SystemJob<'a, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sys = unsafe { self.sys.as_ref().descriptor.name.as_str() };
        f.debug_struct("SystemJob").field("sys", &sys).finish()
    }
}

unsafe impl<'a, R> Send for SystemJob<'a, R> {}

#[cfg(feature = "parallel")]
impl<'a, R> AsJob for SystemJob<'a, R> {
    unsafe fn execute(this: *const ()) -> ExecutionState {
        unsafe {
            let job: *const Self = this.cast();
            let job = &*job;
            let sys = job.sys.as_ref();
            let world = job.world.as_ref();

            #[cfg(feature = "tracing")]
            let _e = tracing::trace_span!("system", name = sys.descriptor.name.as_str()).entered();

            (sys.execute)(world, sys.system_idx);
            ExecutionState::Done
        }
    }
}

// empty function implementations
// would conflict with the macro below if I tried to include them so duplicating it is
impl<'a, R, F> IntoSystem<'a, (), R> for F
where
    F: Fn() -> R + 'static + Copy,
{
    fn descriptor(self) -> SystemDescriptor<'a, R> {
        let factory: Box<dyn Fn() -> Box<InnerSystem<'a, R>>> =
            Box::new(move || Box::new(move |_world: &'a World, _system_idx| (self)()));
        SystemDescriptor {
            id: TypeId::of::<F>(),
            name: std::any::type_name::<F>().into(),
            components_mut: Box::new(HashSet::new),
            resources_mut: Box::new(HashSet::new),
            components_const: Box::new(HashSet::new),
            resources_const: Box::new(HashSet::new),
            exclusive: Box::new(|| false),
            read_only: Box::new(|| true),
            factory,
            after: HashSet::new(),
        }
    }
}

impl<'a, R: 'static, F> IntoOnceSystem<'a, (), R> for F
where
    F: FnOnce() -> R,
{
    fn into_once_system(self) -> impl FnOnce(&World, usize) -> R {
        move |_world: &World, _system_idx| (self)()
    }
    fn descriptor() -> SystemDescriptor<'a, R> {
        let dummy: fn() -> R = || unreachable!();
        dummy.descriptor()
    }
}

macro_rules! impl_intosys_fn {
    ($($t: ident),* $(,)*) => {
        #[allow(unused_parens)]
        #[allow(unused_mut)]
        impl<'a, R, F, $($t: WorldQuery<'a> + 'static,)*>
            IntoSystem<'a, ($($t),*,), R> for F
        where
            F: Fn($($t),*) -> R + 'static + Copy,
        {
            fn descriptor(self) -> SystemDescriptor<'a, R> {
                #[cfg(debug_assertions)]
                {
                    let mut _props = crate::query::QueryProperties::default();
                    // assert queries
                    $(
                        let p = crate::query::ensure_query_valid::<$t>();
                        assert!(p.is_disjoint(&_props) || (p.exclusive && _props.is_empty())
                                , "system {} has incompatible queries!", std::any::type_name::<F>());
                        _props.extend(p);
                    )*
                }
                let factory: Box<dyn Fn()-> Box<InnerSystem<'a, R>>>
                    = Box::new(move || {
                        Box::new(move |_world: &'a World, _system_idx| {
                            (self)(
                                $(<$t>::new(_world, _system_idx),)*
                            )
                        })
                    });
                SystemDescriptor {
                    id: TypeId::of::<F>(),
                    name: std::any::type_name::<F>().into(),
                    components_mut:Box::new( || {
                        let mut res = HashSet::new();
                        $(<$t>::components_mut(&mut res);)*
                        res
                    }),
                    resources_mut:Box::new( || {
                        let mut res = HashSet::new();
                        $(<$t>::resources_mut(&mut res);)*
                        res
                    }),
                    components_const:Box::new( || {
                        let mut res = HashSet::new();
                        $(<$t>::components_const(&mut res);)*
                        res
                    }),
                    resources_const:Box::new( || {
                        let mut res = HashSet::new();
                        $(<$t>::resources_const(&mut res);)*
                        res
                    }),
                    exclusive:Box::new( || {
                        // empty system is not exclusive
                        false $(|| <$t>::exclusive())*
                    }),
                    read_only:Box::new( || {
                        // empty system is read_only
                        true $(&& <$t>::read_only())*
                    }),
                    factory,
                    after: HashSet::new(),
                }
            }
        }

        #[allow(unused_parens)]
        #[allow(unused_mut)]
        impl<'a, R: 'static, F, $($t: WorldQuery<'a> + 'static,)*>
            IntoOnceSystem<'a, ($($t),*,), R> for F
        where
            F: FnOnce($($t),*) -> R,
        {
            fn into_once_system(self) -> impl FnOnce(&World, usize) -> R {
                move |_world: &World, _system_idx| {
                    (self)(
                        $( unsafe { <$t>::new(std::mem::transmute::<&World, &World>(_world), _system_idx)},)*
                    )
                }
            }
            fn descriptor() -> SystemDescriptor<'a, R> {
                let dummy: fn($($t),*) -> R = |$(_:$t),*| unreachable!();
                dummy.descriptor()
            }
        }
    };
}

// impl_intosys_fn!();
impl_intosys_fn!(Q0);
impl_intosys_fn!(Q0, Q1);
impl_intosys_fn!(Q0, Q1, Q2);
impl_intosys_fn!(Q0, Q1, Q2, Q3);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5, Q6);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12);
impl_intosys_fn!(Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13);
impl_intosys_fn!(
    Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14
);
impl_intosys_fn!(
    Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15
);
impl_intosys_fn!(
    Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16
);
impl_intosys_fn!(
    Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17
);
impl_intosys_fn!(
    Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18
);
impl_intosys_fn!(
    Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19
);
impl_intosys_fn!(
    Q0, Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12, Q13, Q14, Q15, Q16, Q17, Q18, Q19, Q20
);

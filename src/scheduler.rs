use rustc_hash::FxHashMap;
use std::ptr::NonNull;

use smallvec::SmallVec;

use crate::{
    World,
    job_system::HomogeneousJobGraph,
    query::QueryProperties,
    systems::{ErasedSystem, ShouldRunFlags, SystemJob, SystemStage},
};

#[derive(Debug, Clone)]
pub struct Schedule {
    parents: Vec<SmallVec<[usize; 8]>>,
}

impl Schedule {
    pub fn from_stage(stage: &SystemStage) -> Self {
        let systems = &stage.systems;

        let mut res = Self {
            parents: Vec::with_capacity(systems.len()),
        };
        if systems.is_empty() {
            return res;
        }
        let indices = systems
            .iter()
            .enumerate()
            .map(|(i, sys)| (sys.descriptor.id, i))
            .collect::<FxHashMap<_, _>>();

        let mut history = vec![QueryProperties::from_system(&systems[0].descriptor)];
        history.reserve(systems.len() - 1);
        res.parents.push(Default::default());
        assert!(systems[0].descriptor.after.is_empty(), "bad ordering");
        for (i, sys) in systems.iter().enumerate().skip(1) {
            let props = QueryProperties::from_system(&sys.descriptor);
            res.parents.push(Default::default());

            for (j, history) in history.iter().enumerate().take(i) {
                if !props.is_disjoint(history) {
                    res.parents[i].push(j);
                }
            }
            // Insert explicit dependencies
            // SystemStage systems are ordered based on their ordering criteria
            // So we can't create deadlocks with the above system. We may create duplicate
            // dependencies, but that's fine
            for id in sys.descriptor.after.iter() {
                if let Some(j) = indices.get(id) {
                    res.parents[i].push(*j);
                }
            }

            history.push(props);
        }
        res
    }

    /// NOTE: `stage` must be the same as the one used to create this schedule
    pub fn jobs<'a, T>(
        &self,
        stage: &[ErasedSystem<'a, T>],
        mask: ShouldRunFlags,
        world: &World,
    ) -> HomogeneousJobGraph<SystemJob<'a, T>> {
        debug_assert_eq!(stage.len(), self.parents.len());

        // The filtering (by the mask) will displace jobs ids which can cause UB/segfault when
        // setting the dependencies
        let mut id_map = FxHashMap::default();

        let mut graph = HomogeneousJobGraph::new(
            stage
                .iter()
                .enumerate()
                .filter(|(_, sys)| sys.should_run_mask & mask == sys.should_run_mask)
                .enumerate()
                .map(|(new_id, (old_id, s))| {
                    id_map.insert(old_id, new_id);
                    SystemJob {
                        // TODO: neither of these should move in memory
                        // so maybe memoize the vector and clone per tick?
                        world: NonNull::from(world),
                        sys: NonNull::from(s),
                    }
                })
                .collect::<Vec<_>>(),
        );

        for (i, parents) in self.parents.iter().enumerate() {
            let Some(i) = id_map.get(&i) else {
                continue;
            };
            for j in parents {
                let Some(j) = id_map.get(&j) else {
                    continue;
                };
                graph.add_dependency(*j, *i);
            }
        }

        graph
    }
}

#[cfg(test)]
mod test;

use std::{any::TypeId, collections::HashSet, marker::PhantomData};

use crate::{
    SystemProperties,
    prelude::{Filter, Query},
    query::{QueryFragment, WorldQuery},
};

/// A QuerySet can be used when a system needs multiple, coupled queries.
///
/// ## Limitations
///
/// Currently if a sub-query needs mutable access to a component, then all sub-queries to the same
/// component must also be mutable.
///
/// The way we implement fetch can actually break Rust's aliasing semantics. The programmer must
/// ensure that using different queries does not result in mutable aliasing (fetching the same
/// component on the same entity twice at the same time).
///
/// ```
/// use cecs::prelude::*;
/// #[derive(Default, Clone)]
/// struct Foo {
///     value: i32,
/// }
///
/// #[derive(Default, Clone)]
/// struct Bar;
///
/// fn sys<'a>(
///     mut q: QuerySet<(
///         Query<'a, (&'a mut Foo, &'a Bar)>,
///         Query<'a, &'a mut Foo>,
///     )>,
/// ) {
///     for foo in q.q1_mut().iter_mut() {
///         assert_eq!(foo.value, 0);
///         foo.value = 42;
///     }
///
///     // notice how foo is not changed in this loop, but q0 still has to request mutable access
///     for (foo, _bar) in q.q0().iter() {
///         assert_eq!(foo.value, 42);
///     }
/// }
/// let mut world = World::new(4);
/// world.run_system(sys).unwrap();
/// ```
pub struct QuerySet<Inner> {
    world: std::ptr::NonNull<crate::World>,
    _m: PhantomData<Inner>,
}

unsafe impl<T> Send for QuerySet<T> {}
unsafe impl<T> Sync for QuerySet<T> {}

macro_rules! impl_tuple {
    ($($idx: tt , $t: ident , $f: ident , $q: ident , $q_mut: ident , $set: ident);+ $(;)?) => {
        impl<'a, $($t, $f),*> QuerySet<($(Query<'a, $t, $f>),*)>
        where
            $(
            $t: QueryFragment,
            $f: Filter,
            )*
        {
            $(
                pub fn $q<'b>(&'b self) -> Query<'b, $t, $f>
                    where 'a: 'b
                {
                    unsafe {Query::new(self.world.as_ref())}
                }

                pub fn $q_mut<'b>(&'b mut self) -> Query<'b, $t, $f>
                    where 'a: 'b
                {
                    unsafe {Query::new(self.world.as_ref())}
                }
            )*
        }

        unsafe impl<'a, $($t, $f),*> WorldQuery<'a> for QuerySet<($(Query<'a, $t, $f>),*)>
        where
            $(
            $t: QueryFragment,
            $f: Filter,
            )*
        {
            fn new(db: &'a crate::World, _props: SystemProperties) -> Self {
                Self {
                    world: std::ptr::NonNull::from(db),
                    _m: PhantomData,
                }
            }

            fn exclusive() -> bool {false}

            fn read_only() -> bool {
                true
                $(
                    &&
                    <$t as QueryFragment>::read_only()
                )*
            }

            fn components_mut(set: &mut HashSet<TypeId>) {
                // sub queries may have overlapping type (that's the point of the QuerySet)
                // types_mut will panic in this case, so we'll try all in isolation, then
                // add the types to the output
                $(
                    let mut $set = set.clone();
                    <$t as QueryFragment>::types_mut(&mut $set);
                )*

                $(
                    set.extend($set.into_iter());
                )*
            }

            fn components_const(set: &mut HashSet<TypeId>) {
                $(
                    <$t as QueryFragment>::types_const(set);
                )*
                // remove mutable components from the const compoennts
                let mut mutable_components = HashSet::new();
                let mut tmp = HashSet::new();
                $(
                    tmp.clear();
                    // types_mut may check the set for duplicate components, so always pass an
                    // empty set here
                    <$t as QueryFragment>::types_mut(&mut tmp);
                    mutable_components.extend(tmp.drain());
                )*
                set.retain(|t| !mutable_components.contains(t));
            }
        }
    };
}

impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1;);
impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1; 2 , T2 , F2 , q2 , q2_mut , set2;);
impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1; 2 , T2 , F2 , q2 , q2_mut , set2; 3 , T3 , F3 , q3 , q3_mut , set3;);
impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1; 2 , T2 , F2 , q2 , q2_mut , set2; 3 , T3 , F3 , q3 , q3_mut , set3; 4 , T4 , F4 , q4 , q4_mut , set4;);
impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1; 2 , T2 , F2 , q2 , q2_mut , set2; 3 , T3 , F3 , q3 , q3_mut , set3; 4 , T4 , F4 , q4 , q4_mut , set4; 5 , T5 , F5 , q5 , q5_mut , set5;);
impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1; 2 , T2 , F2 , q2 , q2_mut , set2; 3 , T3 , F3 , q3 , q3_mut , set3; 4 , T4 , F4 , q4 , q4_mut , set4; 5 , T5 , F5 , q5 , q5_mut , set5; 6 , T6 , F6 , q6 , q6_mut , set6;);
impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1; 2 , T2 , F2 , q2 , q2_mut , set2; 3 , T3 , F3 , q3 , q3_mut , set3; 4 , T4 , F4 , q4 , q4_mut , set4; 5 , T5 , F5 , q5 , q5_mut , set5; 6 , T6 , F6 , q6 , q6_mut , set6; 7 , T7 , F7 , q7 , q7_mut , set7;);
impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1; 2 , T2 , F2 , q2 , q2_mut , set2; 3 , T3 , F3 , q3 , q3_mut , set3; 4 , T4 , F4 , q4 , q4_mut , set4; 5 , T5 , F5 , q5 , q5_mut , set5; 6 , T6 , F6 , q6 , q6_mut , set6; 7 , T7 , F7 , q7 , q7_mut , set7; 8 , T8 , F8 , q8 , q8_mut , set8;);
impl_tuple!(0 , T0 , F0 , q0 , q0_mut , set0; 1 , T1 , F1 , q1 , q1_mut , set1; 2 , T2 , F2 , q2 , q2_mut , set2; 3 , T3 , F3 , q3 , q3_mut , set3; 4 , T4 , F4 , q4 , q4_mut , set4; 5 , T5 , F5 , q5 , q5_mut , set5; 6 , T6 , F6 , q6 , q6_mut , set6; 7 , T7 , F7 , q7 , q7_mut , set7; 8 , T8 , F8 , q8 , q8_mut , set8; 9 , T9 , F9 , q9 , q9_mut , set9;);

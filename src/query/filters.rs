use std::marker::PhantomData;

use crate::{Component, table::EntityTable};

pub trait Filter {
    fn filter(archetype: &EntityTable) -> bool;
}

pub struct With<T>(PhantomData<T>);

impl<T: Component> Filter for With<T> {
    fn filter(archetype: &EntityTable) -> bool {
        archetype.contains_column::<T>()
    }
}

pub struct WithOut<T>(PhantomData<T>);

impl<T: Component> Filter for WithOut<T> {
    fn filter(archetype: &EntityTable) -> bool {
        !archetype.contains_column::<T>()
    }
}

pub struct Or<X, Y>(PhantomData<(X, Y)>);

impl<X: Filter, Y: Filter> Filter for Or<X, Y> {
    fn filter(archetype: &EntityTable) -> bool {
        X::filter(archetype) || Y::filter(archetype)
    }
}

impl Filter for () {
    fn filter(_archetype: &EntityTable) -> bool {
        true
    }
}

macro_rules! impl_tuple {
    ($($t: ident),+ $(,)?) => {
        impl<$($t : Filter,)+> Filter for ($($t,)+) {
            fn filter(archetype: &EntityTable) -> bool {
                $($t::filter(archetype))&&+
            }
        }
    };
}

impl_tuple!(T0, T1);
impl_tuple!(T0, T1, T2);
impl_tuple!(T0, T1, T2, T3);
impl_tuple!(T0, T1, T2, T3, T4);
impl_tuple!(T0, T1, T2, T3, T4, T5);
impl_tuple!(T0, T1, T2, T3, T4, T5, T6);
impl_tuple!(T0, T1, T2, T3, T4, T5, T6, T7);
impl_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);

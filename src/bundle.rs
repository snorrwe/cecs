use crate::{Component, RowIndex, TypeHash, WorldResult, hash_ty, table::EntityTable};

pub trait Bundle {
    fn compute_hash_from_table(archetype: &EntityTable) -> TypeHash;
    fn can_insert(&self, archetype: &EntityTable) -> bool;
    fn insert_into(self, archetype: &mut EntityTable, index: RowIndex) -> WorldResult<()>;
    fn extend(archetype: &EntityTable) -> EntityTable;
}

macro_rules! impl_tuple {
    ($(($i: tt, $ty: ident)),+ $(,)*) => {
        impl<$($ty: Component),+> Bundle for ($($ty),+,) {
            fn compute_hash_from_table(archetype: &EntityTable) -> TypeHash {
                let mut ty = archetype.ty();
                $(
                   if !archetype.contains_column::<$ty>() {
                        ty = ty ^hash_ty::<$ty>();
                   }
                )*
                ty
            }


            fn can_insert(&self, archetype: &EntityTable) -> bool {
                $(archetype.contains_column::<$ty>())&&*
            }

            fn insert_into(self, archetype: &mut EntityTable, index: RowIndex) -> WorldResult<()> {
                $(archetype.set_component(index, self.$i);)*
                Ok(())
            }

            fn extend(archetype: &EntityTable) -> EntityTable {
                let mut result = archetype.clone_empty();
                $(
                    result = result.extend_with_column::<$ty>();
                )*
                result
            }
        }
    };
}

impl_tuple!((0, T0));
impl_tuple!((0, T0), (1, T1));
impl_tuple!((0, T0), (1, T1), (2, T2));
impl_tuple!((0, T0), (1, T1), (2, T2), (3, T3));
impl_tuple!((0, T0), (1, T1), (2, T2), (3, T3), (4, T4));
impl_tuple!((0, T0), (1, T1), (2, T2), (3, T3), (4, T4), (5, T5));
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14),
    (15, T15)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14),
    (15, T15),
    (16, T16)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14),
    (15, T15),
    (16, T16),
    (17, T17)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14),
    (15, T15),
    (16, T16),
    (17, T17),
    (18, T18)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14),
    (15, T15),
    (16, T16),
    (17, T17),
    (18, T18),
    (19, T19)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14),
    (15, T15),
    (16, T16),
    (17, T17),
    (18, T18),
    (19, T19),
    (20, T20)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14),
    (15, T15),
    (16, T16),
    (17, T17),
    (18, T18),
    (19, T19),
    (20, T20),
    (21, T21)
);
impl_tuple!(
    (0, T0),
    (1, T1),
    (2, T2),
    (3, T3),
    (4, T4),
    (5, T5),
    (6, T6),
    (7, T7),
    (8, T8),
    (9, T9),
    (10, T10),
    (11, T11),
    (12, T12),
    (13, T13),
    (14, T14),
    (15, T15),
    (16, T16),
    (17, T17),
    (18, T18),
    (19, T19),
    (20, T20),
    (21, T21),
    (22, T22)
);

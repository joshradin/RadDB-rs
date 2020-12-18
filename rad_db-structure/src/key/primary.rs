use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use num_bigint::{BigUint, ToBigUint};
use num_traits::FromPrimitive;
use rad_db_types::{Numeric, SameType, Type};
use seahash::SeaHasher;

#[derive(Debug, Clone)]
pub struct PrimaryKeyDefinition(Vec<usize>);

impl PrimaryKeyDefinition {
    pub fn new(fields: Vec<usize>) -> Self {
        PrimaryKeyDefinition(fields)
    }

    pub(crate) fn create_seeds(&self) -> [u64; 4] {
        let mut start: u64 = 0;
        let add = true;
        for f in &self.0 {
            if add {
                start = start.wrapping_add(*f as u64);
            } else {
                start = start.wrapping_mul(*f as u64);
            }
        }
        [
            start,
            start.rotate_left(16),
            start.rotate_left(32),
            start.rotate_left(48),
        ]
    }
}

impl Deref for PrimaryKeyDefinition {
    type Target = Vec<usize>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct PrimaryKey<'a>(Vec<&'a Type>, [u64; 4]);

impl<'a> PrimaryKey<'a> {
    pub fn new(attributes: Vec<&'a Type>, seeds: [u64; 4]) -> Self {
        PrimaryKey(attributes, seeds)
    }

    pub fn hash(&self) -> BigUint {
        if self.len() == 1 {
            if let Type::Numeric(Numeric::Unsigned(unsigned)) = *self.0[0] {
                let fast: u64 = unsigned.into();
                return BigUint::from_u64(fast).unwrap();
            }
        }

        let mut hash_value = 0.to_biguint().unwrap();

        for ty in self {
            hash_value <<= std::mem::size_of::<u64>() * 8;
            /// seeds need to be consistent between runs
            let mut hasher = SeaHasher::with_seeds(self.1[0], self.1[1], self.1[2], self.1[3]);
            ty.hash(&mut hasher);
            let single_hashed = hasher.finish().to_biguint().unwrap();
            hash_value |= single_hashed;
        }
        hash_value
    }
}

impl<'a> Deref for PrimaryKey<'a> {
    type Target = Vec<&'a Type>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq for PrimaryKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        if !self.same_type(other) {
            return false;
        }

        let mut self_iter = self.iter();
        let mut other_iter = self.iter();

        while let (Some(mine), Some(theirs)) = (self_iter.next(), other_iter.next()) {
            if mine != theirs {
                return false;
            }
        }

        true
    }
}

impl<'a> IntoIterator for &PrimaryKey<'a> {
    type Item = &'a Type;
    type IntoIter = <Vec<&'a Type> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let ret = (&self.0).into_iter().map(|v| *v).collect::<Vec<_>>();
        ret.into_iter()
    }
}

impl<'a> IntoIterator for PrimaryKey<'a> {
    type Item = &'a Type;
    type IntoIter = <Vec<&'a Type> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Eq for PrimaryKey<'_> {}

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use num_bigint::{BigUint, ToBigUint};
use rad_db_types::{SameType, Type};

#[derive(Debug, Clone)]
pub struct PrimaryKeyDefinition(Vec<usize>);

impl PrimaryKeyDefinition {
    pub fn new(fields: Vec<usize>) -> Self {
        PrimaryKeyDefinition(fields)
    }
}

impl Deref for PrimaryKeyDefinition {
    type Target = Vec<usize>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct PrimaryKey<'a>(Vec<&'a Type>);

impl<'a> PrimaryKey<'a> {
    pub fn hash(&self) -> BigUint {
        let mut hash_value = 0.to_biguint().unwrap();

        for ty in self {
            hash_value <<= std::mem::size_of::<u64>() * 8;
            let mut hasher = DefaultHasher::new();
            ty.hash(&mut hasher);
            let single_hashed = hasher.finish().to_biguint().unwrap();
            hash_value |= single_hashed;
        }
        hash_value
    }
}

impl<'a> PrimaryKey<'a> {
    pub fn new(attributes: Vec<&'a Type>) -> Self {
        PrimaryKey(attributes)
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

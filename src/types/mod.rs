//! This module defines the traits for a type and a few of the primitive types
//! Types will implement the [`DBType`] trait to allow for extendability
//!
//! [`DBType`]: trait.DBType.html



use std::hash::Hash;
use std::ops::{RangeInclusive};

pub mod integer;

pub trait DBType : Hash + Eq + Ord + Sized {

    fn min(&self) -> Self;
    fn max(&self) -> Self;

    fn range(&self) -> RangeInclusive<Self> {
        RangeInclusive::new(self.min(), self.max())
    }
}


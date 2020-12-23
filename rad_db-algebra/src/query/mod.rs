use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};

pub mod conditions;
pub mod query_iterator;
pub mod query_node;
pub mod query_result;
pub mod optimization;

/// An object that can be turned into an iterator multiple times
pub trait Repeatable {
    type Item;
    type IntoIter: Iterator<Item = Self::Item>;

    fn get_iterator(&self) -> Self::IntoIter;
}

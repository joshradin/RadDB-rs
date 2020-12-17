use std::ops::{Deref, DerefMut};

use rad_db_types::Type;
use std::error::Error;
use std::str::FromStr;

/// Represents a single row within a database.
/// A tuple knows no information about itself besides its contents
#[derive(Debug)]
pub struct Tuple(Vec<Type>);

impl Tuple {
    pub fn new<I: Iterator<Item = Type>>(values: I) -> Self {
        Tuple(values.collect())
    }
}

impl Deref for Tuple {
    type Target = Vec<Type>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Tuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl IntoIterator for Tuple {
    type Item = Type;
    type IntoIter = <Vec<Type> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

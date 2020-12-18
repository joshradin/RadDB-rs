use std::ops::{Deref, DerefMut};

use rad_db_types::serialization::serialize_values;
use rad_db_types::Type;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::iter::FromIterator;
use std::str::FromStr;

/// Represents a single row within a database.
/// A tuple knows no information about itself besides its contents
#[derive(Debug, Clone)]
pub struct Tuple(Vec<Type>);

impl Tuple {
    pub fn new<I: IntoIterator<Item = Type>>(values: I) -> Self {
        Tuple(values.into_iter().collect())
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

impl<'a> IntoIterator for &'a Tuple {
    type Item = &'a Type;
    type IntoIter = <&'a Vec<Type> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl FromIterator<Type> for Tuple {
    fn from_iter<T: IntoIterator<Item = Type>>(iter: T) -> Self {
        Self::new(iter)
    }
}

impl<'a> FromIterator<&'a Type> for Tuple {
    fn from_iter<T: IntoIterator<Item = &'a Type>>(iter: T) -> Self {
        Self::new(iter.into_iter().cloned())
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serialize_values(self.clone()))
    }
}

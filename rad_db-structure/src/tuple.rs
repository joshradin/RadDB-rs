use std::error::Error;
use std::fmt::{Display, Formatter};
use std::iter::FromIterator;
use std::ops::{
    Add, Deref, DerefMut, Index, IndexMut, Range, RangeFrom, RangeFull, RangeInclusive, RangeTo,
    RangeToInclusive, Sub,
};
use std::str::FromStr;

use rad_db_types::serialization::serialize_values;
use rad_db_types::{Type, Value};
use std::cmp::Reverse;

/// Represents a single row within a database.
/// A tuple knows no information about itself besides its contents
#[derive(Debug, Clone, PartialEq)]
pub struct Tuple(Vec<Type>);

impl Tuple {
    pub fn new<I: IntoIterator<Item = Type>>(values: I) -> Self {
        Tuple(values.into_iter().collect())
    }

    pub fn concat(self, other: Self) -> Self {
        let mut backing = self.0;
        backing.extend(other);
        Self(backing)
    }

    pub fn remove_at_indexes<I: IntoIterator<Item = usize>>(self, indexes: I) -> Self {
        let mut removal: Vec<usize> = indexes.into_iter().collect();
        removal.sort_by_key(|u| Reverse(*u));
        let mut values = self.0;
        for remove in removal {
            values.remove(remove);
        }
        Tuple(values)
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

impl Add for Tuple {
    type Output = Tuple;

    fn add(self, rhs: Self) -> Self::Output {
        self.concat(rhs)
    }
}

impl Add<&Tuple> for Tuple {
    type Output = Tuple;

    fn add(self, rhs: &Tuple) -> Self::Output {
        self.concat(rhs.clone())
    }
}

impl Add<Tuple> for &Tuple {
    type Output = Tuple;

    fn add(self, rhs: Tuple) -> Self::Output {
        self.clone().concat(rhs)
    }
}

impl Add<&Tuple> for &Tuple {
    type Output = Tuple;

    fn add(self, rhs: &Tuple) -> Self::Output {
        self.clone().concat(rhs.clone())
    }
}

impl Index<usize> for Tuple {
    type Output = Value;

    fn index(&self, index: usize) -> &Self::Output {
        self.get(index).unwrap()
    }
}

impl IndexMut<usize> for Tuple {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.get_mut(index).unwrap()
    }
}

impl Index<Range<usize>> for Tuple {
    type Output = [Type];

    fn index(&self, index: Range<usize>) -> &Self::Output {
        self.0.get(index).unwrap()
    }
}

impl Index<RangeFrom<usize>> for Tuple {
    type Output = [Type];

    fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
        self.0.get(index).unwrap()
    }
}

impl Index<RangeTo<usize>> for Tuple {
    type Output = [Type];

    fn index(&self, index: RangeTo<usize>) -> &Self::Output {
        self.0.get(index).unwrap()
    }
}

impl Index<RangeInclusive<usize>> for Tuple {
    type Output = [Type];

    fn index(&self, index: RangeInclusive<usize>) -> &Self::Output {
        self.0.get(index).unwrap()
    }
}

impl Index<RangeToInclusive<usize>> for Tuple {
    type Output = [Type];

    fn index(&self, index: RangeToInclusive<usize>) -> &Self::Output {
        self.0.get(index).unwrap()
    }
}

impl Index<RangeFull> for Tuple {
    type Output = [Type];

    fn index(&self, index: RangeFull) -> &Self::Output {
        self.0.get(index).unwrap()
    }
}

impl Sub<&[usize]> for Tuple {
    type Output = Tuple;

    fn sub(self, rhs: &[usize]) -> Self::Output {
        let mut removal: Vec<usize> = <[usize]>::to_vec(&Box::new(rhs));
        removal.sort_by_key(|u| Reverse(*u));
        let mut values = self.0;
        for remove in removal {
            values.remove(remove);
        }
        Tuple(values)
    }
}

impl Sub<&[usize]> for &Tuple {
    type Output = Tuple;

    fn sub(self, rhs: &[usize]) -> Self::Output {
        self.clone() - rhs
    }
}

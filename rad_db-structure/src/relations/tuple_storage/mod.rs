use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use crate::key::primary::{PrimaryKey, PrimaryKeyDefinition};
use crate::tuple::Tuple;

mod block;


/// When a tuple couldn't be inserted for some reason
#[derive(Debug)]
pub enum TupleInsertionError {
    PrimaryKeyPresent,
    IncorrectTypes(Vec<usize>),
}

impl Display for TupleInsertionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TupleInsertionError::PrimaryKeyPresent => {
                write!(f, "Couldn't insert tuple, primary key already present")
            }
            TupleInsertionError::IncorrectTypes(vec) => {
                write!(f, "Invalid types at indexes {:?}", vec)
            }
        }
    }
}

impl Error for TupleInsertionError {}

pub type InsertionResult<T> = Result<T, TupleInsertionError>;

pub struct TupleStorage {
    primary_key_definition: PrimaryKeyDefinition,
    len: usize,
}


impl TupleStorage {
    pub fn new(primary_key_definition: PrimaryKeyDefinition) -> Self {
        Self {
            primary_key_definition,
            len: 0
        }
    }

    /// Insert an entire tuple into the storage medium
    fn insert(&mut self, tuple: Tuple) -> InsertionResult<()> {
        unimplemented!()
    }
    fn remove(&mut self, primary_key: PrimaryKey<'_>) -> Result<Tuple, ()> {
        unimplemented!()
    }

    fn find_by_primary(&self, primary_key: PrimaryKey<'_>) -> Result<&Tuple, ()> {
        unimplemented!()
    }
    fn all_tuples(&self) -> Vec<&Tuple> {
        unimplemented!()
    }

    fn hash_tuple(&self, tuple: &Tuple) -> u64 {
        let primary_key = self.get_primary_key_of_tuple(tuple);
        let mut hasher = DefaultHasher::new();
        primary_key.hash(&mut hasher);
        hasher.finish()
    }

    fn get_primary_key_definition(&self) -> &PrimaryKeyDefinition;

    fn get_primary_key_of_tuple<'a>(&self, tuple: &'a Tuple) -> PrimaryKey<'a> {
        let definition = self.get_primary_key_definition();
        let ret = tuple
            .iter()
            .enumerate()
            .filter(|(pos, _)| definition.contains(pos))
            .map(|(_, val)| val)
            .collect();
        PrimaryKey::new(ret)
    }

    fn len(&self) -> usize {

    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}


use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use crate::identifier::Identifier;
use crate::key::primary::{PrimaryKey, PrimaryKeyDefinition};
use crate::relations::tuple_storage::extendible_hashing::BlockDirectory;
use crate::relations::RelationDefinition;
use crate::tuple::Tuple;
use num_bigint::BigUint;
use std::collections::HashMap;

mod block;
mod extendible_hashing;
mod lock;

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
    identifier: Identifier,
    relation: RelationDefinition,
    primary_key_definition: PrimaryKeyDefinition,
    len: usize,
    true_storage: BlockDirectory,
}

impl TupleStorage {
    pub fn new(
        identifier: Identifier,
        relation: RelationDefinition,
        primary_key_definition: PrimaryKeyDefinition,
    ) -> Self {
        let mut storage = Self {
            identifier: identifier.clone(),
            relation: relation.clone(),
            primary_key_definition: primary_key_definition.clone(),
            len: 0,
            true_storage: BlockDirectory::new(identifier, relation, 4096, primary_key_definition),
        };

        storage
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

    fn hash_tuple(&self, tuple: &Tuple) -> BigUint {
        let primary_key = self.get_primary_key_of_tuple(tuple);
        primary_key.hash()
    }

    fn get_primary_key_definition(&self) -> &PrimaryKeyDefinition {
        &self.primary_key_definition
    }

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
        self.len
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use num_bigint::BigUint;

pub use extendible_hashing::{BlockIterator, StoredTupleIterator};

use crate::identifier::Identifier;
use crate::key::primary::{PrimaryKey, PrimaryKeyDefinition};
use crate::relations::tuple_storage::extendible_hashing::BlockDirectory;
use crate::relations::RelationDefinition;
use crate::tuple::Tuple;
use crate::Rename;

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

#[derive(Debug)]
pub struct TupleStorage {
    identifier: Identifier,
    relation: RelationDefinition,
    primary_key_definition: PrimaryKeyDefinition,
    true_storage: BlockDirectory,
}

impl TupleStorage {
    pub fn new(
        identifier: Identifier,
        relation: RelationDefinition,
        primary_key_definition: PrimaryKeyDefinition,
        max_size: usize,
    ) -> Self {
        Self {
            identifier: identifier.clone(),
            relation: relation.clone(),
            primary_key_definition: primary_key_definition.clone(),
            true_storage: BlockDirectory::new(
                identifier,
                relation,
                max_size,
                primary_key_definition,
            ),
        }
    }

    pub fn to_skeleton(&self) -> Self {
        Self::new(
            self.identifier.clone(),
            self.relation.clone(),
            self.primary_key_definition.clone(),
            self.true_storage.bucket_size(),
        )
    }

    /// Insert an entire tuple into the storage medium
    pub fn insert(&mut self, tuple: Tuple) -> InsertionResult<Option<Tuple>> {
        let hash = self.hash_tuple(&tuple);
        let result = Ok(self.true_storage.insert(tuple, hash));
        //println!("{:#?}", self.true_storage);
        result
    }
    pub fn remove(&mut self, primary_key: PrimaryKey<'_>) -> Result<Tuple, ()> {
        unimplemented!()
    }

    pub fn find_by_primary(&self, primary_key: PrimaryKey<'_>) -> Result<&Tuple, ()> {
        unimplemented!()
    }
    /// Gets a [StoredTupleIterator] for the tuple storage
    ///
    /// [StoredTupleIterator]: StoredTupleIterator
    pub fn all_tuples(&self) -> StoredTupleIterator {
        (&self.true_storage).into_iter()
    }

    /// Gets a [BlockIterator] for the tuple storage
    ///
    /// [BlockIterator]: self::BlockIterator
    pub fn blocks(&self) -> BlockIterator {
        (&self.true_storage).blocks()
    }

    pub fn hash_tuple(&self, tuple: &Tuple) -> BigUint {
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
        PrimaryKey::new(ret, definition.create_seeds())
    }

    pub(crate) fn len(&self) -> usize {
        self.true_storage.len()
    }
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Rename<Identifier> for TupleStorage {
    fn rename(&mut self, name: Identifier) {
        self.identifier = name.clone();
        self.true_storage.rename(name);
    }
}

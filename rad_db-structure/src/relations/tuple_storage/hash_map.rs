use std::collections::HashMap;

use crate::key::primary::{PrimaryKey, PrimaryKeyDefinition};
use crate::relations::tuple_storage::{InsertionResult, TupleInsertionError, TupleStorage};
use crate::tuple::Tuple;

pub struct HashTable {
    primary_key_definition: PrimaryKeyDefinition,
    backing: HashMap<u64, Tuple>,
}

impl TupleStorage for HashTable {
    fn new(primary_key_definition: PrimaryKeyDefinition) -> Self
    where
        Self: Sized,
    {
        Self {
            primary_key_definition,
            backing: Default::default(),
        }
    }

    fn insert(&mut self, tuple: Tuple) -> InsertionResult<()> {
        let hash = self.hash_tuple(&tuple);
        if self.backing.contains_key(&hash) {
            Err(TupleInsertionError::PrimaryKeyPresent)
        } else {
            self.backing.insert(hash, tuple);
            Ok(())
        }
    }

    fn remove(&mut self, primary_key: PrimaryKey<'_>) -> Result<Tuple, ()> {
        let hash = primary_key.default_hash();
        self.backing.remove(&hash).ok_or(())
    }

    fn find_by_primary(&self, primary_key: PrimaryKey<'_>) -> Result<&Tuple, ()> {
        let hash = primary_key.default_hash();
        self.backing.get(&hash).ok_or(())
    }

    fn all_tuples(&self) -> Vec<&Tuple> {
        self.backing.values().collect()
    }

    fn get_primary_key_definition(&self) -> &PrimaryKeyDefinition {
        &self.primary_key_definition
    }

    fn len(&self) -> usize {
        self.backing.len()
    }
}

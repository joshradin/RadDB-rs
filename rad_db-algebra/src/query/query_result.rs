use crate::query::Repeatable;
use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::RelationDefinition;
use rad_db_structure::tuple::Tuple;
use rad_db_types::{Type, Value};
use std::collections::HashMap;
use std::iter::FromIterator;

enum InternalIterator {
    Tuple(Box<dyn Iterator<Item = Tuple>>),
    Block(Box<dyn Iterator<Item = Vec<Tuple>>>),
    RepeatableTuple(Box<dyn Repeatable<Item = Tuple>>),
    RepeatableBlock(Box<dyn Repeatable<Item = Vec<Tuple>>>),
}

pub struct QueryResult {
    relation: Vec<(Identifier, Type)>,
    internal: InternalIterator,
}
const ITEMS_PER_BLOCK: usize = 16;
impl QueryResult {
    pub fn with_tuples<I: IntoIterator<Item = Tuple> + 'static>(
        relation: Vec<(Identifier, Type)>,
        tuples: I,
    ) -> Self {
        QueryResult {
            relation,
            internal: InternalIterator::Tuple(Box::new(tuples.into_iter())),
        }
    }

    pub fn with_blocks<I: IntoIterator<Item = Vec<Tuple>> + 'static>(
        relation: Vec<(Identifier, Type)>,
        tuples: I,
    ) -> Self {
        QueryResult {
            relation,
            internal: InternalIterator::Block(Box::new(tuples.into_iter())),
        }
    }

    pub fn relation(&self) -> &Vec<(Identifier, Type)> {
        &self.relation
    }

    /// Converts the result into an iterator of tuples
    pub fn tuples(self) -> Box<dyn Iterator<Item = Tuple>> {
        match self.internal {
            InternalIterator::Tuple(i) => Box::new(i),
            InternalIterator::Block(i) => Box::new(i.flatten()),
            InternalIterator::RepeatableTuple(i) => i.get_iterator(),
            InternalIterator::RepeatableBlock(i) => Box::new(i.get_iterator().flatten()),
        }
    }

    /// Attempts to get an iterator of tuples without consuming itself
    pub fn repeatable_tuples(&self) -> Option<Box<dyn Iterator<Item = Tuple>>> {
        match &self.internal {
            InternalIterator::Tuple(_) | InternalIterator::Block(_) => None,
            InternalIterator::RepeatableTuple(i) => Some(i.get_iterator()),
            InternalIterator::RepeatableBlock(i) => Some(Box::new(i.get_iterator().flatten())),
        }
    }

    /// Converts the result into an iterator of blocks of tuples
    pub fn blocks(self) -> Box<dyn Iterator<Item = Vec<Tuple>>> {
        match self.internal {
            InternalIterator::Tuple(i) => {
                let mut ret = Vec::new();
                let collected = i.collect::<Vec<_>>();
                let mut current = Vec::new();

                let mut iterator = collected.into_iter();
                while let Some(tuple) = iterator.next() {
                    current.push(tuple);
                    if current.len() >= ITEMS_PER_BLOCK {
                        ret.push(current);
                        current = vec![];
                    }
                }
                if !current.is_empty() {
                    ret.push(current);
                }
                Box::new(ret.into_iter())
            }
            InternalIterator::Block(i) => Box::new(i.into_iter()),
            InternalIterator::RepeatableTuple(i) => {
                let mut ret = Vec::new();
                let collected = i.get_iterator().collect::<Vec<_>>();
                let mut current = Vec::new();

                let mut iterator = collected.into_iter();
                while let Some(tuple) = iterator.next() {
                    current.push(tuple);
                    if current.len() >= ITEMS_PER_BLOCK {
                        ret.push(current);
                        current = vec![];
                    }
                }
                if !current.is_empty() {
                    ret.push(current);
                }
                Box::new(ret.into_iter())
            }
            InternalIterator::RepeatableBlock(i) => i.get_iterator(),
        }
    }

    /// Tries to get an iterator of blocks of tuples without consuming the result
    pub fn repeatable_blocks(&self) -> Option<Box<dyn Iterator<Item = Vec<Tuple>>>> {
        match &self.internal {
            InternalIterator::Tuple(_) | InternalIterator::Block(_) => None,
            InternalIterator::RepeatableTuple(i) => {
                let mut ret = Vec::new();
                let collected = i.get_iterator().collect::<Vec<_>>();
                let mut current = Vec::new();

                let mut iterator = collected.into_iter();
                while let Some(tuple) = iterator.next() {
                    current.push(tuple);
                    if current.len() >= ITEMS_PER_BLOCK {
                        ret.push(current);
                        current = vec![];
                    }
                }
                if !current.is_empty() {
                    ret.push(current);
                }
                Some(Box::new(ret.into_iter()))
            }
            InternalIterator::RepeatableBlock(i) => Some(i.get_iterator()),
        }
    }

    /// Gets the index in a tuple of this identifier
    pub(crate) fn get_value_in_tuple<'a>(
        &self,
        id: &Identifier,
        tuple: &'a Tuple,
    ) -> Option<&'a Value> {
        let position = self.relation.iter().position(|(rel_id, _)| rel_id == id);
        match position {
            None => None,
            Some(position) => tuple.get(position),
        }
    }

    /// Gets the identifier to index mapping of this tuple
    pub fn identifier_mappings(&self) -> HashMap<Identifier, usize> {
        self.relation
            .iter()
            .enumerate()
            .map(|(index, id)| (id.0.clone(), index))
            .collect()
    }
}

impl IntoIterator for QueryResult {
    type Item = Tuple;
    type IntoIter = <Vec<Tuple> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        Vec::from_iter(self.tuples()).into_iter()
    }
}

impl IntoIterator for &mut QueryResult {
    type Item = Tuple;
    type IntoIter = <Vec<Tuple> as IntoIterator>::IntoIter;

    /// If the iterator is not naturally repeatable, this can be very expensive
    fn into_iter(self) -> Self::IntoIter {
        if let Some(repeatable) = self.repeatable_tuples() {
            let ret: Vec<_> = repeatable.collect();
            ret.into_iter()
        } else {
            // Expensive operation
            match &mut self.internal {
                InternalIterator::Tuple(tuples) => {
                    let mut replaced =
                        std::mem::replace(tuples, Box::new(Vec::<Tuple>::new().into_iter()));
                    let saved_tuples: Vec<_> = replaced.collect();
                    let output: Vec<_> = saved_tuples.iter().cloned().collect();
                    std::mem::replace(tuples, Box::new(saved_tuples.into_iter()));
                    output.into_iter()
                }
                InternalIterator::Block(tuples) => {
                    let mut replaced =
                        std::mem::replace(tuples, Box::new(Vec::<Vec<Tuple>>::new().into_iter()));
                    let saved_tuples: Vec<_> = replaced.collect();
                    let output: Vec<_> = saved_tuples.iter().cloned().collect();
                    std::mem::replace(tuples, Box::new(saved_tuples.into_iter()));
                    let flattened: Vec<_> = output.into_iter().flatten().collect();
                    flattened.into_iter()
                }
                _ => unreachable!(),
            }
        }
    }
}

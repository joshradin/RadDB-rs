use crate::query::query_iterator::{QueryIterator, ReferencedQueryIterator};
use crate::query::query_node::Source;
use crate::query::Repeatable;
use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::tuple_storage::BlockIterator;
use rad_db_structure::relations::RelationDefinition;
use rad_db_structure::tuple::Tuple;
use rad_db_types::{Type, Value};
use std::collections::HashMap;
use std::iter::FromIterator;
use std::marker::PhantomData;

pub enum QueryResultFullData<'a> {
    Tuples(Vec<Tuple>),
    BlockData(QueryResultBlocks<'a>),
}

pub enum QueryResultBlocks<'a> {
    Blocks(Vec<Vec<Tuple>>),
    Source(Source<'a>),
}

pub struct QueryResult<'a> {
    relation: Vec<(Identifier, Type)>,
    internal: QueryResultFullData<'a>,
    total_created_tuples: usize,
}
const ITEMS_PER_BLOCK: usize = 16;
impl<'a> QueryResult<'a> {
    pub fn with_tuples<I: IntoIterator<Item = Tuple>>(
        relation: Vec<(Identifier, Type)>,
        tuples: I,
        extra: usize,
    ) -> Self {
        let vec: Vec<_> = tuples.into_iter().collect();
        let len = vec.len();
        QueryResult {
            relation,
            internal: QueryResultFullData::Tuples(vec),
            total_created_tuples: len + extra,
        }
    }

    pub fn from_source(relation: Vec<(Identifier, Type)>, source: Source<'a>) -> Self {
        let len = source.source_len();
        QueryResult {
            relation,
            internal: QueryResultFullData::BlockData(QueryResultBlocks::Source(source)),
            total_created_tuples: len,
        }
    }

    pub fn relation(&self) -> &Vec<(Identifier, Type)> {
        &self.relation
    }

    /// Converts the result into an iterator of tuples
    pub fn tuples(self) -> QueryResultFullData<'a> {
        self.internal
    }

    /// Attempts to get an iterator of tuples without consuming itself
    pub fn repeatable_tuples(&mut self) -> impl Iterator<Item = Tuple> {
        if let QueryResultFullData::BlockData(_) = &self.internal {
            let old = std::mem::replace(&mut self.internal, QueryResultFullData::Tuples(vec![]));

            if let QueryResultFullData::BlockData(source) = old {
                if let QueryResultFullData::Tuples(new_vec) = &mut self.internal {
                    for tuple in source.into_iter().flatten() {
                        new_vec.push(tuple);
                    }
                }
            }
        }

        if let QueryResultFullData::Tuples(vector) = &self.internal {
            vector.clone().into_iter()
        } else {
            unreachable!()
        }
    }

    /// Converts the result into an iterator of blocks of tuples
    pub fn blocks(self) -> QueryResultBlocks<'a> {
        match self.internal {
            QueryResultFullData::Tuples(s) => {
                let mut ret = Vec::new();
                let mut current = Vec::new();

                let mut iterator = s.into_iter();
                while let Some(tuple) = iterator.next() {
                    current.push(tuple.clone());
                    if current.len() >= ITEMS_PER_BLOCK {
                        ret.push(current);
                        current = vec![];
                    }
                }
                if !current.is_empty() {
                    ret.push(current);
                }
                QueryResultBlocks::Blocks(ret)
            }
            QueryResultFullData::BlockData(b) => b,
        }
    }

    /// Tries to get an iterator of blocks of tuples without consuming the result
    pub fn repeatable_blocks(&self) -> Option<BlockIterator> {
        match &self.internal {
            QueryResultFullData::BlockData(b) => match b {
                QueryResultBlocks::Blocks(_) => None,
                QueryResultBlocks::Source(s) => Some(s.get_iterator()),
            },
            _ => None,
        }
    }

    /// Gets the index in a tuple of this identifier
    pub(crate) fn get_value_in_tuple<'b>(
        &self,
        id: &Identifier,
        tuple: &'b Tuple,
    ) -> Option<&'b Value> {
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

    pub fn total_created_tuples(&self) -> usize {
        self.total_created_tuples
    }
}

impl<'a> Iterator for QueryResultBlocks<'a> {
    type Item = Vec<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            QueryResultBlocks::Blocks(blocks) => blocks.pop(),
            QueryResultBlocks::Source(source) => source.next(),
        }
    }
}

impl<'a> IntoIterator for QueryResultFullData<'a> {
    type Item = Tuple;
    type IntoIter = QueryIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        QueryIterator::new(self)
    }
}

impl<'a> IntoIterator for QueryResult<'a> {
    type Item = Tuple;
    type IntoIter = QueryIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        QueryIterator::new(self.internal)
    }
}

impl<'a> IntoIterator for &'a QueryResult<'a> {
    type Item = Tuple;
    type IntoIter = ReferencedQueryIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ReferencedQueryIterator::new(&self.internal)
    }
}

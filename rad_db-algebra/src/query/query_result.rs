use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::RelationDefinition;
use rad_db_structure::tuple::Tuple;
use std::iter::FromIterator;
use rad_db_types::{Type, Value};

enum InternalIterator {
    Tuple(Box<dyn Iterator<Item = Tuple>>),
    Block(Box<dyn Iterator<Item = Vec<Tuple>>>),
}

pub struct QueryResult {
    relation: Vec<(Identifier, Type)>,
    internal: InternalIterator,
}

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
    pub fn tuples(self) -> Box<dyn Iterator<Item=Tuple>> {
        match self.internal {
            InternalIterator::Tuple(i) => Box::new(i),
            InternalIterator::Block(i) => Box::new(i.flatten()),
        }
    }
    pub fn blocks(self) -> Box<dyn Iterator<Item=Vec<Tuple>>> {
      match self.internal {
        InternalIterator::Tuple(i) => {
            let mut ret = Vec::new();
            let collected = i.collect::<Vec<_>>();
            let mut current = Vec::new();
            const items_per_block: usize = 16;
            let mut iterator = collected.into_iter();
            while let Some(tuple) = iterator.next() {
                current.push(tuple);
                if current.len() >= items_per_block {
                    ret.push(current);
                    current = vec![];
                }
            }
            if !current.is_empty() {
                ret.push(current);
            }
           Box::new(ret.into_iter())
        },
        InternalIterator::Block(i) => Box::new(i.into_iter()),
       }
    }
    

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
}


impl IntoIterator for QueryResult {
    type Item = Tuple;
    type IntoIter = <Vec<Tuple> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        Vec::from_iter(self.tuples()).into_iter()
    }
}

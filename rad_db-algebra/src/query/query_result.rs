use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::RelationDefinition;
use rad_db_structure::tuple::Tuple;
use rad_db_types::{Type, Value};

pub struct QueryResult {
    relation: Vec<(Identifier, Type)>,
    tuples: Vec<Tuple>,
}

impl QueryResult {
    pub fn new(relation: Vec<(Identifier, Type)>, tuples: Vec<Tuple>) -> Self {
        QueryResult { relation, tuples }
    }

    pub fn relation(&self) -> &Vec<(Identifier, Type)> {
        &self.relation
    }
    pub fn tuples(&self) -> &Vec<Tuple> {
        &self.tuples
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

impl IntoIterator for &QueryResult {
    type Item = Tuple;
    type IntoIter = <Vec<Tuple> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.tuples.clone().into_iter()
    }
}

impl IntoIterator for QueryResult {
    type Item = Tuple;
    type IntoIter = <Vec<Tuple> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.tuples.into_iter()
    }
}

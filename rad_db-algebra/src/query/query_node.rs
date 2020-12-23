use crate::query::conditions::{JoinCondition, Condition};
use crate::query::query_iterator::QueryIterator;
use crate::query::query_result::QueryResult;
use crate::query::Repeatable;
use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::tuple_storage::{BlockIterator, StoredTupleIterator};
use rad_db_structure::relations::Relation;
use rad_db_structure::tuple::Tuple;
use rad_db_types::{Type, Value};
use std::collections::HashMap;

pub enum Projection {
    Flat(Identifier),
    Renamed(Identifier, String),
}

pub struct Crawler<'a> {
    source: &'a Relation,
    iterator: Option<BlockIterator<'a>>,
}

impl<'a> Crawler<'a> {
    pub fn new(source: &'a Relation) -> Self {
        Crawler {
            source,
            iterator: None,
        }
    }
}

impl<'a> Iterator for Crawler<'a> {
    type Item = Vec<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iterator.is_none() {
            self.iterator = Some(self.source.blocks());
        }

        self.iterator.as_mut().unwrap().next()
    }
}

pub enum Source<'a> {
    Flat(Crawler<'a>),
    Renamed(Crawler<'a>, String),
}

impl<'a> Source<'a> {
    pub fn source_len(&self) -> usize {
        match self {
            Source::Flat(c) => c.source.len(),
            Source::Renamed(c, _) => c.source.len(),
        }
    }
}

impl<'a> Repeatable for Source<'a> {
    type Item = Vec<Tuple>;
    type IntoIter = BlockIterator<'a>;

    fn get_iterator(&self) -> Self::IntoIter {
        match self {
            Source::Flat(c) => c.source.blocks(),
            Source::Renamed(c, _) => c.source.blocks(),
        }
    }
}

impl Iterator for Source<'_> {
    type Item = Vec<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Source::Flat(c) => c.next(),
            Source::Renamed(c, _) => c.next(),
        }
    }
}

pub enum Query<'a> {
    Source(Source<'a>),
    Projection(Vec<Projection>),
    Selection(Condition),
    CrossProduct,
    InnerJoin(JoinCondition),
    LeftJoin(JoinCondition),
    RightJoin(JoinCondition),
    NaturalJoin,
}

pub enum QueryChildren<'a> {
    None,
    One(QueryNode<'a>),
    Two(QueryNode<'a>, QueryNode<'a>),
}

pub struct QueryNode<'a> {
    query: Query<'a>,
    children: Box<QueryChildren<'a>>,
    resulting_relation: Vec<(Identifier, Type)>,
    mapping: HashMap<Identifier, Identifier>,
}

impl<'a> QueryNode<'a> {
    pub fn source(relation: &'a Relation) -> Self {
        let mapping = relation
            .attributes()
            .iter()
            .map(|(id, _)| {
                let identifier = Identifier::new(id);
                (identifier.clone(), identifier)
            })
            .collect();
        Self {
            query: Query::Source(Source::Flat(Crawler::new(relation))),
            children: Box::new(QueryChildren::None),
            resulting_relation: relation
                .attributes()
                .iter()
                .map(|(id, val)| (Identifier::new(id), val.clone()))
                .collect(),
            mapping,
        }
    }

    pub fn source_with_name(relation: &'a Relation, name: String) -> Self {
        let mapping = relation
            .attributes()
            .iter()
            .map(|(id, _)| {
                let old_identifier = Identifier::new(id);
                let new_identifier = Identifier::concat(&name, id);
                (old_identifier, new_identifier)
            })
            .collect();
        Self {
            query: Query::Source(Source::Renamed(Crawler::new(relation), name.clone())),
            children: Box::new(QueryChildren::None),
            resulting_relation: relation
                .attributes()
                .iter()
                .map(|(id, val)| (Identifier::concat(&name, id), val.clone()))
                .collect(),
            mapping,
        }
    }

    pub fn inner_join(left: QueryNode<'a>, right: QueryNode<'a>, condition: JoinCondition) -> Self {
        let mut result = Vec::new();
        result.extend(left.resulting_relation.iter().cloned());
        result.extend(right.resulting_relation.iter().cloned());
        let mapping = result
            .iter()
            .map(|(id, _)| (id.clone(), id.clone()))
            .collect();

        QueryNode {
            query: Query::InnerJoin(condition),
            children: Box::new(QueryChildren::Two(left, right)),
            resulting_relation: result,
            mapping: mapping,
        }
    }

    pub fn cross_product(left: QueryNode<'a>, right: QueryNode<'a>) -> Self {
        let mut result = Vec::new();
        result.extend(left.resulting_relation.iter().cloned());
        result.extend(right.resulting_relation.iter().cloned());
        let mapping = result
            .iter()
            .map(|(id, _)| (id.clone(), id.clone()))
            .collect();

        QueryNode {
            query: Query::CrossProduct,
            children: Box::new(QueryChildren::Two(left, right)),
            resulting_relation: result,
            mapping: mapping,
        }
    }

    pub fn optimize_query(&mut self) {}

    pub fn optimized(mut self) -> Self {
        self.optimize_query();
        self
    }

    pub fn execute_query<'q>(self) -> QueryResult<'q>
    where
        'a: 'q,
    {
        let mut output_tuples: Vec<Tuple> = vec![];
        let relation = self.resulting_relation.clone();
        let mut extra = 0;

        match (self.query, *self.children) {
            (Query::Source(source), QueryChildren::None) => {
                let inner = QueryResult::from_source(relation, source);
                return inner;
            }
            (Query::InnerJoin(join), QueryChildren::Two(left, right)) => {
                let left_id = &self.mapping[join.left_id()]; // the name of the left id in the left result
                let right_id = &self.mapping[join.right_id()]; // the name of the right id in the right result

                let left = left.execute_query();
                let right = right.execute_query();

                extra += left.total_created_tuples() + right.total_created_tuples();

                let left_mappings = left.identifier_mappings();
                let right_mappings = right.identifier_mappings();

                let left_index = left_mappings[left_id];
                let right_index = right_mappings[right_id];

                if right.repeatable_blocks().is_some() {
                    let left_blocks = left.blocks();
                    for left_block in left_blocks {
                        let right_blocks = right.repeatable_blocks().unwrap();
                        for right_block in right_blocks {
                            for left_tuple in &left_block {
                                for right_tuple in &right_block {
                                    if left_tuple[left_index] == right_tuple[right_index] {
                                        output_tuples.push(left_tuple + right_tuple);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    let mut right = right;
                    for left_tuple in left {
                        for right_tuple in &right {
                            if left_tuple[left_index] == right_tuple[right_index] {
                                output_tuples.push(&left_tuple + right_tuple);
                            }
                        }
                    }
                }
            }
            (Query::CrossProduct, QueryChildren::Two(left, right)) => {
                let left = left.execute_query();
                let right = right.execute_query();

                extra += left.total_created_tuples() + right.total_created_tuples();

                if right.repeatable_blocks().is_some() {
                    let left_blocks = left.blocks();
                    for left_block in left_blocks {
                        let right_blocks = right.repeatable_blocks().unwrap();
                        for right_block in right_blocks {
                            for left_tuple in &left_block {
                                for right_tuple in &right_block {
                                    output_tuples.push(left_tuple + right_tuple);
                                }
                            }
                        }
                    }
                } else {
                    let mut right = right;
                    for left_tuple in left {
                        for right_tuple in &right {
                            output_tuples.push(&left_tuple + right_tuple);
                        }
                    }
                }
            }
            _ => panic!("Invalid query"),
        }

        QueryResult::with_tuples(relation, &mut output_tuples.into_iter(), extra)
    }
}

#[cfg(test)]
mod join_tests {
    use super::*;
    use rad_db_structure::key::primary::PrimaryKeyDefinition;
    use rad_db_structure::relations::Relation;
    use std::iter::FromIterator;

    #[test]
    fn cross_product() {
        let mut relation1 = Relation::new_volatile(
            Identifier::new("test1"),
            vec![("field1", Type::from(0u64))],
            64,
            PrimaryKeyDefinition::new(vec![0]),
        );
        for i in 0..100u64 {
            //println!("Inserting tuple {}", i);
            relation1.insert(Tuple::from_iter(&[Value::from(i)]));
        }
        let mut relation2 = Relation::new_volatile(
            Identifier::new("test2"),
            vec![("field1", Type::from(0u64))],
            64,
            PrimaryKeyDefinition::new(vec![0]),
        );
        for i in 0..100u64 {
            //println!("Inserting tuple {}", i);
            relation2.insert(Tuple::from_iter(&[Value::from(i)]));
        }

        let mut query_node =
            QueryNode::cross_product(QueryNode::source(&relation1), QueryNode::source(&relation2));
        let result = query_node.execute_query();
        let resulting_tuples: Vec<Tuple> = result.tuples().into_iter().collect();
        assert_eq!(resulting_tuples.len(), 100 * 100);
        for i in 0..100u64 {
            for j in 0..100u64 {
                resulting_tuples.contains(&Tuple::from_iter(&[Value::from(i), Value::from(j)]));
            }
        }
    }
}

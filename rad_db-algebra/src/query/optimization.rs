use crate::query::query_node::{QueryNode, QueryChildren, Source};
use crate::query::query_node::Query;
use std::collections::HashMap;
use rad_db_structure::identifier::Identifier;
use rad_db_types::Value;
use rad_db_structure::relations::Relation;
use crate::error::MissingFieldError;
use rand::seq::IteratorRandom;


pub struct Optimizer<'a, 'q> where 'q : 'a {
    query_node: &'a mut QueryNode<'q>,
    start_tuples: usize,
    /// A sample of some amount of random values of the relevant fields in selections
    samples: HashMap<Identifier, Vec<Value>>
}

fn sample_field(field: &Identifier, source: &Relation, samples: usize) -> Result<Vec<Value>, MissingFieldError> {
    let field_index = source.get_field_index(field);
    if field_index.is_none() {
        return Err(MissingFieldError::new(field.clone()));
    }
    let field_index =field_index.unwrap();
    let tuples = source.tuples();
    let mut random = rand::thread_rng();
    let sampled_tuples = tuples.choose_multiple(&mut random, samples);
    let samples_values: Vec<_> = sampled_tuples.into_iter()
        .map(|tuple| tuple.take(field_index) )
        .collect();
    Ok(samples_values)
}

impl<'a, 'query> Optimizer<'a, 'query> where 'query : 'a {
    pub fn new(query: &'a mut QueryNode<'query>, samples: usize) -> Self {
        let tuples = query.approximate_created_tuples();
        let mut samples = HashMap::new();





        Self {
            query_node: query,
            start_tuples: tuples,
            samples
        }
    }

    fn get_relations(query: &QueryNode<'query>) -> Vec<&'query Relation> {
        if let Query::Source(s) = query.query() {
            vec![s.relation()]
        } else {
            query.children()
                .iter()
                .map(|c| Self::get_relations(*c))
                .flatten()
                .collect()
        }
    }

    /// Optimizes the query, and gives the approximate ratio of reduced tuples being created
    ///
    /// The optimizer can be ran multiple times, theoretically, but all subsequent runs will not
    /// have an effect, and will likely return an efficiency ratio of 1.0
    pub fn optimize(&mut self) -> f64 {
        Self::split_all_ands(self.query_node);
        self.query_node.approximate_created_tuples() as f64 / self.start_tuples as f64
    }

    /// Splits all AND conditionals into multiple selection nodes
    fn split_all_ands(node: &mut QueryNode<'query>) {
        let split_conditions = if let Query::Selection(condition) = node.query_mut() {
            condition.clone().split_and()
        } else {
            vec![]
        };

        if split_conditions.len() > 1 {
            /*
            let mut iterator = split_conditions.into_iter();
            let mut ptr = QueryNode::select_on_condition()iterator.next().unwrap();
            while let Some(condition) = iterator.next() {
                ptr = QueryNode::select_on_condition(ptr, condition);
            }

             */

            let ptr = std::mem::replace(node.children_mut(), QueryChildren::None);
            if let QueryChildren::One(mut ptr) = ptr {
                for condition in split_conditions {
                    ptr = QueryNode::select_on_condition(ptr, condition);
                }
                *node = ptr;
            } else {
                panic!("invalid query")
            }
        }

        for child in node.children_mut_list() {
            Self::split_all_ands(child);
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use rad_db_types::{Type, Value};
    use rad_db_structure::prelude::*;
    use crate::query::conditions::{Condition, ConditionOperation, Operand};
    use std::iter::FromIterator;

    #[test]
    fn and_equivalence() {
        let mut relation1 = Relation::new_volatile(
            Identifier::new("test1"),
            vec![("field1", Type::from(0u64))],
            64,
            PrimaryKeyDefinition::new(vec![0]),
        );
        for i in 0..1000u64 {
            //println!("Inserting tuple {}", i);
            relation1.insert(Tuple::from_iter(&[Value::from(i)]));
        }
        let query =
            QueryNode::select_on_condition(
                QueryNode::source(&relation1),
                Condition::and(
                    Condition::new("field1", ConditionOperation::Equals(Operand::UnsignedNumber(32))),
                    Condition::new("field1", ConditionOperation::Nequals(Operand::UnsignedNumber(34)))
                )
            );
        let query_copied = query.clone();
        let optimized = query.optimized();
        let optimized_tuple_count = optimized.approximate_created_tuples();
        let copied_tuple_count = query_copied.approximate_created_tuples();
        println!("Optimized Tuples: {}", optimized_tuple_count);
        println!("Original Tuples: {}", copied_tuple_count);
        assert_eq!(optimized_tuple_count, copied_tuple_count);
        assert_ne!(optimized.nodes(), query_copied.nodes()); // shouldn't be same
        assert_eq!(optimized.nodes() - 1, query_copied.nodes()); // should be exactly one more node
    }
}
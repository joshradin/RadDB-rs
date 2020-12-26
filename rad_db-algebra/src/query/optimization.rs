use crate::error::MissingFieldError;
use crate::query::conditions::{Condition, JoinCondition};
use crate::query::query_node::QueryOperation;
use crate::query::query_node::{QueryChildren, QueryNode, Source};
use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::Relation;
use rad_db_types::Value;
use rand::seq::IteratorRandom;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;

pub struct Optimizer<'a, 'q>
where
    'q: 'a,
{
    query_node: &'a mut QueryNode<'q>,
    start_tuples: usize,
    /// A sample of some amount of random values of the relevant fields in selections
    samples: HashMap<Identifier, Vec<Value>>,
}

fn sample_field(
    field: &Identifier,
    source: &Relation,
    samples: usize,
) -> Result<Vec<Value>, MissingFieldError> {
    let field_index = source.get_field_index(field);
    if field_index.is_none() {
        return Err(MissingFieldError::new(field.clone()));
    }
    let field_index = field_index.unwrap();
    let tuples = source.tuples();
    let mut random = rand::thread_rng();
    let sampled_tuples = tuples.choose_multiple(&mut random, samples);
    let samples_values: Vec<_> = sampled_tuples
        .into_iter()
        .map(|tuple| tuple.take(field_index))
        .collect();
    Ok(samples_values)
}

fn find_all_selections<'a>(query: &'a QueryNode<'_>) -> Vec<&'a Condition> {
    let mut ret = vec![];

    if let QueryOperation::Selection(condition) = query.query_operation() {
        ret.push(condition);
    }

    for child in query.children() {
        ret.extend(find_all_selections(child));
    }

    ret
}

impl<'a, 'query> Optimizer<'a, 'query>
where
    'query: 'a,
{
    pub fn new(query: &'a mut QueryNode<'query>, samples: usize) -> Self {
        let tuples = query.approximate_created_tuples();
        let mut sampled_fields = HashMap::new();

        let all_relevant_fields: HashSet<_> = find_all_selections(query)
            .into_iter()
            .map(|condition| condition.relevant_fields())
            .flatten()
            .collect();
        for field in all_relevant_fields {
            if let Some(node) = query.find_node_with_field(&field) {
                if let Some(relation) = node.my_relation() {
                    let sample = sample_field(&field, relation, samples)
                        .expect(&*format!("Field {} went missing", field));
                    sampled_fields.insert(field, sample);
                }
            }
        }

        Self {
            query_node: query,
            start_tuples: tuples,
            samples: sampled_fields,
        }
    }

    fn get_relations(query: &QueryNode<'query>) -> Vec<&'query Relation> {
        if let QueryOperation::Source(s) = query.query_operation() {
            vec![s.relation()]
        } else {
            query
                .children()
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
        let split_conditions = if let QueryOperation::Selection(condition) = node.query_mut() {
            condition.clone().split_and()
        } else {
            vec![]
        };

        if split_conditions.len() > 1 {
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

    fn push_selects_down(&self) {}

    /// If child is selection, this will flip the conditions
    fn commute_selection(parent: &'query mut QueryNode<'query>) -> bool {
        let parent_condition =
            if let QueryOperation::Selection(parent_condition) = parent.query_operation() {
                parent_condition.clone()
            } else {
                return false;
            };
        let child_condition = if let Some(QueryOperation::Selection(child_condition)) =
            parent.children().get(0).map(|c| c.query_operation())
        {
            child_condition.clone()
        } else {
            return false;
        };

        if let QueryOperation::Selection(parent_condition) = parent.query_mut() {
            *parent_condition = child_condition;
        }

        if let Some(child) = parent.children_mut_list().get_mut(0) {
            if let QueryOperation::Selection(child_condition) = child.query_mut() {
                *child_condition = parent_condition;
            }
        }
        true
    }

    /// Removes all direct child projections
    fn cascade_projection(parent: &'query mut QueryNode<'query>) -> bool {
        let is_projections = if let QueryOperation::Projection(_) = parent.query_operation() {
            true
        } else {
            false
        };

        if is_projections {
            if let QueryChildren::One(mut ptr) = parent.take_children() {
                while let QueryOperation::Projection(_) = ptr.query_operation() {
                    if let QueryChildren::One(new_ptr) = ptr.take_children() {
                        ptr = new_ptr
                    } else {
                        break;
                    }
                }
                *parent.children_mut() = QueryChildren::One(ptr);
            }
            return true;
        }
        false
    }

    /// Commute selection and projection
    fn commute_projection_and_selection(parent: &mut QueryNode<'query>) -> bool {
        let swap = if let QueryOperation::Projection(_) = parent.query_operation() {
            let child = parent.children()[0];
            if let QueryOperation::Selection(_) = child.query_operation() {
                true
            } else {
                false
            }
        } else if let QueryOperation::Selection(_) = parent.query_operation() {
            let child = parent.children()[0];
            if let QueryOperation::Projection(_) = child.query_operation() {
                true
            } else {
                false
            }
        } else {
            false
        };

        if swap {
            let take = parent.take_children();
            if let QueryChildren::One(mut child) = take {
                std::mem::swap(parent, &mut child);
                *parent.children_mut() = QueryChildren::One(child);
            }
            return true;
        }
        false
    }

    /// Swaps the children of a join operation for inner joins or cross products
    fn commute_join(join: &'query mut QueryNode<'query>) -> bool {
        let is_join = match join.query_operation() {
            QueryOperation::CrossProduct => true,
            QueryOperation::InnerJoin(_) => true,
            QueryOperation::NaturalJoin => true,
            _ => false,
        };

        if is_join {
            if let QueryChildren::Two(left, right) = join.children_mut() {
                std::mem::swap(left, right);
            }
            return true;
        }
        false
    }

    /// Turns a selection followed by a cross product into a inner join, if select.f1=f2(R1xR2) is
    /// equivalent to R1 join.f1=f2 R2. This is true when f1 is a field in either a child of R1 or R1 itself, and f2 is the same for R2
    fn commute_selection_with_join(selection: &'query mut QueryNode<'query>) -> bool {
        let make_join = if let QueryOperation::Selection(condition) = selection.query_operation() {
            let make_join = {
                let children = selection.children();
                if let QueryOperation::CrossProduct = children[0].query_operation() {
                    if condition.not_conjunction() {
                        let relevant_fields = Vec::from_iter(condition.relevant_fields());
                        let first_node = selection.find_node_with_field(&relevant_fields[0]);
                        let second_node = selection.find_node_with_field(&relevant_fields[1]);

                        match (first_node, second_node) {
                            (Some(first_node), Some(second_node)) => {
                                if first_node == second_node
                                    || first_node == selection
                                    || second_node == selection
                                {
                                    return false; // nodes can't be each-other, or the parent node
                                }

                                if children[0].is_parent_or_self(first_node) {
                                    (true, false, relevant_fields)
                                } else {
                                    (true, true, relevant_fields)
                                }
                            }
                            _ => return false,
                        }
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            }; // second boolean is whether condition is reversed, where true is reversed

            make_join
        } else {
            return false;
        };
        if let (true, reverse, fields) = make_join {
            let children = selection.take_children();
            if let QueryChildren::Two(left, right) = children {
                let join_condition = if reverse {
                    JoinCondition::new(fields[1].clone(), fields[0].clone())
                } else {
                    JoinCondition::new(fields[0].clone(), fields[1].clone())
                };

                let join = QueryNode::inner_join(left, right, join_condition);

                *selection = join;
            }
            return true;
        }
        false
    }

    /// Splits a projection over a join.
    ///
    /// If the projection contains the fields used in the join, then
    /// the project is completely split and moved down the tree.
    ///
    /// If the projection doesn't contain the fields, instead new projections are made that contain
    /// the projection and the fields used for the join. The original projection is kept.
    fn split_projections_over_join(projection: &'query mut QueryNode<'query>) -> bool {
        if let QueryOperation::Projection(projections) = projection.query_operation() {
            if let QueryOperation::InnerJoin(join_condition) =
                projection.children()[0].query_operation()
            {
                let projections = projections.to_owned();
                let join_condition = join_condition.to_owned();

                let mut child = {
                    if let QueryChildren::One(child) = projection.take_children() {
                        child
                    } else {
                        unreachable!()
                    }
                };

                let mut left_fields = Vec::new();
                let mut right_fields = Vec::new();

                if let QueryChildren::Two(mut left, mut right) = child.take_children() {
                    for id in projections {
                        let query = projection.find_node_with_field(&id).unwrap();
                        if left.is_parent_or_self(query) {
                            left_fields.push(id.clone())
                        } else if right.is_parent_or_self(query) {
                            right_fields.push(id.clone())
                        }
                    }

                    if left_fields.contains(join_condition.left_id())
                        && right_fields.contains(join_condition.right_id())
                    {
                        let left_projection = QueryNode::projection(left, left_fields);
                        let right_projection = QueryNode::projection(right, right_fields);
                        let join = QueryNode::inner_join(
                            left_projection,
                            right_projection,
                            join_condition,
                        );
                        *projection = join;
                    } else {
                        if !left_fields.contains(join_condition.left_id()) {
                            left_fields.push(join_condition.left_id().clone());
                        }
                        if !right_fields.contains(join_condition.right_id()) {
                            right_fields.push(join_condition.right_id().clone());
                        }
                        let left_projection = QueryNode::projection(left, left_fields);
                        let right_projection = QueryNode::projection(right, right_fields);
                        let join = QueryNode::inner_join(
                            left_projection,
                            right_projection,
                            join_condition,
                        );
                        *projection.children_mut() = QueryChildren::One(join);
                    }
                } else {
                    unreachable!()
                }
                return true;
            }
        }
        false
    }

    fn push_selection_through_join(selection: &'query mut QueryNode<'query>) -> bool {
        unimplemented!()
        /*
        enum Child { Left, Right}


        let (push, side): (bool, Option<Child>) = if let QueryOperation::Selection(condition) = selection.query_operation() {
            let child = selection.children()[0];
            if child.is_join() {
                let child_children = child.children();
                let left = child_children[0];
                let right = child_children[1];

                let fields = condition.relevant_fields();
                let mut relevant_nodes = vec![];
                for field in fields {
                    if let Some(node) = child.find_node_with_field(field) {
                        relevant_nodes.push(node);
                    } else {
                        return false; // node not in this tree
                    }
                }
                let mut side = None;
                let mut push = false;

                for relevant_node in relevant_nodes {

                }

                ()
            } else {
                (false, None)
            }
        } else {
            (false, None)
        }

         */
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::conditions::{Condition, ConditionOperation, Operand};
    use rad_db_structure::prelude::*;
    use rad_db_types::{Type, Value};
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
        let query = QueryNode::select_on_condition(
            QueryNode::source(&relation1),
            Condition::and(
                Condition::new(
                    "field1",
                    ConditionOperation::Equals(Operand::UnsignedNumber(32)),
                ),
                Condition::new(
                    "field1",
                    ConditionOperation::Nequals(Operand::UnsignedNumber(34)),
                ),
            ),
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

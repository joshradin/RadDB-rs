use crate::query::conditions::{Condition, ConditionOperation, JoinCondition, Operand};
use crate::query::optimization::Optimizer;
use crate::query::query_iterator::QueryIterator;
use crate::query::query_result::QueryResult;
use crate::query::Repeatable;
use crate::relation_mapping::MappedRelation;
use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::tuple_storage::{BlockIterator, StoredTupleIterator};
use rad_db_structure::relations::Relation;
use rad_db_structure::tuple::Tuple;
use rad_db_types::{Type, Value};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;

#[derive(Clone)]
pub struct Crawler<'a> {
    source: MappedRelation<'a>,
    iterator: Option<BlockIterator<'a>>,
}

impl<'a> Crawler<'a> {
    pub fn new(source: MappedRelation<'a>) -> Self {
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
            self.iterator = Some(self.source.relation().blocks());
        }

        self.iterator.as_mut().unwrap().next()
    }
}

#[derive(Clone)]
pub struct Source<'a>(Crawler<'a>);

impl<'a> Deref for Source<'a> {
    type Target = Crawler<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> Source<'a> {
    pub fn source_len(&self) -> usize {
        self.source.relation().len()
    }

    pub fn relation(&self) -> &'a Relation {
        self.source.relation()
    }
}

impl<'a> Repeatable for Source<'a> {
    type Item = Vec<Tuple>;
    type IntoIter = BlockIterator<'a>;

    fn get_iterator(&self) -> Self::IntoIter {
        self.source.relation().blocks()
    }
}

impl Iterator for Source<'_> {
    type Item = Vec<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

#[derive(Clone)]
pub enum QueryOperation<'a> {
    Source(Source<'a>),
    Projection(Vec<Identifier>),
    Selection(Condition),
    CrossProduct,
    InnerJoin(JoinCondition),
    LeftJoin(JoinCondition),
    RightJoin(JoinCondition),
    NaturalJoin,
}

#[derive(Clone)]
pub enum QueryChildren<'a> {
    None,
    One(QueryNode<'a>),
    Two(QueryNode<'a>, QueryNode<'a>),
}

#[derive(Clone)]
pub struct QueryNode<'a> {
    query: QueryOperation<'a>,
    children: Box<QueryChildren<'a>>,
    resulting_relation: Vec<(Identifier, Type)>,
    mapping: HashMap<Identifier, Identifier>,
    id: usize,
}

impl<'a> PartialEq<&QueryNode<'a>> for &QueryNode<'a> {
    fn eq(&self, other: &&QueryNode<'a>) -> bool {
        *other as *const QueryNode<'a> == *self as *const QueryNode<'a>
    }
}

impl<'a> QueryNode<'a> {
    pub fn source(relation: &'a Relation) -> Self {
        let mapped_relation = MappedRelation::new(relation);
        let mapping = relation
            .attributes()
            .iter()
            .map(|(id, _)| {
                let identifier = Identifier::new(id);
                (identifier.clone(), identifier)
            })
            .collect();
        Self {
            query: QueryOperation::Source(Source(Crawler::new(mapped_relation))),
            children: Box::new(QueryChildren::None),
            resulting_relation: relation
                .attributes()
                .iter()
                .map(|(id, val)| (Identifier::new(id), val.clone()))
                .collect(),
            mapping,
            id: 0,
        }
    }

    pub fn source_with_name(relation: &'a Relation, name: String) -> Self {
        let mapped_relation = MappedRelation::new(relation).alias_relation(name.clone());
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
            query: QueryOperation::Source(Source(Crawler::new(mapped_relation))),
            children: Box::new(QueryChildren::None),
            resulting_relation: relation
                .attributes()
                .iter()
                .map(|(id, val)| (Identifier::concat(&name, id), val.clone()))
                .collect(),
            mapping,
            id: 0,
        }
    }

    pub fn inner_join(mut left: Self, mut right: Self, condition: JoinCondition) -> Self {
        let mut result = Vec::new();
        result.extend(left.resulting_relation.iter().cloned());
        result.extend(right.resulting_relation.iter().cloned());
        let mapping = result
            .iter()
            .map(|(id, _)| (id.clone(), id.clone()))
            .collect();
        left.increment_id();
        right.increase_id_by(1 + left.count());

        QueryNode {
            query: QueryOperation::InnerJoin(condition),
            children: Box::new(QueryChildren::Two(left, right)),
            resulting_relation: result,
            mapping: mapping,
            id: 0,
        }
    }

    pub fn cross_product(mut left: Self, mut right: Self) -> Self {
        let mut result = Vec::new();
        result.extend(left.resulting_relation.iter().cloned());
        result.extend(right.resulting_relation.iter().cloned());
        let mapping = result
            .iter()
            .map(|(id, _)| (id.clone(), id.clone()))
            .collect();
        left.increment_id();
        right.increase_id_by(1 + left.count());

        QueryNode {
            query: QueryOperation::CrossProduct,
            children: Box::new(QueryChildren::Two(left, right)),
            resulting_relation: result,
            mapping: mapping,
            id: 0,
        }
    }

    pub fn select_on_condition(node: Self, condition: Condition) -> Self {
        let vec = node.resulting_relation.clone();
        let map = node.mapping.clone();
        Self {
            query: QueryOperation::Selection(condition),
            children: Box::new(QueryChildren::One(node)),
            resulting_relation: vec,
            mapping: map,
            id: 0,
        }
    }

    pub fn select_eq(node: Self, id: Identifier, eq: Operand) -> Self {
        Self::select_on_condition(node, Condition::new(id, ConditionOperation::Equals(eq)))
    }

    pub fn projection<Id: Into<Identifier> + ToOwned<Owned = Id>, I: IntoIterator<Item = Id>>(
        mut node: Self,
        fields: I,
    ) -> Self {
        let projections: Vec<Identifier> = fields.into_iter().map(|i| i.into()).collect();
        let resulting_relation = projections
            .iter()
            .filter_map(|id| {
                if let Some(pos) = node
                    .resulting_relation
                    .iter()
                    .position(|(inner_id, _)| id == inner_id)
                {
                    let (_, ty) = &node.resulting_relation[pos];
                    Some((id.clone(), ty.clone()))
                } else {
                    None
                }
            })
            .collect();
        node.increment_id();
        Self {
            query: QueryOperation::Projection(projections),
            children: Box::new(QueryChildren::One(node)),
            resulting_relation,
            mapping: Default::default(),
            id: 0,
        }
    }

    /// Increases the ids of all of the nodes in this tree by one
    fn increment_id(&mut self) {
        self.increase_id_by(1)
    }

    /// Increases the ids of all of the nodes in this tree by this value
    fn increase_id_by(&mut self, by: usize) {
        self.id += by;
        for (i, child) in self.children_mut_list().into_iter().enumerate() {
            child.increase_id_by(i);
        }
    }

    /// Gets the number of nodes in this tree
    pub fn count(&self) -> usize {
        let mut ret = 1;
        for child in self.children() {
            ret += child.count();
        }
        ret
    }

    pub fn optimize_query(&mut self) {
        let mut optimizer = Optimizer::new(self, 500);
        optimizer.optimize();
    }

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
            (QueryOperation::Source(source), QueryChildren::None) => {
                let inner = QueryResult::from_source(relation, source);
                return inner;
            }
            (QueryOperation::InnerJoin(join), QueryChildren::Two(left, right)) => {
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
            (QueryOperation::CrossProduct, QueryChildren::Two(left, right)) => {
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

    pub fn approximate_created_tuples(&self) -> usize {
        match &self.query {
            QueryOperation::Source(s) => s.source_len(),
            QueryOperation::Projection(_) => {
                if let QueryChildren::One(child) = &*self.children {
                    child.approximate_created_tuples()
                } else {
                    panic!("Invalid query")
                }
            }
            QueryOperation::Selection(c) => {
                if let QueryChildren::One(child) = &*self.children {
                    c.selectivity(child.approximate_created_tuples()) as usize
                } else {
                    panic!("Invalid query")
                }
            }
            QueryOperation::CrossProduct => {
                if let QueryChildren::Two(l, r) = &*self.children {
                    l.approximate_created_tuples() * r.approximate_created_tuples()
                } else {
                    panic!("Invalid query")
                }
            }
            QueryOperation::InnerJoin(_) => {
                if let QueryChildren::Two(l, r) = &*self.children {
                    max(
                        l.approximate_created_tuples(),
                        r.approximate_created_tuples(),
                    )
                } else {
                    panic!("Invalid query")
                }
            }
            QueryOperation::LeftJoin(_) => {
                if let QueryChildren::Two(l, r) = &*self.children {
                    l.approximate_created_tuples() * r.approximate_created_tuples()
                } else {
                    panic!("Invalid query")
                }
            }
            QueryOperation::RightJoin(_) => {
                if let QueryChildren::Two(l, r) = &*self.children {
                    l.approximate_created_tuples() * r.approximate_created_tuples()
                } else {
                    panic!("Invalid query")
                }
            }
            QueryOperation::NaturalJoin => {
                if let QueryChildren::Two(l, r) = &*self.children {
                    max(
                        l.approximate_created_tuples(),
                        r.approximate_created_tuples(),
                    )
                } else {
                    panic!("Invalid query")
                }
            }
        }
    }

    pub fn children(&self) -> Vec<&QueryNode<'a>> {
        match &*self.children {
            QueryChildren::None => {
                vec![]
            }
            QueryChildren::One(o) => {
                vec![o]
            }
            QueryChildren::Two(l, r) => {
                vec![l, r]
            }
        }
    }

    pub(super) fn children_mut(&mut self) -> &mut QueryChildren<'a> {
        &mut self.children
    }

    pub(super) fn children_mut_list(&mut self) -> Vec<&mut QueryNode<'a>> {
        match &mut *self.children {
            QueryChildren::None => {
                vec![]
            }
            QueryChildren::One(o) => {
                vec![o]
            }
            QueryChildren::Two(l, r) => {
                vec![l, r]
            }
        }
    }

    pub fn query_operation(&self) -> &QueryOperation<'a> {
        &self.query
    }

    pub(super) fn query_mut(&mut self) -> &mut QueryOperation<'a> {
        &mut self.query
    }

    /// Gets the count of nodes in this query
    pub fn nodes(&self) -> usize {
        1usize
            + self
                .children()
                .iter()
                .map(|child| child.nodes())
                .sum::<usize>()
    }

    /// Finds the lowest node with this relation in it. If multiple children contain the
    /// relation, this node is the lowest node.
    pub fn find_relation<I: Into<Identifier> + ToOwned<Owned = I>>(
        &self,
        relation: I,
    ) -> Option<&QueryNode> {
        let id = relation.into();
        for child in self.children() {
            if let Some(node) = child.find_relation(&id) {
                return Some(node);
            }
        }

        if let QueryOperation::Source(source) = &self.query {
            if source.source.valid_name(&id) {
                return Some(self);
            }
        }

        None
    }

    /// Finds the lowest node with these relations in it. If multiple children contain the
    /// relations, this node is the lowest node.
    pub fn find_relations<Iter, Id>(&self, relations: Iter) -> Option<&QueryNode<'a>>
    where
        Id: Into<Identifier>,
        Iter: IntoIterator<Item = Id>,
    {
        let ids: HashSet<Identifier> = relations.into_iter().map(|id| id.into()).collect();
        self.find_relations_helper(&ids).1
    }

    /// Returns the list of relations that this node has access to
    fn find_relations_helper(
        &self,
        relations: &HashSet<Identifier>,
    ) -> (HashSet<Identifier>, Option<&QueryNode<'a>>) {
        let mut ret = None;
        let mut found_relations = HashSet::new();
        for child in self.children() {
            match child.find_relations_helper(relations) {
                (vec, None) => {
                    found_relations.extend(vec);
                }
                (_, Some(child_result)) => {
                    if ret.is_none() {
                        ret = Some(child_result);
                    } else {
                        return (HashSet::new(), Some(self));
                    }
                }
            }
        }

        if found_relations.is_superset(relations) {
            return (HashSet::new(), Some(self));
        }

        if let QueryOperation::Source(source) = &self.query {
            for id in relations {
                if source.source.valid_name(&id) {
                    found_relations.insert(id.clone());
                    break;
                }
            }
        }

        (found_relations, None)
    }

    /// If this node only has one relation, this function finds such relation. If there are
    /// multiple relations that this is parent of, None is returned.
    pub(super) fn my_relation(&self) -> Option<&'a Relation> {
        if let QueryOperation::Source(source) = &self.query {
            // Garuanteed no children
            return Some(source.source.relation());
        }

        let mut ret = None;
        for child in self.children() {
            if let Some(child_relation) = child.my_relation() {
                if ret.is_none() {
                    ret = Some(child_relation)
                } else {
                    return None;
                }
            }
        }

        ret
    }

    /// Finds a node with a field. If multiple relations within the query have the same field, but aren't
    /// part of the same relation
    pub fn find_node_with_field<I: Into<Identifier>>(&self, field: I) -> Option<&QueryNode<'a>> {
        let id = field.into();
        if let QueryOperation::Source(source) = &self.query {
            if source.source.contains_field(&id) {
                return Some(self);
            }
        }

        let mut ret = None;
        for child in self.children() {
            if let Some(node) = child.find_node_with_field(&id) {
                if ret == None {
                    ret = Some(node);
                } else {
                    if ret.unwrap() != node {
                        return None;
                    }
                }
            }
        }

        ret
    }

    pub(super) fn take_children(&mut self) -> QueryChildren<'a> {
        *std::mem::replace(&mut self.children, Box::new(QueryChildren::None))
    }

    /// Checks if the referenced node is this node or a child of this node
    pub fn is_parent_or_self(&self, other: &QueryNode<'a>) -> bool {
        if self == other {
            return true;
        }

        match &*self.children {
            QueryChildren::None => false,
            QueryChildren::One(child) => child.is_parent_or_self(other),
            QueryChildren::Two(l, r) => l.is_parent_or_self(other) || r.is_parent_or_self(other),
        }
    }

    /// Pushes a parent to where this node is
    pub fn push(&mut self, new_parent: QueryNode<'a>) {
        let old = std::mem::replace(self, new_parent);
        self.children = Box::new(QueryChildren::One(old));
    }

    /// Recalculates the resulting relations of the children nodes
    pub fn recalculate_resulting_relation(&mut self) {
        for child in self.children_mut_list() {
            child.recalculate_resulting_relation();
        }

        let relation = match &self.query {
            QueryOperation::Source(_) => self.resulting_relation.clone(),
            QueryOperation::Projection(p) => {
                let child = self.children()[0];
                child
                    .resulting_relation
                    .iter()
                    .filter_map(|(id, ty)| {
                        if p.contains(id) {
                            Some((id.clone(), ty.clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            }
            QueryOperation::Selection(_) => {
                let child = self.children()[0];
                child.resulting_relation.clone()
            }
            QueryOperation::CrossProduct
            | QueryOperation::InnerJoin(_)
            | QueryOperation::LeftJoin(_)
            | QueryOperation::RightJoin(_)
            | QueryOperation::NaturalJoin => {
                let mut left = self.children()[0].resulting_relation.clone();
                left.extend(self.children()[1].resulting_relation.clone());
                left
            }
        };

        self.resulting_relation = relation;
    }

    /// Gets the tree-specific id of the node
    pub fn id(&self) -> usize {
        self.id
    }

    /// Whether this node is a join
    pub fn is_join(&self) -> bool {
        match self.query_operation() {
            QueryOperation::CrossProduct => true,
            QueryOperation::InnerJoin(_) => true,
            QueryOperation::LeftJoin(_) => true,
            QueryOperation::RightJoin(_) => true,
            QueryOperation::NaturalJoin => true,
            _ => false,
        }
    }

    /// Tests if this node has access to all of the fields in the iterator
    pub fn contains_all_fields<Id: Into<Identifier>, Iter>(&self, iterator: Iter) -> bool
    where
        Iter: IntoIterator<Item = Id>,
    {
        let mut iterator = iterator.into_iter().map(|i| i.into());
        self.contains_all_fields_helper(&mut iterator)
    }

    fn contains_all_fields_helper(&self, iter: &mut dyn Iterator<Item = Identifier>) -> bool {
        self.find_relations(iter).is_some()
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

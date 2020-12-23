use crate::query::query_node::QueryNode;

pub struct Optimizer<'a, 'b> where 'b : 'a {
    query_node: &'a mut QueryNode<'b>
}

impl<'a, 'b> Optimizer<'a, 'b> where 'b : 'a {
    pub fn new(query: &'a mut QueryNode<'b>) -> Self {
        Self {
            query_node: query
        }
    }

    pub fn optimize(&self) {

    }
}
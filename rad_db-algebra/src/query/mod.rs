pub mod conditions;
pub mod query_iterator;
pub mod query_node;
pub mod query_result;

/// An object that can be turned into an iterator multiple times
pub trait Repeatable {
    type Item;

    fn get_iterator(&self) -> Box<dyn Iterator<Item = Self::Item>>;
}

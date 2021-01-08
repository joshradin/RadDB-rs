use rad_db_structure::identifier::Identifier;
use rad_db_structure::tuple::Tuple;
use rad_db_types::Value;
use std::ops::Index;
use std::sync::Arc;

/// A tuple containing information on it's fields
pub struct WrappedTuple<'a> {
    fields: &'a Vec<Identifier>,
    tuple: &'a Tuple,
}

impl<'a> WrappedTuple<'a> {
    pub fn new(fields: &'a Vec<Identifier>, tuple: &'a Tuple) -> Self {
        WrappedTuple { fields, tuple }
    }
}

impl<'a, I: Into<Identifier>> Index<I> for WrappedTuple<'a> {
    type Output = Value;

    fn index(&self, index: I) -> &Self::Output {
        let id = index.into();
        let pos = self.fields.iter().position(|f| f == id);
        match pos {
            None => {
                panic!("No field named {} in this tuple", id)
            }
            Some(index) => &self.tuple[index],
        }
    }
}

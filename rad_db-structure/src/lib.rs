use crate::identifier::Identifier;

pub mod constraint;
pub mod identifier;
pub mod key;
pub mod relations;
pub mod tuple;

pub trait Rename<I: Into<Identifier>> {
    fn rename(&mut self, name: I);
}

pub mod prelude {

    pub use crate::identifier::Identifier;
    pub use crate::key::primary::*;
    pub use crate::relations::{
        Relation,
        RelationDefinition
    };
    pub use crate::tuple::Tuple;
}

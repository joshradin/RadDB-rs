use crate::identifier::Identifier;

pub mod constraint;
pub mod identifier;
pub mod key;
pub mod relations;
pub mod tuple;

pub trait Rename<I: Into<Identifier>> {
    fn rename(&mut self, name: I);
}

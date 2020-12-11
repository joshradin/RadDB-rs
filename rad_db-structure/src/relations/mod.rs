use rad_db_types::Type;

#[doc(hidden)]
mod relation_struct;
pub use relation_struct::*;

pub trait AsTypeList {
    fn to_type_list(&self) -> Vec<Type>;
}

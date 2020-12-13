use rad_db_types::Type;
use std::ops::{Deref, DerefMut};

/// Represents a single row within a database.
/// A tuple knows no information about itself besides its contents
pub struct Tuple(Vec<Type>);

impl Deref for Tuple {
    type Target = Vec<Type>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Tuple {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

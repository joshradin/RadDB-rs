use rad_db_structure::relations::tuple_storage::TupleStorage;
use rad_db_structure::tuple::Tuple;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct QueryBuffer {
    storage: VecDeque<Tuple>,
}

impl QueryBuffer {
    /// Creates a new buffer, with an optional max storage.
    ///
    /// # Warning
    /// There is an implicit max storage of the max value of isize::MAX/std::mem::sizeof<usize>
    pub fn new() -> Self {
        QueryBuffer {
            storage: VecDeque::new(),
        }
    }

    /// Attempts to push a tuple onto the buffer
    ///
    /// # Panic
    /// Will panic if the buffer is full
    pub fn push(&mut self, tuple: Tuple) {
        self.storage.push_back(tuple)
    }

    /// Attempts to push a tuple onto the buffer
    ///
    /// # Panic
    /// Will panic if the buffer becomes full and another tuples is attempted to be added
    pub fn push_all<I : IntoIterator<Item=Tuple>>(&mut self, iterator: I) {
        self.storage.extend(iterator)
    }

    /// Returns true if there isn't any tuples in the buffer
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }
}

impl Iterator for &mut QueryBuffer {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        self.storage.pop_front()
    }
}

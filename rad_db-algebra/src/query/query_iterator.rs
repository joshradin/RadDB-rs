use rad_db_structure::relations::tuple_storage::TupleStorage;
use rad_db_structure::tuple::Tuple;
use std::collections::VecDeque;

#[derive(Debug)]
pub struct QueryBuffer {
    max_storage: Option<usize>,
    storage: VecDeque<Tuple>,
}

impl QueryBuffer {
    /// Creates a new buffer, with an optional max storage.
    ///
    /// # Warning
    /// There is an implicit max storage of the max value of isize::MAX/std::mem::sizeof<usize>
    pub fn new(max_storage: Option<usize>) -> Self {
        QueryBuffer {
            max_storage,
            storage: match max_storage {
                None => VecDeque::new(),
                Some(capacity) => VecDeque::with_capacity(capacity),
            },
        }
    }

    /// Attempts to push a tuple onto the buffer
    ///
    /// # Panic
    /// Will panic if the buffer is full
    pub fn push(&mut self, tuple: Tuple) {
        match self.try_push(tuple) {
            Ok(_) => {}
            Err(_) => {
                panic!("Could not push tuple into buffer")
            }
        }
    }

    /// Attempts to push a tuple onto the buffer, returning OK(()) if successful
    pub fn try_push(&mut self, tuple: Tuple) -> Result<(), ()> {
        let max_storage = match self.max_storage {
            None => usize::MAX / std::mem::size_of::<usize>(),
            Some(max) => max,
        };

        if self.storage.len() == max_storage {
            return Err(());
        }

        self.storage.push_back(tuple);
        Ok(())
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

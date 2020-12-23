use std::borrow::Cow;
use std::collections::VecDeque;

use rad_db_structure::relations::tuple_storage::{BlockIterator, TupleStorage};
use rad_db_structure::tuple::Tuple;

use crate::query::query_result::{QueryResultBlocks, QueryResultFullData};
use crate::query::Repeatable;

pub struct QueryIterator<'a> {
    backing: QueryResultFullData<'a>,
    buffer: VecDeque<Tuple>,
}

impl<'a> QueryIterator<'a> {
    pub(crate) fn new(backing: QueryResultFullData<'a>) -> Self {
        QueryIterator {
            backing,
            buffer: VecDeque::new(),
        }
    }
}

impl Iterator for QueryIterator<'_> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buffer.is_empty() {
            return self.buffer.pop_front();
        }

        match &mut self.backing {
            QueryResultFullData::Tuples(tuples) => tuples.pop(),
            QueryResultFullData::BlockData(blocks) => {
                match blocks {
                    QueryResultBlocks::Blocks(blocks) => {
                        if let Some(tuples) = blocks.pop() {
                            self.buffer.extend(tuples);
                        }
                    }
                    QueryResultBlocks::Source(source) => {
                        if let Some(tuples) = source.next() {
                            self.buffer.extend(tuples);
                        }
                    }
                }

                self.buffer.pop_front()
            }
        }
    }
}

pub struct ReferencedQueryIterator<'a> {
    backing: &'a QueryResultFullData<'a>,
    buffer: VecDeque<Tuple>,
    blocks_count: usize,
    block_iterator: Option<BlockIterator<'a>>,
}

impl<'a> ReferencedQueryIterator<'a> {
    pub fn new(backing: &'a QueryResultFullData<'a>) -> Self {
        ReferencedQueryIterator {
            backing,
            buffer: VecDeque::new(),
            blocks_count: 0,
            block_iterator: None,
        }
    }
}

impl Iterator for ReferencedQueryIterator<'_> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.buffer.is_empty() {
            return self.buffer.pop_front();
        }

        if let Some(block_iterator) = &mut self.block_iterator {
            let tuples: Option<Vec<Tuple>> = block_iterator.next();
            if let Some(tuples) = tuples {
                self.buffer.extend(tuples);
            }
            return self.buffer.pop_front();
        }

        match &mut self.backing {
            QueryResultFullData::Tuples(tuples) => {
                self.buffer.extend(tuples.into_iter().cloned());
                self.buffer.pop_front()
            }
            QueryResultFullData::BlockData(blocks) => {
                match blocks {
                    QueryResultBlocks::Blocks(blocks) => {
                        if let Some(tuples) = blocks.get(self.blocks_count) {
                            self.buffer.extend(tuples.into_iter().cloned());
                        }
                        self.blocks_count += 1;
                    }
                    QueryResultBlocks::Source(source) => {
                        let mut block_iterator: BlockIterator = source.get_iterator();
                        let tuples: Option<Vec<Tuple>> = block_iterator.next();
                        if let Some(tuples) = tuples {
                            self.buffer.extend(tuples);
                        }
                        return self.buffer.pop_front();
                        self.block_iterator = Some(block_iterator);
                    }
                }

                self.buffer.pop_front()
            }
        }
    }
}

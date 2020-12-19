use std::cell::UnsafeCell;
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::ops::{BitAnd, Deref, DerefMut};
use std::sync::{Arc, RwLock, RwLockReadGuard};

use num_bigint::{BigUint, ToBigUint};
use num_traits::{One, ToPrimitive, Zero};

use crate::identifier::Identifier;
use crate::key::primary::{PrimaryKey, PrimaryKeyDefinition};
use crate::relations::tuple_storage::block::{Block, InUse};
use crate::relations::tuple_storage::lock::{Lock, LockRead, LockWrite};
use crate::relations::tuple_storage::TupleStorage;
use crate::relations::RelationDefinition;
use crate::tuple::Tuple;
use crate::Rename;

/// A local bucket that contains information on the local block
pub(super) struct Bucket {
    local_depth: usize,
    block: Block,
    mask: BigUint,
}

impl Bucket {
    fn len(&self) -> usize {
        self.block.len()
    }

    fn max(&self) -> usize {
        1 << (self.local_depth - 1)
    }

    fn mask(&self) -> usize {
        mask(self.local_depth)
    }
}

fn mask(depth: usize) -> usize {
    let mut ret = 0;
    for _ in 0..depth {
        ret <<= 1;
        ret |= 1;
    }
    ret
}

impl Deref for Bucket {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        &self.block
    }
}

impl DerefMut for Bucket {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.block
    }
}

/// The structure that maintains the buckets directory. The user only has control over the bucket size
/// of the structure
pub struct BlockDirectory {
    parent_table: Identifier,
    relationship_definition: RelationDefinition,
    bucket_lock: Lock,
    buckets: UnsafeCell<Vec<Box<Bucket>>>,
    bucket_size: usize,
    global_depth: usize,
    /// Key is the directory hash, value is the location of the index of the corresponding bucket
    directories: RwLock<HashMap<BigUint, usize>>,
    mask: BigUint,
    primary_key_definition: PrimaryKeyDefinition,
}

impl BlockDirectory {
    /// Creates a new block directory with a specified bucket_size
    pub fn new(
        parent_table: Identifier,
        relationship_definition: RelationDefinition,
        bucket_size: usize,
        primary_key_definition: PrimaryKeyDefinition,
    ) -> Self {
        BlockDirectory {
            parent_table,
            relationship_definition,
            bucket_lock: Default::default(),
            buckets: Default::default(),
            bucket_size,
            global_depth: 1,
            directories: Default::default(),
            mask: BigUint::one(),
            primary_key_definition,
        }
    }

    fn hash_tuple(&self, tuple: &Tuple) -> BigUint {
        let primary_key = self.get_primary_key_of_tuple(tuple);
        primary_key.hash()
    }

    fn get_primary_key_of_tuple<'a>(&self, tuple: &'a Tuple) -> PrimaryKey<'a> {
        let definition = &self.primary_key_definition;
        let ret = tuple
            .iter()
            .enumerate()
            .filter(|(pos, _)| definition.contains(pos))
            .map(|(_, val)| val)
            .collect();
        PrimaryKey::new(ret, definition.create_seeds())
    }

    fn generate_mask(&mut self) {
        let mut mask = BigUint::zero();
        for _ in 0..self.global_depth {
            mask <<= 1;
            mask |= BigUint::one();
        }
        self.mask = mask;
    }

    fn get_directory(&self, hash: &BigUint) -> BigUint {
        hash.bitand(&self.mask)
    }

    pub(super) fn buckets(&self) -> (&Vec<Box<Bucket>>, LockRead) {
        unsafe {
            let read = self.bucket_lock.read();
            (&*self.buckets.get(), read)
        }
    }

    pub(super) fn buckets_mut(&self) -> (&mut Vec<Box<Bucket>>, LockWrite) {
        unsafe {
            let write = self.bucket_lock.write();
            (&mut *self.buckets.get(), write)
        }
    }

    pub(super) fn bucket(&self, index: usize, _read: &LockRead<'_>) -> Option<&Box<Bucket>> {
        if index >= self.bucket_count() {
            return None;
        }

        unsafe { (*self.buckets.get()).get(index) }
    }

    pub(super) fn bucket_mut(
        &self,
        index: usize,
        _write: &mut LockWrite<'_>,
    ) -> Option<&mut Box<Bucket>> {
        if index >= self.bucket_count() {
            return None;
        }

        unsafe { (*self.buckets.get()).get_mut(index) }
    }

    /// Creates a new block and returns its id/index
    fn create_new_bucket(&self, local_depth: usize) -> usize {
        let (mut buckets, _lock) = self.buckets_mut();
        let id = buckets.len();
        let block = Block::new(
            self.parent_table.clone(),
            id,
            self.relationship_definition.clone(),
        );
        let bucket = Bucket {
            local_depth,
            block,
            mask: mask(local_depth).to_biguint().unwrap(),
        };

        buckets.push(Box::new(bucket));
        id
    }

    /// Expand the directory
    fn expand_directory(&mut self) {
        {
            let mut lock = self.directories.write().unwrap();
            let mut new_hash_map = HashMap::with_capacity(lock.len() * 2);
            for (key, value) in lock.iter() {
                let new_key1 = key.clone();
                let new_key2 = key.clone() | (BigUint::one() << self.global_depth);
                new_hash_map.insert(new_key1, *value);
                new_hash_map.insert(new_key2, *value);
            }

            *lock = new_hash_map;
        }
        self.global_depth += 1;
        self.generate_mask();
    }

    fn split_bucket(&mut self, bucket_index: usize, directory_number: &BigUint) {
        // println!("[BEFORE split] {:?}", self);
        let (new_block_index, tuples, local_depth) = {
            {
                let expand = {
                    let (mut buckets, _lock) = self.buckets();
                    let bucket = &buckets[bucket_index];
                    bucket.local_depth == self.global_depth
                };
                if expand {
                    self.expand_directory();
                }
            }
            let (mut buckets, lock) = self.buckets_mut();
            let bucket = &mut buckets[bucket_index];
            bucket.local_depth += 1;
            let local_depth = bucket.local_depth;

            let mut in_use = bucket.get_contents_mut();
            let mut tuples = in_use.take_all();
            std::mem::drop(in_use);
            std::mem::drop(lock);
            (self.create_new_bucket(local_depth), tuples, local_depth)
        };

        {
            let small_mask = mask(local_depth - 1).to_biguint().unwrap();
            let original_real_check = small_mask & directory_number;

            let higher_directory_check =
                original_real_check | (BigUint::one() << (local_depth - 1));
            let mut directories = self.directories.write().unwrap();
            let local_mask = mask(local_depth);

            /*
            let local_mask =
            let mut rewrite =
            for key in directories.keys() {
                if &(key & directory_number) == key {

                }
            }

             */

            for dir in directories.iter_mut() {
                let masked_local = dir.0 & BigUint::from(local_mask);
                //let check = &masked & &higher_directory_check;
                if &masked_local == &higher_directory_check && dir.1 == &bucket_index {
                    *dir.1 = new_block_index;
                }
            }

            //directories.insert(higher_directory_check, new_block_index);
        }
        //println!("[DURING split] {:?}", self);
        let (mut buckets, _lock) = self.buckets_mut();

        for tuple in tuples {
            let hash = self.hash_tuple(&tuple);

            let dir = self.get_directory(&hash);
            let bucket_from_dir = self.directories.read().unwrap().get(&dir).cloned().unwrap();
            let as_usize = bucket_from_dir.to_usize().unwrap();
            let bucket = &mut buckets[as_usize];
            /*
            let bucket = if masked == bucket_index {
                &mut buckets[bucket_index]
            } else if masked == new_block_index {
                &mut buckets[new_block_index]
            } else {
                panic!("Something in the split bucket function went wrong when determining what bucket to put something in")
            };

             */
            let mut use_mut = bucket.get_contents_mut();
            use_mut.insert_tuple(hash, tuple);
        }
        // println!("[AFTER split] {:#?}", self);
    }

    fn get_bucket_num(&self, directory: &BigUint) -> Option<usize> {
        let lock = self.directories.read().unwrap();
        let bucket_option = lock.get(directory);
        bucket_option.map(|u| *u)
    }

    fn get_bucket_from_directory(&self, directory: BigUint) -> &Bucket {
        let bucket_option = self.get_bucket_num(&directory);
        {
            if let Some(bucket) = bucket_option {
                unsafe {
                    let (bucket_lock, _lock) = self.buckets();
                    let boxed = &*bucket_lock[bucket] as *const Bucket;

                    return &*boxed;
                }
            }
        }
        let mut lock = self.directories.write().unwrap();
        let new_bucket = self.create_new_bucket(1);
        lock.insert(directory, new_bucket);
        let (buckets, _lock) = self.buckets();
        unsafe {
            let boxed = &*buckets[new_bucket] as *const Bucket;

            return &*boxed;
        }
    }

    fn get_bucket_from_directory_mut(&mut self, directory: BigUint) -> &mut Bucket {
        {
            let lock = self.directories.read().unwrap();
            let bucket_option = lock.get(&directory);
            if let Some(bucket) = bucket_option {
                unsafe {
                    let (mut bucket_lock, _lock) = self.buckets_mut();
                    let boxed = &mut *bucket_lock[*bucket] as *mut Bucket;

                    return &mut *boxed;
                }
            }
        }
        let mut lock = self.directories.write().unwrap();
        let new_bucket = self.create_new_bucket(1);
        lock.insert(directory, new_bucket);
        let (buckets, _lock) = self.buckets_mut();
        unsafe {
            let boxed = &mut *buckets[new_bucket] as *mut Bucket;

            return &mut *boxed;
        }
    }

    pub fn insert(&mut self, tuple: Tuple, full_hash: BigUint) -> Option<Tuple> {
        let (bucket, directory_number) = {
            let directory_number = self.get_directory(&full_hash);
            let bucket_size = self.bucket_size;
            let bucket = self.get_bucket_from_directory(directory_number.clone());
            let len = bucket.len();
            if len == bucket_size {
                // Overflow!
                let bucket_num = self.get_bucket_num(&directory_number).unwrap();
                self.split_bucket(bucket_num, &directory_number);
                return self.insert(tuple, full_hash);
            } else {
                // easy insert
                let bucket = self.get_bucket_from_directory_mut(directory_number.clone());
                (bucket, directory_number)
            }
        };
        //let bucket = self.get_bucket_from_directory_mut(directory_number.clone());

        let ret = {
            let mut in_use = bucket.block.get_contents_mut();
            in_use.insert_tuple(full_hash, tuple)
        };
        if ret.is_none() {
            //*bucket.len_mut() += 1;
            if bucket.len() > self.bucket_size {
                panic!(
                    "Added too many tuples to bucket {}",
                    self.get_bucket_num(&directory_number).unwrap()
                )
            }
        }
        ret
    }

    pub(super) fn get_bucket_for_primary_key(&self, full_hash: BigUint) -> &Bucket {
        let directory_number = self.get_directory(&full_hash);
        self.get_bucket_from_directory(directory_number.clone())
    }

    pub fn bucket_count(&self) -> usize {
        self.buckets().0.len()
    }

    pub unsafe fn len_unsafe(&self) -> usize {
        let mut output = 0;

        let buckets = &*self.buckets.get();
        for b in buckets {
            output += b.block.len()
        }

        output
    }

    pub fn len(&self) -> usize {
        let mut output = 0;

        let (buckets, _) = self.buckets();
        for b in buckets {
            output += b.block.len()
        }

        output
    }
}

pub struct StoredTupleIterator<'a> {
    buffer: VecDeque<Tuple>,
    bucket_num: usize,
    max_block_num: usize,
    directory: &'a BlockDirectory,
    read: LockRead<'a>,
}

impl<'a> StoredTupleIterator<'a> {
    fn new(directory: &'a BlockDirectory) -> Self {
        let read = directory.bucket_lock.read();
        let max_block_num = directory.bucket_count();

        StoredTupleIterator {
            buffer: Default::default(),
            bucket_num: 0,
            max_block_num,
            directory,
            read,
        }
    }
}

impl<'a> Iterator for StoredTupleIterator<'a> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() && self.bucket_num >= self.max_block_num {
            return None;
        }

        while self.buffer.is_empty() && self.bucket_num < self.max_block_num {
            let block = self.directory.bucket(self.bucket_num, &self.read).unwrap();
            let contents = block.get_contents();
            for tuple in contents.all() {
                self.buffer.push_back(tuple.clone())
            }
            self.bucket_num += 1;
        }
        self.buffer.pop_front()
    }
}

impl<'a> IntoIterator for &'a BlockDirectory {
    type Item = Tuple;
    type IntoIter = StoredTupleIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        StoredTupleIterator::new(self)
    }
}

impl Rename<Identifier> for BlockDirectory {
    fn rename(&mut self, name: Identifier) {
        self.parent_table = name;
    }
}

impl Debug for BlockDirectory {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{} Block Directory {{", self.parent_table)?;
        unsafe {
            writeln!(f, "\tLen = {}", self.len_unsafe())?;
        }
        writeln!(f, "\tGlobal Depth = {}", self.global_depth)?;
        writeln!(f, "\tMask = {:b}", self.mask)?;
        writeln!(f, "\tBucket Size = {}", self.bucket_size)?;
        writeln!(f, "\tDirectories:")?;
        let guard = self.directories.read().unwrap();
        for (key, value) in &*guard {
            writeln!(f, "\t\t{:b} -> {}", key, value)?;
        }
        writeln!(f, "\tBuckets:")?;
        let buckets = unsafe { &*self.buckets.get() };
        for (index, bucket) in buckets.iter().enumerate() {
            write!(
                f,
                "\t\tBucket {}: Length={} Local Depth={}",
                index,
                bucket.len(),
                bucket.local_depth,
            )?;
            if f.alternate() {
                writeln!(f, " Contents {{ ")?;
                let content = bucket.get_contents();
                for tuple in content.all_with_key() {
                    writeln!(f, "\t\t\t{}: {}", tuple.0, tuple.1)?;
                }
                writeln!(f, "\t\t}}")?;
            } else {
                writeln!(f)?;
            }
        }
        write!(f, "}}")
    }
}

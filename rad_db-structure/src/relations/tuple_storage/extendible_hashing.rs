use crate::identifier::Identifier;
use crate::key::primary::{PrimaryKey, PrimaryKeyDefinition};
use crate::relations::tuple_storage::block::{Block, InUse};
use crate::relations::tuple_storage::lock::{Lock, LockRead, LockWrite};
use crate::relations::RelationDefinition;
use crate::tuple::Tuple;
use num_bigint::{BigUint, ToBigUint};
use num_traits::{One, ToPrimitive, Zero};
use std::cell::UnsafeCell;
use std::cmp::min;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{RwLock, RwLockReadGuard};

/// A local bucket that contains information on the local block
struct Bucket {
    local_depth: usize,
    block: Block,
    len: usize,
    mask: BigUint,
}

impl Bucket {
    fn len(&self) -> usize {
        self.len
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
        PrimaryKey::new(ret)
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
        hash & &self.mask
    }

    fn buckets(&self) -> (&Vec<Box<Bucket>>, LockRead) {
        unsafe {
            let read = self.bucket_lock.read();
            (&*self.buckets.get(), read)
        }
    }

    fn buckets_mut(&self) -> (&mut Vec<Box<Bucket>>, LockWrite) {
        unsafe {
            let write = self.bucket_lock.write();
            (&mut *self.buckets.get(), write)
        }
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
            len: 0,
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
        let (new_block_index, tuples, local_depth) = {
            {
                let (mut buckets, _lock) = self.buckets();
                let bucket = &buckets[bucket_index];
                if bucket.local_depth == self.global_depth {
                    // Directory expansion
                    self.expand_directory();
                }
            }
            let (mut buckets, _lock) = self.buckets_mut();
            let bucket = &mut buckets[bucket_index];
            bucket.local_depth += 1;
            let local_depth = bucket.local_depth;

            let mut in_use = bucket.get_contents_mut();
            let mut tuples = in_use.take_all();

            (self.create_new_bucket(local_depth), tuples, local_depth)
        };

        {
            let higher_directory = directory_number | (BigUint::one() << (local_depth - 1));
            let mut directories = self.directories.write().unwrap();
            directories.insert(higher_directory, new_block_index);
        }

        let (mut buckets, _lock) = self.buckets_mut();

        let mask = mask(local_depth);
        for tuple in tuples {
            let hash = self.hash_tuple(&tuple);
            let masked: BigUint = hash & mask.to_biguint().unwrap();
            let masked = masked.to_usize().unwrap();
            let bucket = if masked == bucket_index {
                &mut buckets[bucket_index]
            } else if masked == new_block_index {
                &mut buckets[new_block_index]
            } else {
                panic!("Something in the split bucket function went wrong when determining what bucket to put something in")
            };
            let mut use_mut = bucket.get_contents_mut();
            use_mut.insert_tuple(masked, tuple);
        }
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
        let directory_number = self.get_directory(&full_hash);
        let bucket_size = self.bucket_size;
        let bucket = self.get_bucket_from_directory_mut(directory_number.clone());

        if bucket.len() == bucket_size {
            // Overflow!
            let bucket_num = self.get_bucket_num(&directory_number).unwrap();
            self.split_bucket(bucket_num, &directory_number);
            self.insert(tuple, full_hash)
        } else {
            // easy insert
            let masked = (&bucket.mask & full_hash).to_usize().unwrap();
            let mut in_use = bucket.block.get_contents_mut();
            let replaced_opt = in_use.insert_tuple(masked, tuple);
            if replaced_opt.is_none() {
                bucket.len += 1;
            }
            replaced_opt
        }
    }

    pub fn block_count(&self) -> usize {
        self.buckets().0.len()
    }
}

pub struct StorageTupleIterator<'a> {
    bucket_num: usize,
    tuple_num: usize,
    in_use: Option<InUse<'a>>,
    directory: &'a BlockDirectory,
    read: LockRead<'a>,
}

impl<'a> StorageTupleIterator<'a> {
    fn new(directory: &'a BlockDirectory) -> Self {
        let read = directory.bucket_lock.read();
        StorageTupleIterator {
            bucket_num: 0,
            tuple_num: 0,
            in_use: None,
            directory,
            read,
        }
    }
}

impl<'a> Iterator for StorageTupleIterator<'a> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.bucket_num >= self.directory.block_count() {
            return None;
        }

        if let Some(current_iterator) = &mut self.current_iterator {
            if let Some(next) = current_iterator.next() {
                return Some(next);
            } else {
                self.current_iterator = None;
            }
        }

        let buckets = unsafe { &*self.directory.buckets.get() };

        while let Some(bucket) = buckets.get(self.bucket_num) {
            self.in_use = Some(bucket.block.get_contents());
            if let Some(in_use) = &self.in_use {
                let mut next_iterator = in_use.all();
                let ret = next_iterator.next();
                self.bucket_num += 1;
                if let Some(ret) = ret {
                    self.current_iterator = Some(Box::new(next_iterator));
                    Some(ret.clone())
                }
            } else {
                unreachable!()
            }
        }

        None
    }
}

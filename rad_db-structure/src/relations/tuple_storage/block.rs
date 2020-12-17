use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::Hasher;
use std::io::Write;
use std::io::{BufRead, BufReader, BufWriter};
use std::iter::FilterMap;
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::path::PathBuf;
use std::ptr::null_mut;
use std::str::FromStr;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender, TryRecvError};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::Duration;
use thread::JoinHandle;

use memmap::{Mmap, MmapMut};

use rad_db_types::deserialization::parse_using_types;
use rad_db_types::serialization::serialize_values;
use rad_db_types::Type;

use crate::identifier::Identifier;
use crate::relations::RelationDefinition;
use crate::tuple::Tuple;

pub const NUM_TUPLES_PER_BLOCK: usize = 512;

pub struct Block {
    parent_table: Identifier,
    relationship_definition: RelationDefinition,
    block_num: usize,
    block_contents: Option<BlockContents>,
    usage: RwLock<()>,
    reads: AtomicUsize,
}

#[derive(Debug)]
pub struct ReadInUseError;

impl Display for ReadInUseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not get readable contents of this block")
    }
}

impl Error for ReadInUseError {}

impl From<PoisonError<RwLockReadGuard<'_, ()>>> for ReadInUseError {
    fn from(_: PoisonError<RwLockReadGuard<'_, ()>>) -> Self {
        ReadInUseError
    }
}

#[derive(Debug)]
pub struct WriteInUseError;

impl Display for WriteInUseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not get writable contents of this block")
    }
}

impl Error for WriteInUseError {}

impl From<PoisonError<RwLockWriteGuard<'_, ()>>> for WriteInUseError {
    fn from(_: PoisonError<RwLockWriteGuard<'_, ()>>) -> Self {
        WriteInUseError
    }
}

impl Block {
    pub fn new(
        parent_table: Identifier,
        block_num: usize,
        relationship_definition: RelationDefinition,
    ) -> Self {
        let ret = Block {
            parent_table,
            relationship_definition,
            block_num,
            block_contents: None,
            usage: RwLock::new(()),
            reads: Default::default(),
        };
        ret.initialize_file();
        ret
    }

    fn initialize_file(&self) -> std::io::Result<()> {
        let file_name = self.file_name();
        if file_name.exists() {
            return Ok(());
        }

        &OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(file_name)?;

        Ok(())
    }

    /// Gets immutable access to the contents of the block
    pub fn get_contents(&self) -> InUse {
        self.try_get_contents().unwrap()
    }

    pub fn try_get_contents(&self) -> Result<InUse, ReadInUseError> {
        let read_guard = self.usage.read()?;
        self.reads.fetch_add(1, Ordering::Acquire);
        if self.block_contents.is_none() {
            unsafe {
                self.load();
            }
        }
        let ret = InUse {
            parent: self,
            read: read_guard,
        };
        Ok(ret)
    }

    /// Attempts to get immutable access to the contents of the block
    pub fn get_contents_mut(&mut self) -> InUseMut {
        self.try_get_contents_mut().unwrap()
    }

    pub fn try_get_contents_mut(&mut self) -> Result<InUseMut, WriteInUseError> {
        let write_copy = (self as *mut Self);
        let write_guard = self.usage.write()?;
        if self.block_contents.is_none() {
            unsafe {
                self.load();
            }
        }
        unsafe {
            let ret = InUseMut {
                parent: &mut *write_copy,
                write: write_guard,
            };
            Ok(ret)
        }
    }

    fn file_name(&self) -> PathBuf {
        let mut ret = PathBuf::new();
        for name in &self.parent_table {
            ret.push(name);
        }
        ret.push(format!("block_{}.txt", self.block_num));
        ret
    }

    unsafe fn load(&self) {
        let path = self.file_name();
        std::fs::create_dir_all(&path);
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .open(&path)
            .expect("Could not open file");

        let mut buf_reader = BufReader::new(&file);
        let mut tuples = vec![];
        loop {
            let mut str = String::new();
            match buf_reader.read_line(&mut str) {
                Err(_) => {
                    panic!("Couldn't read block form file")
                }
                Ok(0) => break,
                Ok(_) => {
                    if str == "NONE" {
                        tuples.push(None);
                    } else {
                        let tuple = Tuple::new(
                            parse_using_types(str, &self.relationship_definition)
                                .expect("Could not parse type")
                                .into_iter(),
                        );
                        tuples.push(Some(tuple));
                    }
                }
            }
        }

        let contents = BlockContents {
            relationship: self.relationship_definition.clone(),
            file,
            internal: tuples,
        };
        unsafe {
            let mutable = self as *const Self as *mut Self;
            (*mutable).block_contents = Some(contents);
        }
    }

    unsafe fn unload(&self) {
        let unsafe_self = self as *const Self as *mut Self;

        let replaced = std::mem::replace(&mut (*unsafe_self).block_contents, None);
        if let Some(contents) = replaced {
            let BlockContents { file, internal, .. } = contents;
            file.set_len(0);
            let mut buf_writer = BufWriter::new(file);
            for tuple in internal {
                match tuple {
                    Some(tuple) => writeln!(buf_writer, "{}", serialize_values(tuple.into_iter())),
                    None => writeln!(buf_writer, "NONE"),
                };
            }
        }
    }
}

pub struct InUse<'a> {
    parent: &'a Block,
    read: RwLockReadGuard<'a, ()>,
}

impl Deref for InUse<'_> {
    type Target = BlockContents;

    fn deref(&self) -> &Self::Target {
        if self.parent.block_contents.is_none() {
            unsafe {
                self.parent.load();
            }
        }
        self.parent.block_contents.as_ref().unwrap()
    }
}

impl Drop for InUse<'_> {
    fn drop(&mut self) {
        if self.parent.reads.fetch_sub(1, Ordering::Acquire) == 1 {
            unsafe { self.parent.unload() }
        }
    }
}

pub struct InUseMut<'a> {
    parent: &'a mut Block,
    write: RwLockWriteGuard<'a, ()>,
}

impl Drop for InUseMut<'_> {
    fn drop(&mut self) {
        unsafe { self.parent.unload() }
    }
}

impl Deref for InUseMut<'_> {
    type Target = BlockContents;

    fn deref(&self) -> &Self::Target {
        self.parent.block_contents.as_ref().unwrap()
    }
}

impl DerefMut for InUseMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.parent.block_contents.as_mut().unwrap()
    }
}

pub struct BlockContents {
    relationship: RelationDefinition,
    file: File,
    internal: Vec<Option<Tuple>>,
}

impl BlockContents {
    pub fn get_tuple(&self, index: usize) -> Option<&Tuple> {
        self.internal[index].as_ref()
    }

    pub fn get_tuple_mut(&mut self, index: usize) -> Option<&mut Tuple> {
        self.internal[index].as_mut()
    }

    pub fn insert_tuple(&mut self, index: usize, tuple: Tuple) -> Option<Tuple> {
        std::mem::replace(&mut self.internal[index], Some(tuple))
    }

    pub fn remove_tuple(&mut self, index: usize) -> Option<Tuple> {
        std::mem::replace(&mut self.internal[index], None)
    }

    pub fn all(&self) -> impl Iterator<Item = &Tuple> {
        self.internal.iter().filter_map(|item| item.as_ref())
    }

    pub fn all_mut(&mut self) -> impl Iterator<Item = &mut Tuple> {
        self.internal.iter_mut().filter_map(|item| item.as_mut())
    }
}

impl Index<usize> for BlockContents {
    type Output = Tuple;

    fn index(&self, index: usize) -> &Self::Output {
        match self.internal[index].as_ref() {
            None => {
                panic!("{} out of bounds", index)
            }
            Some(r) => r,
        }
    }
}

impl IndexMut<usize> for BlockContents {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        match self.internal[index].as_mut() {
            None => {
                panic!("{} out of bounds", index)
            }
            Some(r) => r,
        }
    }
}

impl<'a> IntoIterator for &'a BlockContents {
    type Item = &'a Tuple;
    type IntoIter = <Vec<&'a Tuple> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.all().collect::<Vec<&'a Tuple>>().into_iter()
    }
}

impl<'a> IntoIterator for &'a mut BlockContents {
    type Item = &'a mut Tuple;
    type IntoIter = <Vec<&'a mut Tuple> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.all_mut().collect::<Vec<&'a mut Tuple>>().into_iter()
    }
}

use std::fmt::{Debug, Formatter};
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut, Index, Shr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use rad_db_types::Type;

use crate::identifier::Identifier;
use crate::key::primary::PrimaryKeyDefinition;
use crate::relations::tuple_storage::{BlockIterator, StoredTupleIterator, TupleStorage};
use crate::relations::AsTypeList;
use crate::tuple::Tuple;
use crate::Rename;

pub struct Relation {
    name: Identifier,
    attributes: Vec<(String, Type)>,
    primary_key: PrimaryKeyDefinition,
    backing_table: TupleStorage,
}

impl Relation {
    /// Creates a new relation that will save it's contents into the file system
    pub fn new<S: ToString, I: IntoIterator<Item = (S, Type)>>(
        name: Identifier,
        attributes: I,
        bucket_size: usize,
        primary_key: PrimaryKeyDefinition,
    ) -> Self {
        let attributes: Vec<(String, Type)> = attributes
            .into_iter()
            .map(|(s, ty)| (s.to_string(), ty))
            .collect();
        let definition: Vec<_> = attributes
            .clone()
            .into_iter()
            .map(|(string, ty)| (Identifier::with_parent(&name, string), ty))
            .collect();
        let definition = RelationDefinition::new(definition);
        let backing_table =
            Relation::generate_tuple_storage(&name, bucket_size, &primary_key, definition);
        Relation {
            name,
            attributes,
            primary_key,
            backing_table,
        }
    }

    /// Creates a relation that only lasts for as long as the program runs
    pub fn new_volatile<S: ToString, I: IntoIterator<Item = (S, Type)>>(
        name: Identifier,
        attributes: I,
        bucket_size: usize,
        primary_key: PrimaryKeyDefinition,
    ) -> Self {
        let attributes: Vec<(String, Type)> = attributes
            .into_iter()
            .map(|(s, ty)| (s.to_string(), ty))
            .collect();
        let definition: Vec<_> = attributes
            .clone()
            .into_iter()
            .map(|(string, ty)| (Identifier::with_parent(&name, string), ty))
            .collect();
        let definition = RelationDefinition::new(definition);
        let backing_table =
            TupleStorage::new_volatile(name.clone(), definition,primary_key.clone(), bucket_size);
        Relation {
            name,
            attributes,
            primary_key,
            backing_table,
        }
    }

    fn generate_tuple_storage(
        name: &Identifier,
        bucket_size: usize,
        primary_key: &PrimaryKeyDefinition,
        definition: RelationDefinition,
    ) -> TupleStorage {
        TupleStorage::new(name.clone(), definition, primary_key.clone(), bucket_size)
    }

    /// Loads the relation from memory
    pub fn load_from_memory(id: Identifier) -> Self {
        unimplemented!()
    }

    /// Gets the name of the relation
    pub fn name(&self) -> &Identifier {
        &self.name
    }

    /// Gets the name and types of the relation
    pub fn attributes(&self) -> &Vec<(String, Type)> {
        &self.attributes
    }

    /// Gets the primary key definition of the relation
    pub fn primary_key(&self) -> &PrimaryKeyDefinition {
        &self.primary_key
    }

    /// Gets the amount of tuples in the relation
    pub fn len(&self) -> usize {
        self.backing_table.len()
    }

    /// Gets whether there are tuples in the relation or not
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get_relation_definition(&self) -> RelationDefinition {
        let mut ret = Vec::new();
        for (name, ty) in &self.attributes {
            let identifier = Identifier::with_parent(&self.name, name);
            ret.push((identifier, ty.clone()));
        }
        RelationDefinition::new(ret)
    }

    /// Gets a [StoredTupleIterator] for the tuple storage
    ///
    /// [StoredTupleIterator]: tuple_storage::StoredTupleIterator
    pub fn tuples(&self) -> StoredTupleIterator {
        self.backing_table.all_tuples()
    }

    /// Gets a [BlockIterator] for the tuple storage
    ///
    /// [BlockIterator]: tuple_storage::BlockIterator
    pub fn blocks(&self) -> BlockIterator {
        self.backing_table.blocks()
    }

    /// Makes the relation temporary, so that it's contents are deleted from the
    /// file system after the relation drops
    pub fn into_temp(self) -> TempRelation {
        TempRelation::new(self)
    }

    pub fn insert(&mut self, tuple: Tuple) {
        self.backing_table.insert(tuple);
    }

    pub fn get_field_index<I : Into<Identifier>>(&self, identifier: I) -> Option<usize> {
        self.get_field_index_of_identifier(identifier.into())
    }

    fn get_field_index_of_identifier(&self, identifier: Identifier) -> Option<usize> {
        let field_name =
            match identifier.parent() {
                None => {
                    identifier.base()
                }
                Some(parent) => {
                    if parent == &self.name {
                        identifier.base()
                    } else {
                        return None;
                    }
                }
            };

        self.attributes
            .iter()
            .map(|(id, _)| id)
            .position(|id| id == field_name)
    }
}

impl<I: Into<Identifier>> Rename<I> for Relation {
    fn rename(&mut self, name: I) {
        self.name = name.into();
        self.backing_table.rename(self.name.clone())
    }
}

impl AsTypeList for Relation {
    fn to_type_list(&self) -> Vec<Type> {
        self.attributes.iter().map(|(_, t)| t).cloned().collect()
    }
}

/// A structure representing the actual names and types of a relation
#[derive(Debug, Clone)]
pub struct RelationDefinition {
    attributes: Vec<(Identifier, Type)>,
}

impl RelationDefinition {
    pub fn new(attributes: Vec<(Identifier, Type)>) -> Self {
        RelationDefinition { attributes }
    }

    /// Gets the minimum number of id levels
    ///
    /// # Example
    /// A list of parent::child1, child2 would have a minimum of 1
    fn min_id_length(&self) -> usize {
        let mut min = None;
        for (id, _) in &self.attributes {
            match min {
                None => min = Some(id.len()),
                Some(old_len) => {
                    let len = id.len();
                    if old_len < len {
                        min = Some(len)
                    }
                }
            }
        }
        min.expect("Identifier can not have zero length, so this can't happen")
    }

    /// Checks if all the ids are of the same length
    fn all_id_len_same(&self) -> bool {
        let mut value = None;
        for (id, _) in &self.attributes {
            match value {
                None => value = Some(id.len()),
                Some(val) => {
                    if val != id.len() {
                        return false;
                    }
                }
            }
        }
        true
    }

    pub fn strip_highest_prefix(&self) -> Option<RelationDefinition> {
        if self.all_id_len_same() {
            let vec: Vec<_> = self
                .attributes
                .iter()
                .map(|(id, ty)| (id.strip_highest_parent(), ty.clone()))
                .filter_map(|(id, ty)| match id {
                    None => None,
                    Some(id) => Some((id, ty)),
                })
                .collect();
            if vec.is_empty() {
                None
            } else {
                Some(RelationDefinition::new(vec))
            }
        } else {
            let min = self.min_id_length();
            let vec: Vec<_> = self
                .attributes
                .iter()
                .map(|(id, ty)| {
                    if id.len() > min {
                        (id.strip_highest_parent(), ty.clone())
                    } else {
                        (Some(id.clone()), ty.clone())
                    }
                })
                .filter_map(|(id, ty)| match id {
                    None => None,
                    Some(id) => Some((id, ty)),
                })
                .collect();
            if vec.is_empty() {
                None
            } else {
                Some(RelationDefinition::new(vec))
            }
        }
    }

    pub fn identifier_iter(&self) -> impl IntoIterator<Item = &Identifier> {
        self.attributes.iter().map(|(id, _)| id)
    }

    pub fn len(&self) -> usize {
        self.attributes.len()
    }
}

impl FromIterator<(Identifier, Type)> for RelationDefinition {
    fn from_iter<T: IntoIterator<Item = (Identifier, Type)>>(iter: T) -> Self {
        Self::new(iter.into_iter().collect())
    }
}

impl FromIterator<(String, Type)> for RelationDefinition {
    fn from_iter<T: IntoIterator<Item = (String, Type)>>(iter: T) -> Self {
        Self::from_iter(iter.into_iter().map(|(id, v)| (Identifier::new(id), v)))
    }
}

impl Index<usize> for RelationDefinition {
    type Output = (Identifier, Type);

    fn index(&self, index: usize) -> &Self::Output {
        &self.attributes[index]
    }
}

impl Index<Identifier> for RelationDefinition {
    type Output = Type;

    fn index(&self, index: Identifier) -> &Self::Output {
        for (id, ty) in &self.attributes {
            if id == &index {
                return ty;
            }
        }
        panic!("Index \"{}\" out of bounds", index)
    }
}

impl Shr<usize> for RelationDefinition {
    type Output = RelationDefinition;

    fn shr(self, rhs: usize) -> Self::Output {
        let mut ret = self;
        for _ in 0..rhs {
            match ret.strip_highest_prefix() {
                None => {}
                Some(next) => ret = next,
            }
        }
        ret
    }
}

impl PartialEq for RelationDefinition {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        let mut zip = self
            .identifier_iter()
            .into_iter()
            .zip(other.identifier_iter());

        for (left, right) in zip {
            if left != right {
                return false;
            }
        }

        true
    }
}

impl IntoIterator for &RelationDefinition {
    type Item = Type;
    type IntoIter = <Vec<Type> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let ret: Vec<_> = self.attributes.iter().map(|(_, ty)| ty.clone()).collect();
        ret.into_iter()
    }
}

/// A relation that automatically destroys all saved data
#[repr(transparent)]
#[derive(Debug)]
pub struct TempRelation(Relation);

static TEMP_COUNT: AtomicUsize = AtomicUsize::new(0);

impl TempRelation {
    pub fn new(mut relation: Relation) -> Self {
        let id = TEMP_COUNT.fetch_add(1, Ordering::Relaxed);
        let name = format!("temp{}", id);
        let fixed = Identifier::concat(name, &relation.name);
        relation.rename(fixed);
        std::fs::remove_dir_all(PathBuf::from(&relation.name));
        Self(relation)
    }
}

impl Deref for TempRelation {
    type Target = Relation;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for TempRelation {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for TempRelation {
    fn drop(&mut self) {
        let skeleton = self.backing_table.to_skeleton();
        let internals = std::mem::replace(&mut self.backing_table, skeleton);
        std::mem::drop(internals);
        let mut file = PathBuf::from("DB_STORAGE");
        file.push(PathBuf::from(&self.name));
        std::fs::remove_dir_all(file.parent().unwrap()).unwrap();
    }
}

impl Debug for Relation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "{:#?}", self.backing_table)
        } else {
            write!(f, "{:?}", self.backing_table)
        }
    }
}

#[cfg(test)]
mod tests {
    use rad_db_types::{Numeric, Unsigned};

    use super::*;

    #[test]
    fn empty_relation() {
        let relation = Relation::new(
            Identifier::new("test"),
            vec![("field1", Type::from(0u8))],
            4,
            PrimaryKeyDefinition::new(vec![0]),
        )
        .into_temp();
        let mut iterator = relation.tuples();
        assert!(iterator.next().is_none());
    }

    #[test]
    fn add_one() {
        let mut relation = Relation::new(
            Identifier::new("test"),
            vec![("field1", Type::from(0u8))],
            4,
            PrimaryKeyDefinition::new(vec![0]),
        )
        .into_temp();
        relation
            .backing_table
            .insert(Tuple::new(vec![3u8.into()].into_iter()));
        let mut iterator = relation.tuples();
        let next = iterator.next();
        assert!(next.is_some());
    }

    #[test]
    fn add_many() {
        let mut relation = Relation::new(
            Identifier::new("test"),
            vec![("field1", Type::from(0u8))],
            7,
            PrimaryKeyDefinition::new(vec![0]),
        ) //;
        .into_temp();
        let mut sum = 0usize;
        for i in 0..128u8 {
            sum += i as usize;
            relation.backing_table.insert(Tuple::from_iter(&[i.into()]));
        }
        let mut iterator = relation.tuples();
        assert_eq!(iterator.size_hint(), (128, Some(128)));
        let calc_sum: usize = iterator
            .map(|t| t[0].clone())
            .filter_map(|ty| {
                if let Type::Numeric(Numeric::Unsigned(Unsigned::Byte(ret))) = ty {
                    Some(ret)
                } else {
                    None
                }
            })
            .map(|i| i as usize)
            .sum();

        println!("{:#?}", relation);
        assert_eq!(calc_sum, sum);
    }

    #[test]
    fn add_many_random() {
        let mut relation = Relation::new(
            Identifier::new("test"),
            vec![("field1", Type::from(0u64))],
            64,
            PrimaryKeyDefinition::new(vec![0]),
        )
        .into_temp();
        let mut sum = 0u64;
        for i in 0..2048 {
            println!("Inserting tuple {}", i);
            let random: u32 = rand::random();
            let random = random as u64;
            sum += random;
            relation
                .backing_table
                .insert(Tuple::from_iter(&[random.into()]));
        }
        let mut iterator = relation.tuples();
        let calc_sum: u64 = iterator
            .map(|t| t[0].clone())
            .filter_map(|ty| {
                if let Type::Numeric(Numeric::Unsigned(Unsigned::Long(ret))) = ty {
                    Some(ret)
                } else {
                    None
                }
            })
            .sum();

        println!("{:#?}", relation);
        assert_eq!(calc_sum, sum);
    }

    #[test]
    fn add_large_random() {
        let mut relation = Relation::new(
            Identifier::new("test"),
            vec![("field1", Type::from(0u64))],
            128,
            PrimaryKeyDefinition::new(vec![0]),
        )
        .into_temp();
        let mut sum = 0u64;
        for i in 0..(1 << 14) {
            println!("Inserting tuple {}", i);
            let random: u32 = rand::random();
            let random = random as u64;
            sum += random;
            relation
                .backing_table
                .insert(Tuple::from_iter(&[random.into()]));
        }
        let mut iterator = relation.tuples();
        let calc_sum: u64 = iterator
            .map(|t| t[0].clone())
            .filter_map(|ty| {
                if let Type::Numeric(Numeric::Unsigned(Unsigned::Long(ret))) = ty {
                    Some(ret)
                } else {
                    None
                }
            })
            .sum();

        println!("{:#?}", relation);
        assert_eq!(calc_sum, sum);
    }

    /// Splits one bucket many times before splitting the other
    #[test]
    fn late_split() {
        for block_size in 1..=64 {
            let mut relation = Relation::new(
                Identifier::new("test"),
                vec![("field1", Type::from(0u64))],
                32,
                PrimaryKeyDefinition::new(vec![0]),
            )
            .into_temp();

            // Insert one even

            relation
                .backing_table
                .insert(Tuple::from_iter(&[Type::from(0u64)]))
                .unwrap();

            // Insert many odds
            for i in 0..32 {
                let value: u64 = 2 * i + 1;
                relation
                    .backing_table
                    .insert(Tuple::from_iter(&[Type::from(value)]))
                    .unwrap();
                //
            }

            // Insert many evens
            for i in 1..32 {
                let value: u64 = 2 * i;
                relation
                    .backing_table
                    .insert(Tuple::from_iter(&[Type::from(value)]))
                    .unwrap();
            }
            assert_eq!(relation.len(), 64);
            println!()
        }
    }
}

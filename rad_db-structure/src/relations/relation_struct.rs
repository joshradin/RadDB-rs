use crate::key::primary::PrimaryKeyDefinition;
use crate::relations::tuple_storage::TupleStorage;
use crate::relations::AsTypeList;
use crate::tuple::Tuple;
use rad_db_types::Type;

#[derive(Debug)]
pub struct Relation {
    name: String,
    attributes: Vec<(String, Type)>,
    primary_key: PrimaryKeyDefinition,
    backing_table: Option<Box<dyn TupleStorage>>,
}

impl Relation {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn attributes(&self) -> &Vec<(String, Type)> {
        &self.attributes
    }
    pub fn primary_key(&self) -> &PrimaryKeyDefinition {
        &self.primary_key
    }

    pub fn with_backing_table<T: 'static + TupleStorage>(mut self) -> Result<Self, ()> {
        self.add_backing_table::<T>().map(|_| self)
    }
    pub fn add_backing_table<T: 'static + TupleStorage>(&mut self) -> Result<(), ()> {
        let definition = self.primary_key().clone();
        let boxed: Box<dyn TupleStorage> = Box::new(T::new(definition));
        if self.backing_table.is_none() {
            self.backing_table = Some(boxed);
            Ok(())
        } else {
            Err(())
        }
    }
}

impl AsTypeList for Relation {
    fn to_type_list(&self) -> Vec<Type> {
        self.attributes.iter().map(|(_, t)| t).cloned().collect()
    }
}

use rad_db_types::Type;
use crate::key::primary::PrimaryKey;
use crate::relations::AsTypeList;

pub struct Relation {
    name: String,
    attributes: Vec<(String, Type)>,
    primary_key: PrimaryKey
}

impl Relation {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn attributes(&self) -> &Vec<(String, Type)> {
        &self.attributes
    }
    pub fn primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }
}

impl AsTypeList for Relation {
    fn to_type_list(&self) -> Vec<Type> {
        self.attributes
            .iter()
            .map(|(_, t)| t)
            .cloned()
            .collect()
    }
}
use rad_db_structure::identifier::Identifier;
use std::collections::HashMap;
use rad_db_structure::relations::Relation;
use std::ops::Deref;

#[derive(Clone)]
pub struct MappedRelation<'r> {
    relation_identifier: Identifier,
    aliased_self: Option<String>,
    aliased_fields: HashMap<String, Identifier>,
    relation: &'r Relation
}

impl<'r> MappedRelation<'r> {

    pub fn new(relation: &'r Relation) -> Self {
        Self {
            relation_identifier: relation.name().clone(),
            aliased_self: None,
            aliased_fields: Default::default(),
            relation
        }
    }

    pub fn alias_relation(mut self, alias: String) -> Self {
        self.aliased_self = Some(alias);
        self
    }

    pub fn add_field_alias<I : Into<Identifier>>(&mut self, field: I, alias: String) {
        self.aliased_fields.insert(alias, field.into());
    }

    pub fn with_field_alias<I: Into<Identifier>>(mut self, field: I, alias: String) -> Self {
        self.add_field_alias(field, alias);
        self
    }

    pub fn contains_field<I : Into<Identifier>>(&self, field: I) -> bool {
        self.get_mapped_field(field).is_some()
    }

    pub fn get_mapped_field<I : Into<Identifier>>(&self, field: I) -> Option<Identifier> {
        let id = field.into();
        match &id.parent() {
            None => {
                if let Some(s) = self.aliased_fields.get(id.base()) {
                    Some(s.clone())
                } else {
                    if let Some(index) = self.relation.get_field_index(id) {
                        self.relation
                            .attributes()
                            .get(index)
                            .map(|(s, _)| Identifier::concat(&self.relation_identifier, s))

                    } else {
                        None
                    }
                }
            }
            Some(parent) => {
                if self.valid_name(*parent) {
                    if self.relation.get_field_index(id.base()).is_some() {
                        Some(id)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    pub fn get_field_index<I : Into<Identifier>>(&self, field: I) -> Option<usize> {
        if let Some(full_name) = self.get_mapped_field(field) {
            self.relation.get_field_index(full_name)
        } else {
            None
        }
    }

    /// Determines if the name is a valid name when referring to the relation
    pub fn valid_name(&self, name: &Identifier) -> bool {
        if name == self.relation.name() {
            return true;
        }

        if name.parent().is_none() && name.base() == self.relation.name().base() {
            return true;
        }

        if name.parent().is_none() &&
            self.aliased_self
                .as_ref()
                .map_or(false, |alias| name.base() == alias) {
            return true;
        }

        false
    }

    pub fn relation(&self) -> &'r Relation {
        self.relation
    }
}




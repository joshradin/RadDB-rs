use rad_db_structure::identifier::Identifier;

#[derive(Debug)]
pub struct JoinCondition {
    left_id: Identifier,
    right_id: Identifier,
}

impl JoinCondition {
    pub fn new(left_id: Identifier, right_id: Identifier) -> Self {
        JoinCondition { left_id, right_id }
    }

    pub fn left_id(&self) -> &Identifier {
        &self.left_id
    }
    pub fn right_id(&self) -> &Identifier {
        &self.right_id
    }
}

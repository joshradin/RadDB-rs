use rad_db_structure::identifier::Identifier;
use rad_db_types::Value;

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

pub enum Operand {
    Id(Identifier),
    SignedNumber(i64),
    UnsignedNumber(u64),
    Float(f64),
    String(String),
}

pub enum ConditionOperation {
    Equals(Operand),
    Nequals(Operand),
    And(Box<ConditionOperation>, Box<Condition>),
    Or(Box<ConditionOperation>, Box<Condition>),
}

pub struct Condition;

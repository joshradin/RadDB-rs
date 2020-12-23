use rad_db_structure::identifier::Identifier;
use rad_db_types::Value;
use std::cmp::min;

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

#[derive(PartialEq, Debug, Clone)]
pub enum Operand {
    Id(Identifier),
    SignedNumber(i64),
    UnsignedNumber(u64),
    Float(f64),
    String(String),
}

#[derive(PartialEq, Debug, Clone)]
pub enum ConditionOperation {
    Equals(Operand),
    Nequals(Operand),
    And(Box<ConditionOperation>, Box<Condition>),
    Or(Box<ConditionOperation>, Box<Condition>),
}

impl ConditionOperation {
    fn selectivity(&self, max_tuples: usize) -> f64 {
        match self {
            ConditionOperation::Equals(_) => 1.0 / max_tuples as f64,
            ConditionOperation::Nequals(_) => 1.0 - 1.0 / max_tuples as f64,
            ConditionOperation::And(c, r) => c.selectivity(max_tuples) * r.selectivity(max_tuples),
            ConditionOperation::Or(c, r) => {
                min(c.selectivity(max_tuples) + r.selectivity(max_tuples), 1.0)
            }
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct Condition {
    base: Identifier,
    operation: ConditionOperation,
}

impl Condition {
    pub fn new<I: Into<Identifier>>(base: I, operation: ConditionOperation) -> Self {
        Condition {
            base: base.into(),
            operation,
        }
    }

    pub fn and(left: Self, right: Self) -> Self {
        let Condition { base, operation } = left;
        Condition::new(
            base,
            ConditionOperation::And(Box::new(operation), Box::new(right)),
        )
    }

    /// Splits a conditional from a list of and statements c<sub>1</sub> AND c_<sub>2</sub> AND ... AND c<sub>n</sub>
    /// into a list of Conditions c<sub>1</sub>, c<sub>2</sub>, ..., c<sub>n</sub>
    pub fn split_and(self) -> Vec<Self> {
        let mut ptr = self;
        let mut output = vec![];
        while let Self {
            base,
            operation: ConditionOperation::And(inner, next),
        } = ptr
        {
            let extracted = Condition::new(base, *inner);
            let flattened = extracted.split_and();
            output.extend(flattened);
            ptr = *next;
        }
        output.push(ptr);
        output
    }

    /// A heuristic that approximates how selective a condition is, where the lower the better
    pub fn selectivity(&self, max_tuples: usize) -> f64 {
        self.operation.selectivity(max_tuples)
    }
}

impl<I: Into<Identifier>> From<I> for Operand {
    fn from(s: I) -> Self {
        Operand::Id(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_and() {
        let base_case = Condition::new("id1", ConditionOperation::Equals(Operand::from("id2")));
        let copy = base_case.clone();
        let split = base_case.split_and();
        assert_eq!(split.len(), 1);
        assert_eq!(split[0], copy);
        let case2 = Condition::and(
            Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
            Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
        );
        let split = case2.split_and();
        assert_eq!(split.len(), 2);
        assert_eq!(
            split,
            vec![
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3")))
            ]
        );
        let case3 = Condition::and(
            Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
            Condition::and(
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
                Condition::new("id3", ConditionOperation::Equals(Operand::from("id4"))),
            ),
        );
        let split = case3.split_and();
        assert_eq!(split.len(), 3);
        assert_eq!(
            split,
            vec![
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
                Condition::new("id3", ConditionOperation::Equals(Operand::from("id4")))
            ]
        );
        /// First split
        let case4 = Condition::and(
            Condition::and(
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
            ),
            Condition::new("id3", ConditionOperation::Equals(Operand::from("id4"))),
        );
        let split = case4.split_and();
        assert_eq!(split.len(), 3);
        assert_eq!(
            split,
            vec![
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
                Condition::new("id3", ConditionOperation::Equals(Operand::from("id4")))
            ]
        );
        /// multi split
        let case5 = Condition::and(
            Condition::and(
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
            ),
            Condition::and(
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
            ),
        );
        let split = case5.split_and();
        assert_eq!(split.len(), 4);
        assert_eq!(
            split,
            vec![
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3")))
            ]
        );
        /// multi weird
        let case5 = Condition::and(
            Condition::and(
                Condition::and(
                    Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                    Condition::and(
                        Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                        Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
                    ),
                ),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
            ),
            Condition::and(
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
            ),
        );
        let split = case5.split_and();
        assert_eq!(
            split,
            vec![
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3"))),
                Condition::new("id1", ConditionOperation::Equals(Operand::from("id2"))),
                Condition::new("id2", ConditionOperation::Equals(Operand::from("id3")))
            ]
        );
    }
}

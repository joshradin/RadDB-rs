//! This crate contains all of the types that can be used in the RadDB program. It establishes
//! all relevant traits as well.

use chrono::{Date, DateTime, Local, Utc};
use std::cmp::min;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

pub mod deserialization;
pub mod serialization;

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq)]
pub enum Numeric {
    Float(f32),
    Double(f64),
    Signed(Signed),
    Unsigned(Unsigned),
}

impl Eq for Numeric {}

impl Hash for Numeric {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Numeric::Float(_) | Numeric::Double(_) => {
                panic!("Can't hash on floating point numbers")
            }
            Numeric::Signed(s) => s.hash(state),
            Numeric::Unsigned(o) => o.hash(state),
        }
    }
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum Signed {
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum Unsigned {
    Byte(u8),
    Short(u16),
    Int(u32),
    Long(u64),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Text {
    Char(char),
    String(String, Option<u16>),
    Binary(u8),
    BinaryString(Vec<u8>, u16),
    Blob(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Time {
    Date(Date<Local>),
    DateTime(DateTime<Local>),
    Timestamp(DateTime<Utc>),
    Year(i32),
}

/// Base type for all data types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Type {
    Numeric(Numeric),
    Text(Text),
    Time(Time),
    Boolean(bool),
    Optional(Option<Box<Type>>),
}

impl From<Numeric> for Type {
    fn from(n: Numeric) -> Self {
        Type::Numeric(n)
    }
}

impl From<Signed> for Type {
    fn from(n: Signed) -> Self {
        Numeric::Signed(n).into()
    }
}

impl From<Unsigned> for Type {
    fn from(n: Unsigned) -> Self {
        Numeric::Unsigned(n).into()
    }
}

impl From<Text> for Type {
    fn from(t: Text) -> Self {
        Type::Text(t)
    }
}

impl From<Time> for Type {
    fn from(t: Time) -> Self {
        Type::Time(t)
    }
}

impl From<i8> for Type {
    fn from(i: i8) -> Self {
        Signed::Byte(i).into()
    }
}

impl From<i16> for Type {
    fn from(i: i16) -> Self {
        Signed::Short(i).into()
    }
}
impl From<i32> for Type {
    fn from(i: i32) -> Self {
        Signed::Int(i).into()
    }
}
impl From<i64> for Type {
    fn from(i: i64) -> Self {
        Signed::Long(i).into()
    }
}

impl From<u8> for Type {
    fn from(i: u8) -> Self {
        Unsigned::Byte(i).into()
    }
}

impl From<u16> for Type {
    fn from(i: u16) -> Self {
        Unsigned::Short(i).into()
    }
}
impl From<u32> for Type {
    fn from(i: u32) -> Self {
        Unsigned::Int(i).into()
    }
}
impl From<u64> for Type {
    fn from(i: u64) -> Self {
        Unsigned::Long(i).into()
    }
}

impl From<bool> for Type {
    fn from(n: bool) -> Self {
        Type::Boolean(n)
    }
}

impl From<&str> for Type {
    fn from(s: &str) -> Self {
        let string = s.to_string();
        Type::Text(Text::String(string, None))
    }
}

impl From<String> for Type {
    fn from(s: String) -> Self {
        Type::Text(Text::String(s, None))
    }
}

impl Display for Unsigned {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Unsigned::Byte(b) => b,
            Unsigned::Short(s) => s,
            Unsigned::Int(i) => i,
            Unsigned::Long(d) => d,
        };
        write!(f, "{}", disp)
    }
}

impl Display for Signed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Signed::Byte(b) => b,
            Signed::Short(s) => s,
            Signed::Int(i) => i,
            Signed::Long(d) => d,
        };
        write!(f, "{}", disp)
    }
}

impl Display for Numeric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Numeric::Float(f) => f,
            Numeric::Double(d) => d,
            Numeric::Signed(s) => s,
            Numeric::Unsigned(u) => u,
        };
        write!(f, "{}", disp)
    }
}

impl Display for Text {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Text::Char(c) => c,
            Text::String(s, _) => s,
            Text::Binary(b) => b,
            Text::BinaryString(b, _) => unsafe {
                return write!(f, "{}", String::from_utf8_unchecked(b.clone()));
            },
            Text::Blob(blob) => unsafe {
                return write!(f, "{}", String::from_utf8_unchecked(blob.clone()));
            },
        };
        write!(f, "\"{}\"", disp)
    }
}

impl Display for Time {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Time::Date(d) => d,
            Time::DateTime(datetime) => datetime,
            Time::Timestamp(t) => t,
            Time::Year(yr) => yr,
        };
        write!(f, "{}", disp)
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Type::Numeric(n) => n,
            Type::Text(t) => t,
            Type::Time(t) => t,
            Type::Boolean(b) => b,
            Type::Optional(Some(inner)) => inner,
            Type::Optional(None) => &"NULL",
        };
        write!(f, "{}", disp)
    }
}

pub trait SameType {
    fn same_type(&self, other: &Self) -> bool;
}

impl SameType for Signed {
    fn same_type(&self, other: &Self) -> bool {
        match (self, other) {
            (Signed::Byte(_), Signed::Byte(_)) => true,
            (Signed::Short(_), Signed::Short(_)) => true,
            (Signed::Int(_), Signed::Int(_)) => true,
            (Signed::Long(_), Signed::Long(_)) => true,
            _ => false,
        }
    }
}

impl SameType for Unsigned {
    fn same_type(&self, other: &Self) -> bool {
        match (self, other) {
            (Unsigned::Byte(_), Unsigned::Byte(_)) => true,
            (Unsigned::Short(_), Unsigned::Short(_)) => true,
            (Unsigned::Int(_), Unsigned::Int(_)) => true,
            (Unsigned::Long(_), Unsigned::Long(_)) => true,
            _ => false,
        }
    }
}

impl SameType for Numeric {
    fn same_type(&self, other: &Self) -> bool {
        match (self, other) {
            (Numeric::Signed(self_n), Numeric::Signed(other_n)) => self_n.same_type(other_n),
            (Numeric::Unsigned(self_n), Numeric::Unsigned(other_n)) => self_n.same_type(other_n),
            (Numeric::Double(_), Numeric::Double(_)) => true,
            (Numeric::Float(_), Numeric::Float(_)) => true,
            _ => false,
        }
    }
}

impl SameType for Text {
    fn same_type(&self, other: &Self) -> bool {
        match (self, other) {
            (Text::Char(_), Text::Char(_)) => true,
            (Text::String(_, len1), Text::String(_, len2)) => len1 == len2,
            (Text::Binary(_), Text::Binary(_)) => true,
            (Text::BinaryString(_, len1), Text::BinaryString(_, len2)) => len1 == len2,
            (Text::Blob(_), Text::Blob(_)) => true,
            _ => false,
        }
    }
}

impl SameType for Time {
    fn same_type(&self, other: &Self) -> bool {
        match (self, other) {
            (Time::Date(_), Time::Date(_)) => true,
            (Time::DateTime(_), Time::DateTime(_)) => true,
            (Time::Timestamp(_), Time::Timestamp(_)) => true,
            (Time::Year(_), Time::Year(_)) => true,
            _ => false,
        }
    }
}

impl SameType for Type {
    fn same_type(&self, other: &Self) -> bool {
        match (self, other) {
            (Type::Numeric(self_n), Type::Numeric(other_n)) => self_n.same_type(other_n),
            (Type::Text(self_n), Type::Text(other_n)) => self_n.same_type(other_n),
            (Type::Time(self_n), Type::Time(other_n)) => self_n.same_type(other_n),
            (Type::Boolean(_), Type::Boolean(_)) => true,
            _ => false,
        }
    }
}

impl SameType for Vec<Type> {
    fn same_type(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            false
        } else {
            let mut self_iter = self.into_iter();
            let mut other_iter = other.into_iter();
            while let (Some(mine), Some(theirs)) = (self_iter.next(), other_iter.next()) {
                if !mine.same_type(theirs) {
                    return false;
                }
            }
            true
        }
    }
}

impl SameType for Vec<&Type> {
    fn same_type(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            false
        } else {
            let mut self_iter = self.into_iter();
            let mut other_iter = other.into_iter();
            while let (Some(mine), Some(theirs)) = (self_iter.next(), other_iter.next()) {
                if !mine.same_type(theirs) {
                    return false;
                }
            }
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::serialize_values;
    use chrono::TimeZone;

    #[test]
    fn conversion() {
        let types: Vec<Type> = vec![3.into(), 9.into(), "hello".into()];
        let text = types
            .into_iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>()
            .join(",");
        assert_eq!("3,9,\"hello\"", text);
    }

    #[test]
    fn date() {
        let date = Time::Date(Local.ymd(1999, 3, 7));
        println!("{}", date);
    }

    #[test]
    fn serialize_deserialize() {
        let types: Vec<Type> = vec![
            Signed::Byte(3).into(),
            Unsigned::Long(23241212332).into(),
            Text::String("Hello World!".to_string(), None).into(),
        ];
        let to_check = types.clone();
        let serialized = serialize_values(types);
        let types: Vec<Type> = vec![
            Signed::Byte(0).into(),
            Unsigned::Long(0).into(),
            Text::String(String::new(), None).into(),
        ];
        let deserialized = deserialization::parse_using_types(serialized, types).unwrap();
        assert_eq!(deserialized, to_check);
    }
}

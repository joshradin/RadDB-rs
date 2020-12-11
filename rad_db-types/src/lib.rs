//! This crate contains all of the types that can be used in the RadDB program. It establishes
//! all relevant traits as well.

use chrono::{Date, Utc, Local, DateTime};
use std::fmt::{Display, Formatter};

pub mod serialization;
pub mod deserialization;



#[derive(Debug, Clone, Copy, PartialOrd, PartialEq)]
pub enum Numeric {
    Float(f32),
    Double(f64),
    Signed(Signed),
    Unsigned(Unsigned)
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum Signed {
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64)
}

#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq)]
pub enum Unsigned {
    Byte(u8),
    Short(u16),
    Int(u32),
    Long(u64)
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Text {
    Char(char),
    String(String, Option<u16>),
    Binary(u8),
    BinaryString(Vec<u8>, u16),
    Blob(Vec<u8>)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Time {
    Date(Date<Local>),
    DateTime(DateTime<Local>),
    Timestamp(DateTime<Utc>),
    Year(i32)
}


/// Base type for all data types
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Numeric(Numeric),
    Text(Text),
    Time(Time),
    Boolean(bool)
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
            Unsigned::Byte(b) => { b }
            Unsigned::Short(s) => { s }
            Unsigned::Int(i) => { i }
            Unsigned::Long(d) => { d }
        };
        write!(f, "{}", disp)
    }
}

impl Display for Signed {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Signed::Byte(b) => { b }
            Signed::Short(s) => { s }
            Signed::Int(i) => { i }
            Signed::Long(d) => { d }
        };
        write!(f, "{}", disp)
    }
}

impl Display for Numeric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Numeric::Float(f) => {f}
            Numeric::Double(d) => {d}
            Numeric::Signed(s) => {s}
            Numeric::Unsigned(u) => {u}
        };
        write!(f, "{}", disp)
    }
}

impl Display for Text {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Text::Char(c) => {c}
            Text::String(s, _) => {s}
            Text::Binary(b) => {b}
            Text::BinaryString(b, _) => {
                unsafe {
                    return write!(f, "{}", String::from_utf8_unchecked(b.clone()))
                }
            }
            Text::Blob(blob) => {
                unsafe {
                    return write!(f, "{}", String::from_utf8_unchecked(blob.clone()))
                }
            }
        };
        write!(f, "\"{}\"", disp)
    }
}

impl Display for Time {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Time::Date(d) => {d}
            Time::DateTime(datetime) => {datetime}
            Time::Timestamp(t) => {t}
            Time::Year(yr) => {yr}
        };
        write!(f, "{}", disp)
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let disp: &dyn Display = match self {
            Type::Numeric(n) => {n}
            Type::Text(t) => {t}
            Type::Time(t) => {t}
            Type::Boolean(b) => {b}
        };
        write!(f, "{}", disp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use crate::serialization::serialize_values;

    #[test]
    fn conversion() {
        let types: Vec<Type> = vec![3.into(), 9.into(), "hello".into()];
        let text = types.into_iter().map(|s| s.to_string()).collect::<Vec<String>>().join(",");
        assert_eq!("3,9,\"hello\"", text);
    }

    #[test]
    fn date() {
        let date = Time::Date(Local.ymd(1999,3,7));
        println!("{}", date);
    }

    #[test]
    fn serialize_deserialize() {
        let types: Vec<Type> = vec![Signed::Byte(3).into(), Unsigned::Long(23241212332).into(), Text::String("Hello World!".to_string(), None).into()];
        let to_check = types.clone();
        let serialized = serialize_values(types);
        let types: Vec<Type> = vec![Signed::Byte(0).into(), Unsigned::Long(0).into(), Text::String(String::new(), None).into()];
        let deserialized = deserialization::parse_using_types(serialized, types).unwrap();
        assert_eq!(deserialized, to_check);
    }
}

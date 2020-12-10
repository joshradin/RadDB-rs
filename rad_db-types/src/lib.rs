//! This crate contains all of the types that can be used in the RadDB program. It establishes
//! all relevant traits as well.

use chrono::{Date, Utc, Local, DateTime};
pub mod serialization;

#[macro_use]
extern crate rad_db_derive;

#[type_tree]
#[derive(Debug, Clone, Copy)]
pub enum Numeric {
    Float(f32),
    Double(f64),
    Signed(Signed),
    Unsigned(Unsigned)
}

#[derive(Debug, Clone, Copy)]
pub enum Signed {
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64)
}

#[derive(Debug, Clone, Copy)]
pub enum Unsigned {
    Byte(u8),
    Short(u16),
    Int(u32),
    Long(u64)
}

#[derive(Debug, Clone)]
pub enum Text {
    Char(char),
    String(String, u16),
    Binary(u8),
    BinaryString(Vec<u8>, u16),
    Blob(Vec<u8>)
}

#[derive(Debug, Clone, Copy)]
pub enum Time {
    /*
    Date(Date<Local>),
    DateTime(DateTime<Local>),
    Timestamp(DateTime<Utc>),

     */
    Year(i32)
}


/// Base type for all data types
#[derive(Debug, Clone)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversion() {
        let ty: Type = 0i32.into();
    }
}

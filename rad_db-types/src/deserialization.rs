use std::error::Error;
use std::fmt::{Display, Formatter};

use chrono::{Local, TimeZone};

use crate::{Numeric, Signed, Text, Time, Type, Unsigned};

#[derive(Debug)]
pub struct ParseTupleFailure;

impl Display for ParseTupleFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failure to parse a tuple")
    }
}

impl <E : Error> From<E> for ParseTupleFailure {
    fn from(_: E) -> Self {
        ParseTupleFailure
    }
}

pub type Result<T> = std::result::Result<T, ParseTupleFailure>;

pub fn parse_using_types<S : AsRef<str>, I : IntoIterator<Item=Type>>(to_parse: S, iterator: I) -> Result<Vec<Type>> {
    parse_using_types_helper(to_parse.as_ref(), iterator.into_iter().collect())
}


fn parse_using_types_helper(to_parse: &str, iterator: Vec<Type>) -> Result<Vec<Type>> {
    let mut current = String::new();
    let mut strings_vector = vec![];
    let mut in_quote = false;
    let mut chars_iterator = to_parse.chars();

    while let Some(c) = chars_iterator.next() {
        if c == '"' {
            in_quote = !in_quote;
        } else if c == '\\' {
            let next = chars_iterator.next().ok_or_else(|| ParseTupleFailure)?;
            current += &next.to_string();

        } else if c == '|' && !in_quote {
            let string = std::mem::replace(&mut current, String::new());
            strings_vector.push(string);
        } else {
            current += &c.to_string();
        }

    }

    if !current.trim().is_empty() {
        strings_vector.push(current);
    }


    let mut output = vec![];
    let mut string_iter = strings_vector.into_iter();
    let mut type_iter = iterator.into_iter();
    while let (Some(base_type), Some(string)) = (type_iter.next(),string_iter.next()) {
        let mut created = base_type.clone();
        match &mut created {
            Type::Numeric(n) => {
                match n {
                    Numeric::Float(f) => {
                        *f = string.parse()?;
                    }
                    Numeric::Double(d) => {
                        *d = string.parse()?;
                    }
                    Numeric::Signed(signed) => {
                        match signed {
                            Signed::Byte(b) => {
                                *b = string.parse()?;
                            }
                            Signed::Short(s) => {
                                *s = string.parse()?;
                            }
                            Signed::Int(i) => {
                                *i = string.parse()?;
                            }
                            Signed::Long(l) => {
                                *l = string.parse()?;
                            }
                        }
                    }
                    Numeric::Unsigned(unsigned) => {
                        match unsigned {
                            Unsigned::Byte(b) => {
                                *b = string.parse()?;
                            }
                            Unsigned::Short(s) => {
                                *s = string.parse()?;
                            }
                            Unsigned::Int(i) => {
                                *i = string.parse()?;
                            }
                            Unsigned::Long(l) => {
                                *l = string.parse()?;
                            }
                        }
                    }
                }
            }
            Type::Text(t) => {
                match t {
                    Text::Char(c) => {
                        *c = string.parse()?;
                    }
                    Text::String(s, len) => {
                        if let Some(max_len) = len {
                            if (*max_len as usize) < string.len() {
                                return Err(ParseTupleFailure)
                            }
                        }
                        *s = string;
                    }
                    Text::Binary(b) => {
                        *b = string.as_bytes()[0];
                    }
                    Text::BinaryString(bs, len) => {

                        if *len as usize > string.len() {
                            return Err(ParseTupleFailure)
                        }

                        *bs = string.as_bytes().to_vec();
                    }
                    Text::Blob(blob) => {
                        *blob = string.as_bytes().to_vec();
                    }
                }
            }
            Type::Time(t) => {
                match t {
                    Time::Date(d) => {
                        let split = string.split('-').collect::<Vec<_>>();
                        let year: i32 = split[0].parse()?;
                        let month: u32 = split[0].parse()?;
                        let day: u32 = split[0].parse()?;
                        *d = Local.ymd(year, month, day)
                    }
                    Time::DateTime(d) => {
                        *d = string.parse()?;
                    }
                    Time::Timestamp(t) => {
                        *t = string.parse()?;
                    }
                    Time::Year(y) => {
                        *y = string.parse()?;
                    }
                }
            }
            Type::Boolean(b) => {
                *b = string.parse()?;
            }
        }
        output.push(created);
    }

    if !(string_iter.next().is_none() && type_iter.next().is_none()) {
        Err(ParseTupleFailure)
    } else {
        Ok(output)
    }
}


#[cfg(test)]
mod tests {
    use crate::{Signed, Unsigned};

    use super::*;

    #[test]
    fn deserialize() {
        let types: Vec<Type> = vec![Signed::Byte(0).into(), Unsigned::Long(0).into(), Text::String(String::new(), None).into()];
        let input = vec!["3", "23241212332", "\"Hello World!\""].join("|");
        let output = parse_using_types(input, types).unwrap();
        assert_eq!(output[0], Signed::Byte(3).into());
        assert_eq!(output[1], Unsigned::Long(23241212332).into());
        assert_eq!(output[2], Text::String("Hello World!".to_string(), None).into());
    }

    #[test]
    fn string_too_big() {
        let types: Vec<Type> = vec![Text::String(String::new(), Some(1)).into()];
        let input = vec!["\"Hello World!\""].join("|");
        parse_using_types(input, types).unwrap_err();
    }
}
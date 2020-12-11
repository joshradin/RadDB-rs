use std::str::FromStr;
use crate::{Type, Time, Text};
use std::fmt::{Display, Formatter};
use std::error::Error;
use regex::Regex;


#[derive(Debug)]
pub struct ParseTupleFailure;

impl Display for ParseTupleFailure {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failure to parse a tuple")
    }
}

impl <E : Error> From<E> for ParseTupleFailure {
    fn from(e: E) -> Self {
        ParseTupleFailure
    }
}

pub type Result<T> = std::result::Result<T, ParseTupleFailure>;

pub fn parse_using_types<S : AsRef<str>, I : Iterator<Item=Type>>(to_parse: S, iterator: I) -> Result<Vec<Type>> {
    parse_using_types_helper(to_parse.as_ref(), iterator.collect())
}


fn parse_using_types_helper(to_parse: &str, mut iterator: Vec<Type>) -> Result<Vec<Type>> {
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
            Type::Numeric(n) => {}
            Type::Text(t) => {
                match t {
                    Text::Char(c) => {
                        *c = string.parse()?;
                    }
                    Text::String(s, _) => {
                        *s = string;
                    }
                    Text::Binary(b) => {
                        *b = string.as_bytes()[0];
                    }
                    Text::BinaryString(bs, _) => {
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
                        *d = string.parse()?;
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

    Ok(output)
}
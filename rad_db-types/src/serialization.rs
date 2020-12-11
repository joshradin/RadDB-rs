use crate::{Type, Text};

pub fn serialize_values<I : IntoIterator<Item=Type>>(values: I) -> String {
    let vec =
        values
            .into_iter()
            .map(|v: Type| {
                match v {
                    Type::Text(text) => {
                        match text {
                            Text::Char(c) => { format!("\"{}\"", c) }
                            Text::String(s, _) => { format!("\"{}\"", s) }
                            Text::Binary(_) => { unimplemented!( )}
                            Text::BinaryString(_, _) => { unimplemented!( ) }
                            Text::Blob(_) => { unimplemented!( ) }
                        }
                    },
                    rest => rest.to_string()
                }
            })
            .collect::<Vec<String>>();
    vec.join("|")

}
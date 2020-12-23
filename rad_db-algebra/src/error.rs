use rad_db_structure::identifier::Identifier;
use std::fmt::{Display, Formatter};
macro_rules! quick_error {
    ($error:ty) => {
        impl std::fmt::Display for $error {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self)
            }
        }

        impl std::error::Error for $error { }

    };
    ($error:ident; $($field:ident : $type:ty),*) => {
        #[derive(Debug)]
        pub struct $error { $($field: $type),* }

        impl $error {
            pub fn new($($field: $type),*) -> Self {
                Self { $($field),* }
            }
        }

        quick_error!{$error}
    };
}

/*
#[derive(Debug)]
pub struct MissingFieldError { field: Identifier }

 */

quick_error!{ MissingFieldError; field: Identifier }
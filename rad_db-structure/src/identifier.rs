use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use std::iter::FromIterator;

#[derive(Debug, Clone, PartialEq)]
pub struct Identifier {
    parent: Option<Box<Identifier>>,
    base: String,
}

impl Identifier {
    /// Creates a new identifier without a namespace
    pub fn new<S: AsRef<str>>(string: S) -> Self {
        Identifier {
            parent: None,
            base: string.as_ref().to_string(),
        }
    }

    /// Creates an identifier within a parent namespace
    pub fn with_parent<S: AsRef<str>>(parent: &Identifier, string: S) -> Self {
        Identifier {
            parent: Some(Box::new(parent.clone())),
            base: string.as_ref().to_string(),
        }
    }

    pub fn concat<I1: Into<Identifier>, I2: Into<Identifier>>(parent: I1, child: I2) -> Self {
        let mut output = child.into();
        let output_first = output.first_mut();
        let parent = parent.into();
        output_first.parent = Some(Box::new(parent));
        output
    }

    pub fn base(&self) -> &String {
        &self.base
    }

    pub fn parent(&self) -> Option<&Identifier> {
        if let Some(parent) = &self.parent {
            Some(&*parent)
        } else {
            None
        }
    }

    /// Gets the first identifier in a full identifier
    pub fn first(&self) -> &Identifier {
        match &self.parent {
            None => self,
            Some(s) => s.first(),
        }
    }

    fn first_mut(&mut self) -> &mut Identifier {
        if self.parent.is_some() {
            let parent = self.parent.as_mut().unwrap();
            parent.first_mut()
        } else {
            self
        }
    }

    /// Strips the highest namespace from an identifier
    ///
    /// # Example
    ///
    /// `id1::id2::id3` -> `Some(id2::id3)`
    ///
    /// `id3` -> `None`
    pub fn strip_highest_parent(&self) -> Option<Identifier> {
        let parent = match &self.parent {
            None => return None,
            Some(parent) => parent.strip_highest_parent().map(|id| Box::new(id)),
        };

        Some(Self {
            parent,
            base: self.base.clone(),
        })
    }

    /// Returns the length of the identifier
    pub fn len(&self) -> usize {
        1 + match &self.parent {
            None => 0,
            Some(parent) => parent.len(),
        }
    }
}

impl<'a> IntoIterator for &'a Identifier {
    type Item = &'a str;
    type IntoIter = <VecDeque<&'a str> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let mut ret = VecDeque::new();
        let mut ptr = self;
        loop {
            let string = &*ptr.base;
            ret.push_front(string);
            if let Some(parent) = &ptr.parent {
                ptr = &**parent;
            } else {
                break;
            }
        }
        ret.into_iter()
    }
}

impl IntoIterator for Identifier {
    type Item = String;
    type IntoIter = <VecDeque<String> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        let mut ret = VecDeque::new();
        let mut ptr = self;
        loop {
            let Identifier { parent, base } = ptr;
            ret.push_front(base);
            if let Some(parent) = parent {
                ptr = *parent;
            } else {
                break;
            }
        }
        ret.into_iter()
    }
}

impl<A: AsRef<str>> FromIterator<A> for Identifier {
    fn from_iter<T: IntoIterator<Item = A>>(iter: T) -> Self {
        let mut ret = None;
        for id in iter {
            match ret {
                None => ret = Some(Identifier::new(id)),
                Some(parent) => ret = Some(Identifier::with_parent(&parent, id)),
            }
        }

        ret.expect("Can not create an empty identifier")
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.parent {
            None => {
                write!(f, "{}", self.base())
            }
            Some(parent) => {
                write!(f, "{}::{}", parent, self.base())
            }
        }
    }
}

impl<S: AsRef<str>> From<S> for Identifier {
    fn from(str: S) -> Self {
        Identifier::new(str)
    }
}

impl From<&Identifier> for Identifier {
    fn from(i: &Identifier) -> Self {
        i.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::identifier::Identifier;
    use std::iter::FromIterator;

    #[test]
    fn id_length() {
        assert_eq!(Identifier::new("hello").len(), 1);
        assert_eq!(Identifier::from_iter(vec!["db", "table", "field"]).len(), 3);
    }

    #[test]
    fn strip_prefix() {
        let full_id = Identifier::from_iter(vec!["db", "table", "field"]);
        let strip1 = full_id.strip_highest_parent().unwrap();
        assert_eq!(strip1, Identifier::from_iter(vec!["table", "field"]));
        let strip2 = strip1.strip_highest_parent().unwrap();
        assert_eq!(strip2, Identifier::from_iter(vec!["field"]));
        let strip3 = strip2.strip_highest_parent();
        assert_eq!(strip3, None);
    }

    #[test]
    #[should_panic]
    fn no_zero_length_id() {
        Identifier::from_iter(Vec::<String>::new());
    }

    #[test]
    fn id_display() {
        let single = format!("{}", Identifier::new("table"));
        assert_eq!(single, "table");
        let multiple = format!("{}", Identifier::from_iter(&["db", "table", "field"]));
        assert_eq!(multiple, "db::table::field");
    }

    #[test]
    fn id_concatenation() {
        let concat = Identifier::concat("db", Identifier::new("table"));
        assert_eq!(concat, Identifier::from_iter(&["db", "table"]));
        let concat = Identifier::concat(
            Identifier::new("db"),
            Identifier::from_iter(&["table", "field"]),
        );
        assert_eq!(concat, Identifier::from_iter(&["db", "table", "field"]));
    }
}

#[derive(Debug, Clone)]
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
}

use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::Relation;
use rad_db_structure::tuple::Tuple;
use rad_db_types::Type;

pub enum Projection {
    Flat(Identifier),
    Renamed(Identifier, String),
}

pub enum Source<'a> {
    Flat(&'a Relation),
    Renamed(&'a Relation, String),
}

pub enum Query<'a> {
    Source(Source<'a>),
    Projection(Vec<Projection>),
    Selection(Box<dyn Fn(&Tuple) -> bool>),
    CrossProduct,
    InnerJoin(Identifier, Identifier),
    LeftJoin(Identifier, Identifier),
    RightJoin(Identifier, Identifier),
    NaturalJoin,
}

pub enum QueryChildren<'a> {
    None,
    One(QueryNode<'a>),
    Two(QueryNode<'a>, QueryNode<'a>),
}

pub struct QueryNode<'a> {
    query: Query<'a>,
    children: Box<QueryChildren<'a>>,
    resulting_relation: Vec<(Identifier, Type)>,
    buffer: Option<dyn Iterator<Item = &'a Tuple>>,
}

impl<'a> QueryNode<'a> {
    pub fn source(relation: &'a Relation) -> Self {
        Self {
            query: Query::Source(Source::Flat(relation)),
            children: Box::new(QueryChildren::None),
            resulting_relation: relation
                .attributes()
                .iter()
                .map(|(id, val)| (Identifier::new(id.clone()), val.clone()))
                .collect(),
            buffer: None,
        }
    }

    pub fn source_with_name(relation: &'a Relation, name: String) -> Self {
        Self {
            query: Query::Source(Source::Renamed(relation, name)),
            children: Box::new(QueryChildren::None),
            resulting_relation: relation
                .attributes()
                .iter()
                .map(|(id, val)| (Identifier::new(id.clone()), val.clone()))
                .collect(),
            buffer: None,
        }
    }
}

impl<'a> Iterator for QueryNode<'a> {
    type Item = &'a Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_none() {
            let iterator = Box::new(match &self.query {});
            self.buffer = Some(iterator)
        }

        self.buffer.as_mut().unwrap().next()
    }
}

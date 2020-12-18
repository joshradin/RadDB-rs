use crate::query::query_iterator::QueryBuffer;
use rad_db_structure::identifier::Identifier;
use rad_db_structure::relations::tuple_storage::StoredTupleIterator;
use rad_db_structure::relations::Relation;
use rad_db_structure::tuple::Tuple;
use rad_db_types::Type;

pub enum Projection {
    Flat(Identifier),
    Renamed(Identifier, String),
}

pub struct Crawler<'a> {
    source: &'a Relation,
    iterator: Option<StoredTupleIterator<'a>>,
}

impl<'a> Crawler<'a> {
    pub fn new(source: &'a Relation) -> Self {
        Crawler {
            source,
            iterator: None,
        }
    }
}

impl<'a> Iterator for Crawler<'a> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iterator.is_none() {
            self.iterator = Some(self.source.tuples())
        }

        self.iterator.as_mut().unwrap().next()
    }
}

pub enum Source<'a> {
    Flat(Crawler<'a>),
    Renamed(Crawler<'a>, String),
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
    buffer: QueryBuffer,
}

impl<'a> QueryNode<'a> {
    pub fn source(relation: &'a Relation) -> Self {
        Self {
            query: Query::Source(Source::Flat(Crawler::new(relation))),
            children: Box::new(QueryChildren::None),
            resulting_relation: relation
                .attributes()
                .iter()
                .map(|(id, val)| (Identifier::new(id.clone()), val.clone()))
                .collect(),
            buffer: QueryBuffer::new(None),
        }
    }

    pub fn source_with_name(relation: &'a Relation, name: String) -> Self {
        Self {
            query: Query::Source(Source::Renamed(Crawler::new(relation), name)),
            children: Box::new(QueryChildren::None),
            resulting_relation: relation
                .attributes()
                .iter()
                .map(|(id, val)| (Identifier::new(id.clone()), val.clone()))
                .collect(),
            buffer: QueryBuffer::new(None),
        }
    }

    fn get_tuples_from_source(&self, count: Option<usize>) -> Vec<Tuple> {
        if let Query::Source(source) = &self.query {
            let mut resulting = self.resulting_relation.clone();
        } else {
            panic!("Can not be called from a non-source context");
        }
        unimplemented!()
    }
}

impl<'a> Iterator for QueryNode<'a> {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(out) = (&mut self.buffer).next() {
            return Some(out);
        }

        unimplemented!()
    }
}

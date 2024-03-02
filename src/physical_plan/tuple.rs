use crate::data_types::Data;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub struct Tuple(pub Vec<Data>);

impl Display for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for data in self.0.iter() {
            data.fmt(f)?;
            write!(f, " ")?;
        }

        Ok(())
    }
}

impl Tuple {
    pub fn new<I>(data: I) -> Self
    where
        I: IntoIterator<Item = Data>,
    {
        Self(data.into_iter().collect())
    }
}

pub type TupleStream = Box<dyn Iterator<Item = Tuple>>;

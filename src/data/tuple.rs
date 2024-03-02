use super::types::Data;
use crate::catalog::schema::Schema;
use bytes::{BufMut, Bytes};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone)]
pub struct Tuple(Vec<Data>);

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

    pub fn get(&self, idx: usize) -> Option<&Data> {
        self.0.get(idx)
    }

    pub fn encode(&self) -> Bytes {
        let mut ret = Vec::new();
        for column in self.0.iter() {
            ret.put_slice(column.encode().as_ref());
        }
        ret.into()
    }

    pub fn decode(buf: &mut Bytes, schema: &Schema) -> Self {
        let mut tuple = Vec::with_capacity(schema.n_columns());
        for (_, datatype) in schema.columns() {
            let data = Data::decode(buf, datatype);
            tuple.push(data);
        }

        Tuple(tuple)
    }
}

pub type TupleStream = Box<dyn Iterator<Item = Tuple>>;

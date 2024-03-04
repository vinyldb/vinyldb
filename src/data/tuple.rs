use super::types::{Data, DataType};
use crate::catalog::schema::Schema;
use bytes::BufMut;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
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

    /// Encode this `Tuple` into a sequence of bytes.
    ///
    /// # NOTE
    /// Copy is not avoidable.
    pub fn encode(&self) -> Box<[u8]> {
        let mut ret = Vec::new();
        for column in self.0.iter() {
            ret.put_slice(column.encode().as_ref());
        }
        ret.into_boxed_slice()
    }

    /// Decode a `Tuple` from a sequence of bytes.
    pub fn decode<B: AsRef<[u8]>>(buf: B, schema: &Schema) -> Self {
        let buf = buf.as_ref();
        let mut start = 0_usize;

        let mut tuple = Vec::with_capacity(schema.n_columns());
        for (_, datatype) in schema.columns() {
            let data = Data::decode(&buf[start..], datatype);
            start += data.encode_size();
            tuple.push(data);
        }

        Tuple(tuple)
    }

    /// Return the [`DataType`]s of this `Tuple`.
    pub fn datatypes(&self) -> Vec<DataType> {
        self.0.iter().map(|data| data.datatype()).collect()
    }

    /// Iterate the data in this tuple.
    pub fn iter(&self) -> impl Iterator<Item = &Data> {
        self.0.iter()
    }
}

/// An iterator over [`Tuple`]s.
pub type TupleStream = Box<dyn Iterator<Item = Tuple>>;

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn encode_decode() {
        let schema = Schema::new([
            ("1".into(), DataType::Bool),
            ("2".into(), DataType::String),
            ("3".into(), DataType::Int64),
        ])
        .unwrap();
        let tuple = Tuple::new(vec![
            Data::Bool(true),
            Data::String("foo".into()),
            Data::Int64(1),
        ]);
        let bytes = tuple.encode();

        let decoded = Tuple::decode(bytes, &schema);
        assert_eq!(tuple, decoded);
    }
}

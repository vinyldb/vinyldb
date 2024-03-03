//! Data types

use bytes::{Buf, BufMut, Bytes};
use derive_more::Display;
use sqlparser::ast::DataType as SqlParserDataType;
use std::fmt::Formatter;

/// Data types.
#[derive(Debug, PartialEq, Copy, Clone, Eq)]
pub enum DataType {
    Bool,
    Int64,
    Float64,
    // Timestamp since UNIX Epoch, in seconds.
    Timestamp,
    String,
}

impl TryFrom<SqlParserDataType> for DataType {
    type Error = ();
    fn try_from(value: SqlParserDataType) -> Result<Self, Self::Error> {
        match value {
            SqlParserDataType::Bool => Ok(Self::Bool),
            SqlParserDataType::Int64 => Ok(Self::Int64),
            SqlParserDataType::Float64 => Ok(Self::Float64),
            SqlParserDataType::Timestamp(_, _) => Ok(Self::Timestamp),
            SqlParserDataType::String(_) => Ok(Self::String),

            ty => unimplemented!("uncovered type {}", ty),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let debug = format!("{:?}", self);
        f.write_str(&debug.to_uppercase())?;
        Ok(())
    }
}

/// Data
#[derive(Debug, PartialEq, Clone, Display)]
pub enum Data {
    Bool(bool),
    Int64(i64),
    Float64(f64),
    /// Timestamp since UNIX Epoch, in seconds.
    ///
    /// Stored as an `i64`.
    Timestamp(i64),
    String(String),
}

impl Data {
    // Encode `self` into a sequence of bytes.
    pub fn encode(&self) -> Bytes {
        let mut ret: Vec<u8> = Vec::new();
        match self {
            Data::Bool(raw) => ret.put_u8((*raw).into()),
            Data::Int64(raw) => ret.put_i64_ne(*raw),
            Data::Float64(raw) => ret.put_f64_ne(*raw),
            Data::Timestamp(raw) => ret.put_i64_ne(*raw),
            Data::String(raw) => {
                // string is var-len, so wew store a length before the actual data
                ret.put_u64_ne(
                    raw.len()
                        .try_into()
                        .expect("should never fail on a 64-bit machine"),
                );
                ret.put_slice(raw.as_bytes());
            }
        }

        ret.into()
    }

    // Decode a [`Data`] from `buf` according to the datatype given in `ty`.
    pub fn decode(buf: &mut Bytes, ty: &DataType) -> Self {
        match ty {
            DataType::Bool => Self::Bool(buf.get_u8() == 1),
            DataType::Int64 => Self::Int64(buf.get_i64_ne()),
            DataType::Float64 => Self::Float64(buf.get_f64_ne()),
            DataType::Timestamp => Self::Timestamp(buf.get_i64_ne()),
            DataType::String => {
                let len: usize = buf
                    .get_u64_ne()
                    .try_into()
                    .expect("should never fail on a 64-bit machine");
                let mut dst = vec![0; len];
                buf.copy_to_slice(dst.as_mut_slice());
                let str =
                    String::from_utf8(dst).expect("should be UTFf-8 encoded");

                Self::String(str)
            }
        }
    }

    /// Return its [`DataType`].
    pub fn datatype(&self) -> DataType {
        match self {
            Data::Bool(_) => DataType::Bool,
            Data::Int64(_) => DataType::Int64,
            Data::Float64(_) => DataType::Float64,
            Data::String(_) => DataType::String,
            Data::Timestamp(_) => DataType::Timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use pretty_assertions::assert_eq;

    #[test]
    fn data_encode_works() {
        let raw = true;
        let bool = Data::Bool(raw);
        assert_eq!(bool.encode(), Bytes::from(vec![raw.into()]));

        let raw = 8_i64;
        let int64 = Data::Int64(raw);
        assert_eq!(int64.encode(), Bytes::from(raw.to_ne_bytes().to_vec()));

        let raw = 10_f64;
        let f64 = Data::Float64(raw);
        assert_eq!(f64.encode(), Bytes::from(raw.to_ne_bytes().to_vec()));

        let raw = 70_i64;
        let timestamp = Data::Timestamp(raw);
        assert_eq!(timestamp.encode(), Bytes::from(raw.to_ne_bytes().to_vec()));

        let raw = String::from("VinylDB");
        let string = Data::String(raw.clone());
        let expected = {
            let mut ret = Vec::new();
            ret.put_u64_ne(
                raw.len()
                    .try_into()
                    .expect("should never fail on a 64-bit machine"),
            );
            ret.put_slice(raw.as_bytes());

            ret
        };
        assert_eq!(string.encode(), Bytes::from(expected));
    }

    #[test]
    fn data_decode_works() {
        let types = vec![
            DataType::String,
            DataType::Int64,
            DataType::Bool,
            DataType::String,
            DataType::Bool,
        ];
        let data = vec![
            Data::String("Vinyl".into()),
            Data::Int64(0),
            Data::Bool(false),
            Data::String("DB".into()),
            Data::Bool(true),
        ];

        let mut bytes = {
            let mut ret = BytesMut::new();
            for d in data.clone() {
                ret.put(d.encode());
            }

            Bytes::from(ret)
        };

        for (ty, expected) in types.iter().zip(data) {
            let res = Data::decode(&mut bytes, ty);
            assert_eq!(res, expected);
        }
    }
}

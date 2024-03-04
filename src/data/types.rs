//! Data types

use bytes::{Buf, BufMut};
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

/// Encoded data, it will be `Borrowed` when allocation is not needed.
#[derive(Debug, Clone, PartialEq)]
pub enum DataEncoded<'data> {
    Borrowed(&'data [u8]),
    Owned(Box<[u8]>),
}

impl<'data> AsRef<[u8]> for DataEncoded<'data> {
    fn as_ref(&self) -> &[u8] {
        match self {
            DataEncoded::Borrowed(slice) => slice,
            DataEncoded::Owned(boxed_slice) => boxed_slice.as_ref(),
        }
    }
}

impl Data {
    /// How many bytes it will take after encoding.
    pub fn encode_size(&self) -> usize {
        match self {
            Data::Bool(_) => std::mem::size_of::<u8>(),
            Data::Int64(_) => std::mem::size_of::<i64>(),
            Data::Float64(_) => std::mem::size_of::<f64>(),
            Data::Timestamp(_) => std::mem::size_of::<i64>(),
            Data::String(raw) => std::mem::size_of::<u64>() + raw.len(),
        }
    }

    /// Encode `self` into a sequence of bytes.
    pub fn encode(&self) -> DataEncoded {
        match self {
            Data::Bool(raw) => {
                let bytes: &[u8; 1] = bytemuck::cast_ref(raw);
                DataEncoded::Borrowed(bytes.as_slice())
            }
            Data::Int64(raw) => {
                let bytes: &[u8; 8] = bytemuck::cast_ref(raw);
                DataEncoded::Borrowed(bytes.as_slice())
            }
            Data::Float64(raw) => {
                let bytes: &[u8; 8] = bytemuck::cast_ref(raw);
                DataEncoded::Borrowed(bytes.as_slice())
            }
            Data::Timestamp(raw) => {
                let bytes: &[u8; 8] = bytemuck::cast_ref(raw);
                DataEncoded::Borrowed(bytes.as_slice())
            }
            Data::String(raw) => {
                let mut buf = Vec::new();
                // string is var-len, so wew store a length before the actual data
                buf.put_u64_ne(
                    raw.len()
                        .try_into()
                        .expect("should never fail on a 64-bit machine"),
                );
                buf.put_slice(raw.as_bytes());
                DataEncoded::Owned(buf.into_boxed_slice())
            }
        }
    }

    /// Decode a [`Data`] from `buf` according to the datatype given in `ty`.
    ///
    /// # NOTE
    /// Copy seems to be unavoidable.
    pub fn decode<B: AsRef<[u8]>>(buf: B, ty: &DataType) -> Self {
        let mut buf = buf.as_ref();
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
    use pretty_assertions::assert_eq;

    #[test]
    fn data_encode_works() {
        let raw = true;
        let bool = Data::Bool(raw);
        assert_eq!(
            bool.encode(),
            DataEncoded::Borrowed(vec![raw.into()].as_slice())
        );

        let raw = 8_i64;
        let int64 = Data::Int64(raw);
        assert_eq!(
            int64.encode(),
            DataEncoded::Borrowed(raw.to_ne_bytes().as_slice())
        );

        let raw = 10_f64;
        let f64 = Data::Float64(raw);
        assert_eq!(
            f64.encode(),
            DataEncoded::Borrowed(raw.to_ne_bytes().as_slice())
        );

        let raw = 70_i64;
        let timestamp = Data::Timestamp(raw);
        assert_eq!(
            timestamp.encode(),
            DataEncoded::Borrowed(raw.to_ne_bytes().as_slice())
        );

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
        assert_eq!(
            string.encode(),
            DataEncoded::Owned(expected.into_boxed_slice())
        );
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

        let bytes = {
            let mut ret = Vec::new();
            for d in data.clone() {
                ret.put(d.encode().as_ref());
            }

            ret
        };

        let mut start = 0_usize;
        for (ty, expected) in types.iter().zip(data) {
            let res = Data::decode(&bytes[start..], ty);
            assert_eq!(res, expected);
            start += res.encode_size();
        }
    }
}

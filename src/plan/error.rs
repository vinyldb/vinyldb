use crate::data::types::DataType;
use derive_more::{Display, Error};
use sqlparser::ast::Value;

#[derive(Debug, Display, Error)]
pub enum PlanError {
    #[display(
        fmt = "table {table} has {expect} columns but {found} columns were supplied"
    )]
    MismatchedNumberColumns {
        table: String,
        expect: usize,
        found: usize,
    },
    #[display(
        fmt = "the data type of {column_idx}th column of table {table} should be {expect}"
    )]
    MismatchedType {
        table: String,
        column_idx: usize,
        expect: DataType,
        // TODO: add a found field
    },
    #[display(fmt = "cound not convert {val} to {to}")]
    ConversionError { val: Value, to: DataType },
}

pub type PlanResult<T> = Result<T, PlanError>;

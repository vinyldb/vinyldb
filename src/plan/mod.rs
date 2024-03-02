pub mod error;

use crate::{
    as_variant,
    catalog::{catalog::Catalog, schema::Schema},
    data::{
        tuple::Tuple,
        types::{Data, DataType},
    },
    error::Result,
    logical_plan::LogicalPlan,
};
use error::{PlanError, PlanResult};
use sqlparser::ast::{Expr, ObjectName, Query, SetExpr, Value};
use std::ops::Deref;

fn check_column(
    table: &str,
    column_idx: usize,
    datatype: &DataType,
    value: &Value,
) -> PlanResult<Data> {
    match datatype {
        DataType::Bool => {
            let Value::Boolean(bool) = value else {
                return Err(PlanError::MismatchedType {
                    table: table.to_string(),
                    column_idx,
                    expect: *datatype,
                });
            };

            return Ok(Data::Bool(*bool));
        }
        DataType::Int64 => {
            let Value::Number(num_str, _) = &value else {
                return Err(PlanError::MismatchedType {
                    table: table.to_string(),
                    column_idx,
                    expect: *datatype,
                });
            };
            let num = num_str.parse::<i64>().map_err(|_| {
                PlanError::ConversionError {
                    val: value.clone(),
                    to: *datatype,
                }
            })?;

            return Ok(Data::Int64(num));
        }
        DataType::Float64 => {
            let Value::Number(num_str, _) = &value else {
                return Err(PlanError::MismatchedType {
                    table: table.to_string(),
                    column_idx,
                    expect: *datatype,
                });
            };
            let num = num_str.parse::<f64>().map_err(|_| {
                PlanError::ConversionError {
                    val: value.clone(),
                    to: *datatype,
                }
            })?;

            return Ok(Data::Float64(num));
        }
        DataType::Timestamp => {
            todo!()
        }
        DataType::String => {
            let str = match value {
                Value::SingleQuotedString(str) => str,
                Value::DollarQuotedString(dollar_quoted) => {
                    &dollar_quoted.value
                }
                Value::EscapedStringLiteral(str) => str,
                Value::SingleQuotedByteStringLiteral(str) => str,
                Value::DoubleQuotedByteStringLiteral(str) => str,
                Value::RawStringLiteral(str) => str,
                Value::NationalStringLiteral(str) => str,
                Value::HexStringLiteral(str) => str,
                Value::DoubleQuotedString(str) => str,
                Value::Placeholder(str) => str,
                Value::UnQuotedString(str) => str,
                _ => {
                    return Err(PlanError::MismatchedType {
                        table: table.to_string(),
                        column_idx,
                        expect: *datatype,
                    });
                }
            };

            Ok(Data::String(str.clone()))
        }
    }
}

fn check_row(
    table: &str,
    schema: &Schema,
    row: &Vec<Expr>,
) -> PlanResult<Tuple> {
    let n_column = schema.n_columns();
    let row_len = row.len();
    if n_column != row_len {
        return Err(PlanError::MismatchedNumberColumns {
            table: table.to_string(),
            expect: n_column,
            found: row_len,
        });
    }

    let mut tuple = Vec::with_capacity(n_column);
    let datatypes = schema.column_datatypes();
    for (idx, (datatype, columnn)) in datatypes.zip(row.into_iter()).enumerate()
    {
        let value = as_variant!(Expr::Value, columnn);
        let data = check_column(table, idx, datatype, value)?;

        tuple.push(data);
    }

    Ok(Tuple::new(tuple.into_iter()))
}

pub fn insert(
    catalog: &Catalog,
    table_name: &ObjectName,
    source: &Option<Box<Query>>,
) -> Result<LogicalPlan> {
    let table_name = table_name.to_string();
    let source = as_variant!(Some, source);
    let body = &source.body.deref();
    let values = as_variant!(SetExpr::Values, body);
    let rows = &values.rows;

    let table = catalog.get_table(&table_name)?;
    let table_scheam = table.schema();

    let mut tuples = Vec::with_capacity(rows.len());
    for row in rows {
        let tuple = check_row(&table_name, table_scheam, row)?;
        tuples.push(tuple);
    }

    Ok(LogicalPlan::Insert {
        table: table_name,
        rows: tuples,
    })
}

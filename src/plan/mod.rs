pub mod error;

use crate::{
    as_variant,
    catalog::{schema::Schema, Catalog},
    data::{
        tuple::Tuple,
        types::{Data, DataType},
    },
    error::{Error, Result},
    expr::{Expr, Operator},
    logical_plan::LogicalPlan,
};
use error::{PlanError, PlanResult};
use sqlparser::ast::{
    BinaryOperator, Expr as SqlExpr, ObjectName, Query, SetExpr, Value,
};
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

            Ok(Data::Bool(*bool))
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

            Ok(Data::Int64(num))
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

            Ok(Data::Float64(num))
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
    row: &[SqlExpr],
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
    for (idx, (datatype, columnn)) in datatypes.zip(row.iter()).enumerate() {
        let value = as_variant!(SqlExpr::Value, columnn);
        let data = check_column(table, idx, datatype, value)?;

        tuple.push(data);
    }

    Ok(Tuple::new(tuple))
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

fn value_to_data(val: Value) -> Result<Data> {
    match val {
        Value::Number(str, _) => {
            if let Ok(num) = str.parse::<i64>() {
                return Ok(Data::Int64(num));
            } else if let Ok(num) = str.parse::<f64>() {
                return Ok(Data::Float64(num));
            } else {
                return Err(Error::NotImplemented);
            }
        }
        Value::SingleQuotedString(str) => Ok(Data::String(str)),
        Value::DollarQuotedString(str) => Ok(Data::String(str.value)),
        Value::EscapedStringLiteral(str) => Ok(Data::String(str)),
        Value::SingleQuotedByteStringLiteral(str) => Ok(Data::String(str)),
        Value::DoubleQuotedByteStringLiteral(str) => Ok(Data::String(str)),
        Value::RawStringLiteral(str) => Ok(Data::String(str)),
        Value::NationalStringLiteral(str) => Ok(Data::String(str)),
        Value::HexStringLiteral(str) => Ok(Data::String(str)),
        Value::DoubleQuotedString(str) => Ok(Data::String(str)),
        Value::Boolean(val) => Ok(Data::Bool(val)),
        Value::Null => Err(Error::NotImplemented),
        Value::Placeholder(str) => Ok(Data::String(str)),
        Value::UnQuotedString(str) => Ok(Data::String(str)),
    }
}

fn convert_op(op: BinaryOperator) -> Result<Operator> {
    match op {
        BinaryOperator::Gt => Ok(Operator::Gt),
        BinaryOperator::GtEq => Ok(Operator::GtEq),
        BinaryOperator::Lt => Ok(Operator::Lt),
        BinaryOperator::LtEq => Ok(Operator::LtEq),
        BinaryOperator::LtEq => Ok(Operator::LtEq),
        BinaryOperator::Eq => Ok(Operator::Eq),
        BinaryOperator::NotEq => Ok(Operator::NotEq),
        BinaryOperator::Plus => Ok(Operator::Plus),
        BinaryOperator::Minus => Ok(Operator::Minus),
        _ => Err(Error::NotImplemented),
    }
}

pub fn convert_expr(schema: &Schema, sql_expr: &SqlExpr) -> Result<Expr> {
    match sql_expr {
        SqlExpr::Identifier(iden) => Ok(Expr::Column(iden.value.clone())),
        SqlExpr::Value(val) => {
            let data = value_to_data(val.clone())?;
            Ok(Expr::Literal(data))
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let left = convert_expr(schema, left.deref())?;
            let right = convert_expr(schema, right.deref())?;
            let op = convert_op(op.clone())?;

            Ok(Expr::BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }

        _ => Err(Error::NotImplemented),
    }
}

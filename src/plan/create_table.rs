//! For converting `Statement::CreateTable`.

use crate::{
    catalog::{error::CatalogError, schema::Schema, Catalog},
    error::{Error, Result},
    logical_plan::LogicalPlan,
    plan::object_name_to_table_name::object_name_to_table_name,
};
use sqlparser::ast::Statement;

/// Helper function to parse a `CreateTable` statement, and return the table name
/// and its schema.
///
/// # Undefined Behavior
///
/// The caller should ensure the passed `statement` should be a
/// `Statement::CreateTable`, or this function will be a UB.
pub(crate) unsafe fn create_table_to_name_schema(
    statement: Statement,
) -> Result<(String, Schema)> {
    match statement {
        Statement::CreateTable { name, columns, .. } => {
            let name = object_name_to_table_name(name)?;

            let mut cols = Vec::with_capacity(columns.len());
            for column in columns {
                cols.push((column.name.value, column.data_type.try_into()?));
            }
            let schema = Schema::new_with_duplicate_check(cols)?;

            Ok((name, schema))
        }
        _ => std::hint::unreachable_unchecked(),
    }
}

pub(crate) fn convert(
    catalog: &Catalog,
    statement: Statement,
) -> Result<LogicalPlan> {
    let sql = statement.to_string();
    match statement {
        Statement::CreateTable { name, columns, .. } => {
            let name = object_name_to_table_name(name)?;
            if catalog.contains_table(&name) {
                return Err(Error::CatalogError(CatalogError::TableExists {
                    name,
                }));
            }
            let mut cols = Vec::with_capacity(columns.len());
            for column in columns {
                cols.push((column.name.value, column.data_type.try_into()?));
            }
            let schema = Schema::new_with_duplicate_check(cols)?;

            Ok(LogicalPlan::CreateTable {
                name: name.to_string(),
                schema,
                pk: 0,
                sql,
            })
        }

        // SAFETY:
        // the `statement` is guaranteed to be a `Statement::CreateTable`
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

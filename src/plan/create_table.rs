//! For converting `Statement::CreateTable`.

use crate::{
    catalog::{schema::Schema, Catalog},
    error::Result,
    logical_plan::LogicalPlan,
    plan::object_name_to_table_name::object_name_to_table_name,
};
use sqlparser::ast::Statement;

pub(crate) fn convert(
    _catalog: &Catalog,
    statement: Statement,
) -> Result<LogicalPlan> {
    match statement {
        Statement::CreateTable { name, columns, .. } => {
            let name = object_name_to_table_name(name)?;
            let pk = 0;
            let mut cols = Vec::with_capacity(columns.len());
            for column in columns {
                cols.push((column.name.value, column.data_type.try_into()?));
            }
            let schema = Schema::new(cols)?;

            Ok(LogicalPlan::CreateTable {
                name: name.to_string(),
                schema,
                pk,
            })
        }
        // SAFETY:
        // it has already been checked
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

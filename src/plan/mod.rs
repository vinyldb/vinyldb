//! Converting a SQL AST to a Logical Plan.

pub mod create_table;
pub mod datatype;
pub mod error;
pub mod explain;
pub mod explain_table;
pub mod expr;
pub mod insert;
pub mod object_name_to_table_name;
pub mod op;
pub mod query;
pub mod show_tables;
pub mod value2data;
pub mod values2tuples;

use crate::{
    catalog::Catalog,
    error::{Error, Result},
    logical_plan::LogicalPlan,
    plan::error::{PlanError, UnimplementedFeature},
};
use sqlparser::ast::Statement;

/// Convert an SQL AST (`Statement)` to a [`LogicalPlan`].
pub(crate) fn statement_to_logical_plan(
    catalog: &Catalog,
    statement: Statement,
) -> Result<LogicalPlan> {
    match statement {
        Statement::CreateTable { .. } => {
            create_table::convert(catalog, statement)
        }
        Statement::Explain { .. } => explain::convert(catalog, statement),
        Statement::ShowTables { .. } => {
            show_tables::convert(catalog, statement)
        }

        Statement::ExplainTable { .. } => {
            explain_table::convert(catalog, statement)
        }
        Statement::Insert { .. } => insert::convert(catalog, statement),
        Statement::Query(_) => query::convert(catalog, statement),
        _ => Err(Error::PlanError(PlanError::Unimplemented(
            UnimplementedFeature::Statement { statement },
        ))),
    }
}

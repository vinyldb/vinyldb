use super::statement_to_logical_plan;
use crate::{catalog::Catalog, error::Result, logical_plan::LogicalPlan};
use sqlparser::ast::Statement;

pub(crate) fn convert(
    catalog: &Catalog,
    statement: Statement,
) -> Result<LogicalPlan> {
    match statement {
        Statement::Explain { statement, .. } => {
            let input = Box::into_inner(statement);
            let plan = Box::new(statement_to_logical_plan(catalog, input)?);
            Ok(LogicalPlan::Explain { plan })
        }
        // SAFETY:
        // it has already been checked
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

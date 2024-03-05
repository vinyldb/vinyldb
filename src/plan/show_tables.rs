use sqlparser::ast::Statement;
use crate::catalog::Catalog;
use crate::logical_plan::LogicalPlan;
use crate::error::Result;


pub(crate) fn convert(_catalog: &Catalog, statement: Statement) -> Result<LogicalPlan> {
    match statement {
        Statement::ShowTables {..} => {
            Ok(LogicalPlan::ShowTables) 
        },
        // SAFETY:
        // it has already been checked
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}
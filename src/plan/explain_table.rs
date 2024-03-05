use crate::{
    catalog::Catalog, error::Result, logical_plan::LogicalPlan,
    plan::object_name_to_table_name::object_name_to_table_name,
};
use sqlparser::ast::Statement;

pub(crate) fn convert(
    _catalog: &Catalog,
    statement: Statement,
) -> Result<LogicalPlan> {
    match statement {
        Statement::ExplainTable { table_name, .. } => {
            let table_name = object_name_to_table_name(table_name)?;
            Ok(LogicalPlan::DescribeTable {
                name: table_name.to_string(),
            })
        }
        // SAFETY:
        // it has already been checked
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

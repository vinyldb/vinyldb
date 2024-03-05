use super::{
    error::{PlanError, UnimplementedFeature},
    expr::convert_expr,
};
use crate::{
    catalog::Catalog,
    error::{Error, Result},
    logical_plan::LogicalPlan,
    plan::object_name_to_table_name::object_name_to_table_name,
};
use sqlparser::ast::{SetExpr, Statement, TableFactor};

pub(crate) fn convert(
    catalog: &Catalog,
    statement: Statement,
) -> Result<LogicalPlan> {
    match statement.clone() {
        Statement::Query(query) => {
            let query = Box::into_inner(query);
            let body = Box::into_inner(query.body);
            let SetExpr::Select(select) = body else {
                return Err(Error::PlanError(PlanError::Unimplemented(
                    UnimplementedFeature::Statement { statement },
                )));
            };
            let mut from = select.from;
            if from.len() != 1 {
                return Err(Error::PlanError(PlanError::Unimplemented(
                    UnimplementedFeature::Statement { statement },
                )));
            }
            let from = from.pop().expect("should have exactly 1 element");
            if !from.joins.is_empty() {
                return Err(Error::PlanError(PlanError::Unimplemented(
                    UnimplementedFeature::Statement { statement },
                )));
            }
            let relation = from.relation;
            let TableFactor::Table { name, .. } = relation else {
                return Err(Error::PlanError(PlanError::Unimplemented(
                    UnimplementedFeature::Statement { statement },
                )));
            };
            let table_name = object_name_to_table_name(name)?;
            // check catalog
            let table = catalog.get_table(&table_name)?;
            let schema = table.schema();

            let mut base = LogicalPlan::TableScan { name: table_name };

            if let Some(expr) = select.selection {
                let expr = convert_expr(schema, expr)?;
                base = LogicalPlan::Filter {
                    predicate: expr,
                    input: Box::new(base),
                };
            }

            Ok(base)
        }
        // SAFETY:
        // it has already been checked
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

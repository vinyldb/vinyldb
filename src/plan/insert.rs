use super::error::{PlanError, UnimplementedFeature};
use crate::{
    catalog::Catalog,
    error::{Error, Result},
    logical_plan::LogicalPlan,
    plan::{
        object_name_to_table_name::object_name_to_table_name,
        values2tuples::values_to_tuples,
    },
};
use sqlparser::ast::{SetExpr, Statement};

pub(crate) fn convert(
    catalog: &Catalog,
    statement: Statement,
) -> Result<LogicalPlan> {
    match statement.clone() {
        Statement::Insert {
            table_name, source, ..
        } => {
            let table_name = object_name_to_table_name(table_name)?;
            // check catalog
            catalog.get_table(&table_name)?;
            let Some(source) = source else {
                return Err(Error::PlanError(PlanError::Unimplemented(
                    UnimplementedFeature::Statement { statement },
                )));
            };
            let source = Box::into_inner(source);
            let body = Box::into_inner(source.body);
            let SetExpr::Values(values) = body else {
                return Err(Error::PlanError(PlanError::Unimplemented(
                    UnimplementedFeature::Statement { statement },
                )));
            };
            let tuples = values_to_tuples(catalog, &table_name, values)?;
            Ok(LogicalPlan::Insert {
                table: table_name,
                rows: tuples,
            })
        }
        // SAFETY:
        // it has already been checked
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

use crate::{
    as_variant,
    catalog::Catalog,
    data::tuple::Tuple,
    error::{Error, Result},
    expr::Expr,
    plan::{error::PlanError, expr::convert_expr},
};
use sqlparser::ast::Values;

/// Convert `values` to a `Vec<Tuple>`, with schema check.
pub fn values_to_tuples(
    catalog: &Catalog,
    table_name: &str,
    values: Values,
) -> Result<Vec<Tuple>> {
    let table = catalog.get_table(table_name)?;
    let schema = table.schema();
    let n_columns = table.n_columns();
    let rows = values.rows;
    let mut tuples = Vec::new();
    for row in rows {
        if n_columns != row.len() {
            return Err(Error::PlanError(PlanError::MismatchedNumberColumns {
                table: table_name.to_string(),
                expected: n_columns,
                found: row.len(),
            }));
        }

        let datatypes = schema.column_datatypes();
        let mut tuple = Vec::with_capacity(n_columns);
        for (idx, (expected_datatype, expr)) in
            datatypes.zip(row.into_iter()).enumerate()
        {
            let expr = convert_expr(expr)?;
            let data = as_variant!(Expr::Literal, expr);
            let datatype = data.datatype();
            if &datatype != expected_datatype {
                return Err(Error::PlanError(PlanError::MismatchedType {
                    table: table_name.to_string(),
                    column_idx: idx,
                    expected: *expected_datatype,
                    found: datatype,
                }));
            };

            tuple.push(data);
        }

        tuples.push(Tuple::new(tuple));
    }

    Ok(tuples)
}

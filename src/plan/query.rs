use super::{
    error::{PlanError, UnimplementedFeature},
    expr::convert_expr,
};
use crate::{
    catalog::{schema::Schema, Catalog},
    data::types::{Data, DataType},
    error::{Error, Result},
    expr::Expr,
    logical_plan::LogicalPlan,
    plan::object_name_to_table_name::object_name_to_table_name,
};
use sqlparser::ast::{
    Expr as SQLExpr, SelectItem, SetExpr, Statement, TableFactor,
};
use std::num::NonZeroUsize;

fn evaluate_limit(expr: SQLExpr, input_schema: &Schema) -> Result<usize> {
    let expr = convert_expr(input_schema, expr)?;
    let data = expr.evaluate_as_constant();
    let Data::Int64(limit) = data else {
        return Err(Error::PlanError(PlanError::NonUintLimitOffset {
            expr: data,
        }));
    };

    let limit: usize = limit.try_into().map_err(|_| {
        Error::PlanError(PlanError::NonUintLimitOffset { expr: data })
    })?;

    Ok(limit)
}

fn evaluate_offset(
    expr: SQLExpr,
    input_schema: &Schema,
) -> Result<Option<NonZeroUsize>> {
    let offset = evaluate_limit(expr, input_schema)?;
    let offset = match offset {
        0 => None,
        // SAFETY: it won't be 0
        non_zero => Some(unsafe { NonZeroUsize::new_unchecked(non_zero) }),
    };

    Ok(offset)
}

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

            match (query.limit, query.offset) {
                (Some(limit), Some(offset)) => {
                    let limit = Some(evaluate_limit(limit, schema)?);
                    let offset = evaluate_offset(offset.value, schema)?;

                    base = LogicalPlan::Limit {
                        offset,
                        limit,
                        input: Box::new(base),
                    }
                }
                (Some(limit), None) => {
                    let limit = Some(evaluate_limit(limit, schema)?);
                    base = LogicalPlan::Limit {
                        offset: None,
                        limit,
                        input: Box::new(base),
                    }
                }
                (None, Some(offset)) => {
                    let offset = evaluate_offset(offset.value, schema)?;
                    if offset.is_some() {
                        base = LogicalPlan::Limit {
                            offset,
                            limit: None,
                            input: Box::new(base),
                        }
                    }
                }
                (None, None) => { /*do nothing*/ }
            }

            let projs = select.projection;
            assert!(!projs.is_empty());
            let need_a_projection_logical_plan = !(projs.len() == 1
                && matches!(projs[0], SelectItem::Wildcard(_)));
            if need_a_projection_logical_plan {
                let mut exprs: Vec<Expr> = Vec::new();
                let mut columns: Vec<(String, DataType)> = Vec::new();
                for proj in projs {
                    match proj {
                        SelectItem::UnnamedExpr(expr) => {
                            let expr = convert_expr(schema, expr)?;
                            let Expr::Column(column) = &expr else {
                                return Err(Error::PlanError(PlanError::Unimplemented(UnimplementedFeature::ProjectionWithNonColumnExpr {
                                    expr,
                                })));
                            };
                            let column_datatype =
                                schema.column_datatype(column)?;
                            columns.push((column.clone(), *column_datatype));
                            exprs.push(expr);
                        }
                        SelectItem::ExprWithAlias { .. } => {
                            return Err(Error::PlanError(
                                PlanError::Unimplemented(
                                    UnimplementedFeature::ProjectionWithAlias {
                                        select_item: proj,
                                    },
                                ),
                            ));
                        }
                        SelectItem::QualifiedWildcard(_, _) => {
                            return Err(Error::PlanError(
                                PlanError::Unimplemented(
                                    UnimplementedFeature::ProjectionQualifiedWildcard {
                                        select_item: proj,
                                    },
                                ),
                            ));
                        }
                        SelectItem::Wildcard(_) => {
                            columns.extend(schema.columns().map(
                                |(name, datatype)| (name.clone(), *datatype),
                            ));
                            exprs.extend(
                                schema
                                    .column_names()
                                    .map(|name| Expr::Column(name.to_string())),
                            );
                        }
                    }
                }

                let schema = Schema::new(columns)?;
                base = LogicalPlan::Projection {
                    expr: exprs,
                    schema,
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

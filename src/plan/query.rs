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
    Expr as SQLExpr, Query, SelectItem, SetExpr, Statement, TableFactor,
};
use std::{num::NonZeroUsize, ops::Deref};

fn evaluate_limit(expr: SQLExpr) -> Result<usize> {
    let expr = convert_expr(expr)?;
    assert!(expr.is_constant());
    let data = expr.evaluate_constant_expr().unwrap();
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

fn evaluate_offset(expr: SQLExpr) -> Result<Option<NonZeroUsize>> {
    let offset = evaluate_limit(expr)?;
    let offset = match offset {
        0 => None,
        // SAFETY: it won't be 0
        non_zero => Some(unsafe { NonZeroUsize::new_unchecked(non_zero) }),
    };

    Ok(offset)
}

fn select_without_from(query: Query) -> Result<LogicalPlan> {
    let body = Box::into_inner(query.body);
    let SetExpr::Select(select) = body else {
        unreachable!()
    };

    let mut base = LogicalPlan::OneRowPlaceholder;

    if let Some(expr) = select.selection {
        let expr = convert_expr(expr)?;
        if !expr.is_constant() {
            return Err(Error::PlanError(
                PlanError::NonConstantExprWithoutFrom { expr },
            ));
        }

        base = LogicalPlan::Filter {
            predicate: expr,
            input: Box::new(base),
        };
    }

    match (query.limit, query.offset) {
        (Some(limit), Some(offset)) => {
            let limit = Some(evaluate_limit(limit)?);
            let offset = evaluate_offset(offset.value)?;

            base = LogicalPlan::Limit {
                offset,
                limit,
                input: Box::new(base),
            }
        }
        (Some(limit), None) => {
            let limit = Some(evaluate_limit(limit)?);
            base = LogicalPlan::Limit {
                offset: None,
                limit,
                input: Box::new(base),
            }
        }
        (None, Some(offset)) => {
            let offset = evaluate_offset(offset.value)?;
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

    let mut exprs: Vec<Expr> = Vec::new();
    let mut columns: Vec<(String, DataType)> = Vec::new();
    for proj in projs {
        match proj {
            SelectItem::UnnamedExpr(expr) => {
                let expr = convert_expr(expr)?;
                if !expr.is_constant() {
                    return Err(Error::PlanError(
                        PlanError::NonConstantExprWithoutFrom { expr },
                    ));
                }
                columns.push((
                    expr.to_string(),
                    expr.datatype_of_constant_expr()?,
                ));
                exprs.push(expr);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = convert_expr(expr)?;
                if !expr.is_constant() {
                    return Err(Error::PlanError(
                        PlanError::NonConstantExprWithoutFrom { expr },
                    ));
                }
                columns.push((alias.value, expr.datatype_of_constant_expr()?));
                exprs.push(expr);
            }
            // treat `QualifiedWildcard` like `Wildcard` because we don't support databases.
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {
                return Err(Error::PlanError(PlanError::WildcardWithoutFrom));
            }
        }
    }

    let schema = Schema::new(columns);
    base = LogicalPlan::Projection {
        expr: exprs,
        schema,
        input: Box::new(base),
    };

    Ok(base)
}

fn select_with_from(
    catalog: &Catalog,
    query: Query,
    statement: Statement,
) -> Result<LogicalPlan> {
    let body = Box::into_inner(query.body);
    let SetExpr::Select(select) = body else {
        unreachable!()
    };

    let mut from = select.from;
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
        let expr = convert_expr(expr)?;
        base = LogicalPlan::Filter {
            predicate: expr,
            input: Box::new(base),
        };
    }

    match (query.limit, query.offset) {
        (Some(limit), Some(offset)) => {
            let limit = Some(evaluate_limit(limit)?);
            let offset = evaluate_offset(offset.value)?;

            base = LogicalPlan::Limit {
                offset,
                limit,
                input: Box::new(base),
            }
        }
        (Some(limit), None) => {
            let limit = Some(evaluate_limit(limit)?);
            base = LogicalPlan::Limit {
                offset: None,
                limit,
                input: Box::new(base),
            }
        }
        (None, Some(offset)) => {
            let offset = evaluate_offset(offset.value)?;
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

    let mut exprs: Vec<Expr> = Vec::new();
    let mut columns: Vec<(String, DataType)> = Vec::new();
    for proj in projs {
        match proj {
            SelectItem::UnnamedExpr(expr) => {
                let expr = convert_expr(expr)?;
                columns.push((expr.to_string(), expr.datatype(schema)?));
                exprs.push(expr);
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = convert_expr(expr)?;
                columns.push((alias.value, expr.datatype(schema)?));
                exprs.push(expr);
            }
            // treat `QualifiedWildcard` like `Wildcard` because we don't support databases.
            SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => {
                exprs.extend(
                    schema
                        .column_names()
                        .map(|name| Expr::Column(name.to_string())),
                );
                columns.extend(
                    schema
                        .columns()
                        .map(|(name, datatype)| (name.clone(), *datatype)),
                );
            }
        }
    }

    let schema = Schema::new(columns);
    base = LogicalPlan::Projection {
        expr: exprs,
        schema,
        input: Box::new(base),
    };

    Ok(base)
}

pub(crate) fn convert(
    catalog: &Catalog,
    statement: Statement,
) -> Result<LogicalPlan> {
    match statement.clone() {
        Statement::Query(query) => {
            let SetExpr::Select(ref select) = query.body.deref() else {
                return Err(Error::PlanError(PlanError::Unimplemented(
                    UnimplementedFeature::Statement { statement },
                )));
            };
            let select_from_len = select.from.len();
            let query = Box::into_inner(query);

            match select_from_len {
                0 => Ok(select_without_from(query)?),
                1 => Ok(select_with_from(catalog, query, statement)?),
                _ => {
                    return Err(Error::PlanError(PlanError::Unimplemented(
                        UnimplementedFeature::Statement { statement },
                    )));
                }
            }
        }
        // SAFETY:
        // it has already been checked
        _ => unsafe { std::hint::unreachable_unchecked() },
    }
}

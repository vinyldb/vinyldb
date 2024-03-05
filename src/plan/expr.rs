use super::{
    error::{PlanError, PlanResult},
    op::convert_op,
    value2data::value_to_data,
};
use crate::{catalog::schema::Schema, expr::Expr};
use sqlparser::ast::Expr as SqlExpr;
use std::ops::Deref;
use crate::plan::error::UnimplementedFeature;

pub fn convert_expr(_schema: &Schema, sql_expr: SqlExpr) -> PlanResult<Expr> {
    match sql_expr {
        SqlExpr::Identifier(iden) => Ok(Expr::Column(iden.value)),
        SqlExpr::Value(val) => {
            let data = value_to_data(val)?;
            Ok(Expr::Literal(data))
        }
        SqlExpr::BinaryOp { left, op, right } => {
            let left = convert_expr(_schema, left.deref().clone())?;
            let right = convert_expr(_schema, right.deref().clone())?;
            let op = convert_op(op)?;

            Ok(Expr::BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        }
        expr => Err(PlanError::Unimplemented(UnimplementedFeature::Expr {expr}))
    }
}

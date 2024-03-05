use crate::{
    expr::Operator,
    plan::error::{PlanError, PlanResult, UnimplementedFeature},
};
use sqlparser::ast::BinaryOperator;

pub(crate) fn convert_op(op: BinaryOperator) -> PlanResult<Operator> {
    match op {
        BinaryOperator::Gt => Ok(Operator::Gt),
        BinaryOperator::GtEq => Ok(Operator::GtEq),
        BinaryOperator::Lt => Ok(Operator::Lt),
        BinaryOperator::LtEq => Ok(Operator::LtEq),
        BinaryOperator::Eq => Ok(Operator::Eq),
        BinaryOperator::NotEq => Ok(Operator::NotEq),
        BinaryOperator::Plus => Ok(Operator::Plus),
        BinaryOperator::Minus => Ok(Operator::Minus),
        BinaryOperator::And => Ok(Operator::And),
        BinaryOperator::Or => Ok(Operator::Or),
        op => Err(PlanError::Unimplemented(UnimplementedFeature::Operator {
            op,
        })),
    }
}

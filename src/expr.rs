//! Our expression types and operators that can be applied to types.

use crate::{
    catalog::schema::Schema,
    data::{
        tuple::Tuple,
        types::{Data, DataType},
    },
    error::{Error, Result},
    plan::error::{ExprEvaluationError, PlanError},
};
use derive_more::Display;

#[derive(Debug, Clone, Display)]
pub enum Expr {
    /// A named column
    Column(String),
    /// A literal value
    Literal(Data),
    #[display(fmt = "{} {} {}", left, op, right)]
    /// Binary operation.
    BinaryExpr {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
}

impl Expr {
    /// Figuring out if this `Expr` is a constant, or can be evaluated as a constant.
    ///
    /// An `Expr` is a constant as long as it does not involve `Column`s
    pub fn is_constant(&self) -> bool {
        match self {
            Expr::Literal(_) => true,
            Expr::BinaryExpr { left, right, .. } => {
                left.is_constant() && right.is_constant()
            }

            _ => false,
        }
    }

    /// Evaluate `self` against every row in `data`.
    ///
    /// # NOTE
    /// All the tuples in `data` should have schema `schema`.
    pub fn evaluate(&self, schema: &Schema, data: &Tuple) -> Result<Data> {
        let ret = match self {
            Expr::Column(col_name) => {
                let idx = schema.index_of_column(col_name)?;
                data.get(idx).expect("schema error").clone()
            }
            Expr::Literal(literal) => literal.clone(),
            Expr::BinaryExpr { left, op, right } => {
                let left = left.evaluate(schema, data)?;
                let right = right.evaluate(schema, data)?;
                let data = op.operate(left, right)?;

                data
            }
        };

        Ok(ret)
    }

    /// Evaluate `Expr`s, in batch.
    pub fn evaluate_batch(
        exprs: &[Expr],
        schema: &Schema,
        data: &Tuple,
    ) -> Result<Vec<Data>> {
        let mut result = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let res = expr.evaluate(schema, data)?;
            result.push(res);
        }
        Ok(result)
    }

    /// Evaluate this `Expr` to a constant
    ///
    /// # Panic
    /// This `Expr` must be a constant, or this function will panic.
    pub fn evaluate_constant_expr(&self) -> Result<Data> {
        match self {
            Expr::Literal(data) => Ok(data.clone()),
            Expr::BinaryExpr { left, op, right } => {
                let left = left.evaluate_constant_expr()?;
                let right = right.evaluate_constant_expr()?;
                let data = op.operate(left, right)?;

                Ok(data)
            }
            _ => Err(Error::PlanError(PlanError::ExprEvaluationError(
                ExprEvaluationError::ExprIsNotConstant { expr: self.clone() },
            ))),
        }
    }

    /// Return the datatype of this `Expr`.
    pub fn datatype(&self, schema: &Schema) -> Result<DataType> {
        match self {
            Expr::Column(col_name) => Ok(*schema.column_datatype(col_name)?),
            Expr::Literal(data) => Ok(data.datatype()),
            Expr::BinaryExpr { left, op, right } => {
                let left_datatype = left.datatype(schema)?;
                let right_datatype = right.datatype(schema)?;

                let dt =
                    op.datatype_of_operation(&left_datatype, &right_datatype)?;
                Ok(dt)
            }
        }
    }

    /// Assume this `Expr` is a constant, return the datatype of this `Expr`.
    pub fn datatype_of_constant_expr(&self) -> Result<DataType> {
        match self {
            Expr::Literal(data) => Ok(data.datatype()),
            Expr::BinaryExpr { left, op, right } => {
                let left_datatype = left.datatype_of_constant_expr()?;
                let right_datatype = right.datatype_of_constant_expr()?;

                let dt =
                    op.datatype_of_operation(&left_datatype, &right_datatype)?;
                Ok(dt)
            }

            _ => Err(Error::PlanError(PlanError::ExprEvaluationError(
                ExprEvaluationError::ExprIsNotConstant { expr: self.clone() },
            ))),
        }
    }
}

/// Operators supported by VinylDB.
#[derive(Debug, Copy, Clone, Display)]
pub enum Operator {
    #[display(fmt = ">")]
    /// >
    Gt,
    #[display(fmt = ">=")]
    /// >=
    GtEq,
    #[display(fmt = ">")]
    /// <
    Lt,
    #[display(fmt = ">=")]
    /// <=
    LtEq,
    #[display(fmt = "=")]
    /// =
    Eq,
    #[display(fmt = "!=")]
    /// !=
    NotEq,
    #[display(fmt = "+")]
    /// +
    Plus,
    #[display(fmt = "-")]
    /// -
    Minus,
    #[display(fmt = "AND")]
    /// AND
    And,
    #[display(fmt = "OR")]
    /// OR
    Or,
}

impl Operator {
    /// Operate on `lhs` and `rhs`.
    pub fn operate(&self, lhs: Data, rhs: Data) -> Result<Data> {
        // Currently, all our Operators require `lhs` and `rhs` should have the same type.
        // this may change in the future.
        let lhs_dt = lhs.datatype();
        let rhs_dt = rhs.datatype();
        if lhs_dt != rhs_dt {
            return Err(Error::PlanError(PlanError::ExprEvaluationError(
                ExprEvaluationError::DoOpOnDiffTypes {
                    lhs: lhs_dt,
                    op: *self,
                    rhs: rhs_dt,
                },
            )));
        }

        let result = match self {
            Operator::Gt => Data::Bool(lhs > rhs),
            Operator::GtEq => Data::Bool(lhs >= rhs),
            Operator::Lt => Data::Bool(lhs < rhs),
            Operator::LtEq => Data::Bool(lhs <= rhs),
            Operator::Eq => Data::Bool(lhs == rhs),
            Operator::NotEq => Data::Bool(lhs != rhs),
            Operator::Plus => {
                if lhs_dt != DataType::Int64 && lhs_dt != DataType::Float64 {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::UnsupportedTypeForOp {
                                datatype: lhs_dt,
                                op: *self,
                            },
                        ),
                    ));
                }

                lhs + rhs
            }
            Operator::Minus => {
                if lhs_dt != DataType::Int64 && lhs_dt != DataType::Float64 {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::UnsupportedTypeForOp {
                                datatype: lhs_dt,
                                op: *self,
                            },
                        ),
                    ));
                }

                lhs - rhs
            }
            Operator::And => {
                let Data::Bool(left) = lhs else {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::UnsupportedTypeForOp {
                                datatype: lhs.datatype(),
                                op: *self,
                            },
                        ),
                    ));
                };
                let Data::Bool(right) = rhs else {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::UnsupportedTypeForOp {
                                datatype: rhs.datatype(),
                                op: *self,
                            },
                        ),
                    ));
                };

                Data::Bool(left && right)
            }
            Operator::Or => {
                let Data::Bool(left) = lhs else {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::UnsupportedTypeForOp {
                                datatype: lhs.datatype(),
                                op: *self,
                            },
                        ),
                    ));
                };
                let Data::Bool(right) = rhs else {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::UnsupportedTypeForOp {
                                datatype: rhs.datatype(),
                                op: *self,
                            },
                        ),
                    ));
                };

                Data::Bool(left || right)
            }
        };

        Ok(result)
    }

    /// Return the datatype of `lhs op rhs`.
    pub fn datatype_of_operation(
        &self,
        lhs_dt: &DataType,
        rhs_dt: &DataType,
    ) -> Result<DataType> {
        // Currently, all our Operators require `lhs` and `rhs` should have the same type.
        // this may change in the future.
        if lhs_dt != rhs_dt {
            return Err(Error::PlanError(PlanError::ExprEvaluationError(
                ExprEvaluationError::DoOpOnDiffTypes {
                    lhs: *lhs_dt,
                    op: *self,
                    rhs: *rhs_dt,
                },
            )));
        }

        match self {
            Operator::Gt
            | Operator::GtEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Eq
            | Operator::NotEq => Ok(DataType::Bool),
            Operator::Plus | Operator::Minus => {
                if lhs_dt != &DataType::Int64 && lhs_dt != &DataType::Float64 {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::UnsupportedTypeForOp {
                                datatype: *lhs_dt,
                                op: *self,
                            },
                        ),
                    ));
                }

                Ok(*lhs_dt)
            }
            Operator::And | Operator::Or => {
                if lhs_dt != &DataType::Bool {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::UnsupportedTypeForOp {
                                datatype: *lhs_dt,
                                op: *self,
                            },
                        ),
                    ));
                }

                Ok(DataType::Bool)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn test_schema() -> Schema {
        Schema::new_with_duplicate_check([
            ("name".into(), DataType::String),
            ("age".into(), DataType::Int64),
            ("score".into(), DataType::Int64),
            ("graduated".into(), DataType::Bool),
        ])
        .unwrap()
    }

    fn test_tuple() -> Tuple {
        Tuple::new([
            Data::String("steve".into()),
            Data::Int64(18),
            Data::Int64(0),
            Data::Bool(true),
        ])
    }

    #[test]
    fn evaluate_column_expr() {
        let schema = test_schema();
        let tuple = test_tuple();

        let column_expr = Expr::Column("name".into());
        assert_eq!(
            column_expr.evaluate(&schema, &tuple).unwrap(),
            Data::String("steve".into())
        );

        let column_expr = Expr::Column("age".into());
        assert_eq!(
            column_expr.evaluate(&schema, &tuple).unwrap(),
            Data::Int64(18)
        );

        let column_expr = Expr::Column("score".into());
        assert_eq!(
            column_expr.evaluate(&schema, &tuple).unwrap(),
            Data::Int64(0)
        );
    }

    #[test]
    fn evaluate_literal_expr() {
        let schema = test_schema();
        let tuple = test_tuple();

        let column_expr = Expr::Literal(Data::String("steve".into()));
        assert_eq!(
            column_expr.evaluate(&schema, &tuple).unwrap(),
            Data::String("steve".into())
        );

        let column_expr = Expr::Literal(Data::Int64(18));
        assert_eq!(
            column_expr.evaluate(&schema, &tuple).unwrap(),
            Data::Int64(18)
        );

        let column_expr = Expr::Literal(Data::Int64(0));
        assert_eq!(
            column_expr.evaluate(&schema, &tuple).unwrap(),
            Data::Int64(0)
        );
    }

    #[test]
    fn evaluate_binary_expr() {
        let schema = test_schema();
        let tuple = test_tuple();

        let column_expr = Expr::Column("age".into());
        let literal_expr = Expr::Literal(Data::Int64(17));
        let gt = Expr::BinaryExpr {
            left: Box::new(column_expr.clone()),
            op: Operator::Gt,
            right: Box::new(literal_expr.clone()),
        };
        assert_eq!(gt.evaluate(&schema, &tuple).unwrap(), Data::Bool(true));

        let gteq = Expr::BinaryExpr {
            left: Box::new(column_expr.clone()),
            op: Operator::GtEq,
            right: Box::new(literal_expr.clone()),
        };
        assert_eq!(gteq.evaluate(&schema, &tuple).unwrap(), Data::Bool(true));

        let lt = Expr::BinaryExpr {
            left: Box::new(column_expr.clone()),
            op: Operator::Lt,
            right: Box::new(literal_expr.clone()),
        };
        assert_eq!(lt.evaluate(&schema, &tuple).unwrap(), Data::Bool(false));

        let lteq = Expr::BinaryExpr {
            left: Box::new(column_expr.clone()),
            op: Operator::LtEq,
            right: Box::new(literal_expr.clone()),
        };
        assert_eq!(lteq.evaluate(&schema, &tuple).unwrap(), Data::Bool(false));

        let eq = Expr::BinaryExpr {
            left: Box::new(column_expr.clone()),
            op: Operator::Eq,
            right: Box::new(literal_expr.clone()),
        };
        assert_eq!(eq.evaluate(&schema, &tuple).unwrap(), Data::Bool(false));

        let not_eq = Expr::BinaryExpr {
            left: Box::new(column_expr.clone()),
            op: Operator::NotEq,
            right: Box::new(literal_expr.clone()),
        };
        assert_eq!(not_eq.evaluate(&schema, &tuple).unwrap(), Data::Bool(true));

        let plus = Expr::BinaryExpr {
            left: Box::new(column_expr.clone()),
            op: Operator::Plus,
            right: Box::new(literal_expr.clone()),
        };
        assert_eq!(plus.evaluate(&schema, &tuple).unwrap(), Data::Int64(35));

        let minus = Expr::BinaryExpr {
            left: Box::new(column_expr.clone()),
            op: Operator::Minus,
            right: Box::new(literal_expr.clone()),
        };
        assert_eq!(minus.evaluate(&schema, &tuple).unwrap(), Data::Int64(1));
    }

    #[test]
    #[should_panic]
    fn plus_2_booleans() {
        let schema = test_schema();
        let tuple = test_tuple();

        let graduated = Expr::Column("graduated".into());
        let plus_bool = Expr::BinaryExpr {
            left: Box::new(graduated.clone()),
            op: Operator::Plus,
            right: Box::new(graduated.clone()),
        };
        plus_bool.evaluate(&schema, &tuple).unwrap();
    }

    #[test]
    #[should_panic]
    fn minus_2_booleans() {
        let schema = test_schema();
        let tuple = test_tuple();

        let graduated = Expr::Column("graduated".into());
        let minus_bool = Expr::BinaryExpr {
            left: Box::new(graduated.clone()),
            op: Operator::Minus,
            right: Box::new(graduated.clone()),
        };
        minus_bool.evaluate(&schema, &tuple).unwrap();
    }

    #[test]
    #[should_panic]
    fn plus_2_strings() {
        let schema = test_schema();
        let tuple = test_tuple();

        let name = Expr::Column("name".into());
        let plus_bool = Expr::BinaryExpr {
            left: Box::new(name.clone()),
            op: Operator::Plus,
            right: Box::new(name.clone()),
        };
        plus_bool.evaluate(&schema, &tuple).unwrap();
    }

    #[test]
    #[should_panic]
    fn minus_2_strings() {
        let schema = test_schema();
        let tuple = test_tuple();

        let name = Expr::Column("name".into());
        let minus_bool = Expr::BinaryExpr {
            left: Box::new(name.clone()),
            op: Operator::Minus,
            right: Box::new(name.clone()),
        };
        minus_bool.evaluate(&schema, &tuple).unwrap();
    }
}

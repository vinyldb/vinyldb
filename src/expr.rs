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
                // Currently, all our Operators require `lhs` and `rhs` should have the same type.
                // this may change in the future.
                let lhs = left.datatype();
                let rhs = right.datatype();
                if lhs != rhs {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::DoOpOnDiffTypes {
                                lhs,
                                op: *op,
                                rhs,
                            },
                        ),
                    ));
                }

                match op {
                    Operator::Gt => Data::Bool(left > right),
                    Operator::GtEq => Data::Bool(left >= right),
                    Operator::Lt => Data::Bool(left < right),
                    Operator::LtEq => Data::Bool(left <= right),
                    Operator::Eq => Data::Bool(left == right),
                    Operator::NotEq => Data::Bool(left != right),
                    Operator::Plus => {
                        if lhs != DataType::Int64 && lhs != DataType::Float64 {
                            return Err(Error::PlanError(
                                PlanError::ExprEvaluationError(
                                    ExprEvaluationError::UnsupportedTypeForOp {
                                        datatype: lhs,
                                        op: *op,
                                    },
                                ),
                            ));
                        }

                        left + right
                    }
                    Operator::Minus => {
                        if lhs != DataType::Int64 && lhs != DataType::Float64 {
                            return Err(Error::PlanError(
                                PlanError::ExprEvaluationError(
                                    ExprEvaluationError::UnsupportedTypeForOp {
                                        datatype: lhs,
                                        op: *op,
                                    },
                                ),
                            ));
                        }

                        left - right
                    }
                    Operator::And => {
                        let Data::Bool(left) = left else {
                            return Err(Error::PlanError(
                                PlanError::ExprEvaluationError(
                                    ExprEvaluationError::UnsupportedTypeForOp {
                                        datatype: left.datatype(),
                                        op: *op,
                                    },
                                ),
                            ));
                        };
                        let Data::Bool(right) = right else {
                            return Err(Error::PlanError(
                                PlanError::ExprEvaluationError(
                                    ExprEvaluationError::UnsupportedTypeForOp {
                                        datatype: right.datatype(),
                                        op: *op,
                                    },
                                ),
                            ));
                        };

                        Data::Bool(left && right)
                    }
                    Operator::Or => {
                        let Data::Bool(left) = left else {
                            return Err(Error::PlanError(
                                PlanError::ExprEvaluationError(
                                    ExprEvaluationError::UnsupportedTypeForOp {
                                        datatype: left.datatype(),
                                        op: *op,
                                    },
                                ),
                            ));
                        };
                        let Data::Bool(right) = right else {
                            return Err(Error::PlanError(
                                PlanError::ExprEvaluationError(
                                    ExprEvaluationError::UnsupportedTypeForOp {
                                        datatype: right.datatype(),
                                        op: *op,
                                    },
                                ),
                            ));
                        };

                        Data::Bool(left || right)
                    }
                }
            }
        };

        Ok(ret)
    }

    /// Evaluate this `Expr` to a constant
    ///
    /// # NOTE
    /// This `Expr` must not rely on external data, e.g., tuple or schema, or this
    /// function will panic.
    pub fn evaluate_as_constant(&self) -> Data {
        // TODO: BinaryExpr can be a constant.
        match self {
            Expr::Literal(data) => data.clone(),
            _ => panic!("trying to evaluate a non-constant Expr to a constant"),
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
                // Currently, all our Operators require `lhs` and `rhs` should have the same type.
                // this may change in the future.
                if left_datatype != right_datatype {
                    return Err(Error::PlanError(
                        PlanError::ExprEvaluationError(
                            ExprEvaluationError::DoOpOnDiffTypes {
                                lhs: left_datatype,
                                op: *op,
                                rhs: right_datatype,
                            },
                        ),
                    ));
                }

                match op {
                    Operator::Gt
                    | Operator::GtEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Eq
                    | Operator::NotEq => Ok(DataType::Bool),
                    Operator::Plus | Operator::Minus => {
                        if left_datatype != DataType::Int64
                            || left_datatype != DataType::Float64
                        {
                            return Err(Error::PlanError(
                                PlanError::ExprEvaluationError(
                                    ExprEvaluationError::UnsupportedTypeForOp {
                                        datatype: left_datatype,
                                        op: *op,
                                    },
                                ),
                            ));
                        }

                        Ok(left_datatype)
                    }
                    Operator::And | Operator::Or => {
                        if left_datatype != DataType::Bool {
                            return Err(Error::PlanError(
                                PlanError::ExprEvaluationError(
                                    ExprEvaluationError::UnsupportedTypeForOp {
                                        datatype: left_datatype,
                                        op: *op,
                                    },
                                ),
                            ));
                        }

                        Ok(DataType::Bool)
                    }
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn test_schema() -> Schema {
        Schema::new([
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

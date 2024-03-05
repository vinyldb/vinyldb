use crate::{
    data::types::{Data, DataType},
    expr::{Expr, Operator},
};
use derive_more::{Display, Error};
use sqlparser::ast::{
    BinaryOperator, DataType as SQLDataType, Expr as SQLExpr, ObjectName,
    SelectItem, Statement, Value,
};

/// Features that have not been supported by VinylDB.
#[derive(Debug, Display, Error)]
pub enum UnimplementedFeature {
    /// Technically, this variant contains the other variants, but we should ONLY
    /// use this one in cases that are not covered by the following detailed
    /// variants.
    #[display(fmt = "Unsupported SQL statement {statement}")]
    Statement { statement: Statement },
    #[display(fmt = "Unsupported SQL datatype {ty}")]
    DataType { ty: SQLDataType },
    #[display(fmt = "Unsupported SQL binary operator {op}")]
    Operator { op: BinaryOperator },
    #[display(fmt = "Unsupported SQL Expr {expr}")]
    Expr { expr: SQLExpr },
    #[display(fmt = "Unsupported multi-level table {object_name}")]
    MultiLevelTable { object_name: ObjectName },
    #[display(fmt = "Projection only supports columns, found: {expr}")]
    ProjectionWithNonColumnExpr { expr: Expr },
    #[display(fmt = "Projection does not support alias, found: {select_item}")]
    ProjectionWithAlias { select_item: SelectItem },
    #[display(
        fmt = "Projection does not support QualifiedWildcard, found: {select_item}"
    )]
    ProjectionQualifiedWildcard { select_item: SelectItem },
    #[display(fmt = "NULL is not supported")]
    Null,
}

#[derive(Debug, Display, Error, Copy, Clone)]
pub enum ExprEvaluationError {
    #[display(
        fmt = "trying to do '{op}' on different types '{lhs}' and '{rhs}'"
    )]
    DoOpOnDiffTypes {
        lhs: DataType,
        op: Operator,
        rhs: DataType,
    },
    #[display(fmt = "Operation '{op}' cannot be done on type '{datatype}'")]
    UnsupportedTypeForOp { datatype: DataType, op: Operator },
}

/// Errors that could happen while converting an SQL AST to a [`LogicalPlan`].
///
/// # NOTE
///
/// [`CatalogError`] can also happen during the plan stage.
#[derive(Debug, Display, Error)]
pub enum PlanError {
    #[display(
        fmt = "table {table} has {expected} columns but {found} columns were supplied"
    )]
    MismatchedNumberColumns {
        table: String,
        expected: usize,
        found: usize,
    },
    #[display(
        fmt = "the data type of {column_idx}th column of table {table} should be {expected} but found {found}"
    )]
    MismatchedType {
        table: String,
        column_idx: usize,
        expected: DataType,
        found: DataType,
    },
    #[display(fmt = "could not convert {val} to {to}")]
    ConversionError { val: Value, to: DataType },
    #[display(fmt = "could not evaluate {_0}")]
    ExprEvaluationError(ExprEvaluationError),
    #[display(fmt = "limit is not a unsigned integer {}", limit)]
    NonUintLimit { limit: Data },
    #[display(fmt = "This feature has not been implemented yet: {_0}")]
    Unimplemented(UnimplementedFeature),
}

pub type PlanResult<T> = Result<T, PlanError>;

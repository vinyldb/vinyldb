use super::error::{PlanError, UnimplementedFeature};
use crate::data::types::DataType;
use sqlparser::ast::DataType as SQLDataType;

impl TryFrom<SQLDataType> for DataType {
    type Error = PlanError;
    fn try_from(value: SQLDataType) -> Result<Self, Self::Error> {
        match value {
            SQLDataType::Bool => Ok(Self::Bool),
            SQLDataType::Int64 => Ok(Self::Int64),
            SQLDataType::Float64 => Ok(Self::Float64),
            SQLDataType::Timestamp(_, _) => Ok(Self::Timestamp),
            SQLDataType::String(_) => Ok(Self::String),

            ty => Err(PlanError::Unimplemented(UnimplementedFeature::DataType {ty}))
        }
    }
}

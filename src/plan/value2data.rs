use super::error::{PlanError, PlanResult, UnimplementedFeature};
use crate::data::types::Data;
use sqlparser::ast::Value;

pub(crate) fn value_to_data(val: Value) -> PlanResult<Data> {
    match val {
        Value::Number(str, _) => {
            if let Ok(num) = str.parse::<i64>() {
                Ok(Data::Int64(num))
            } else if let Ok(num) = str.parse::<f64>() {
                Ok(Data::Float64(num))
            } else {
                // I have no idea on how can we handle this properly.
                todo!("error handling TODO")
            }
        }
        Value::SingleQuotedString(str) => Ok(Data::String(str)),
        Value::DollarQuotedString(str) => Ok(Data::String(str.value)),
        Value::EscapedStringLiteral(str) => Ok(Data::String(str)),
        Value::SingleQuotedByteStringLiteral(str) => Ok(Data::String(str)),
        Value::DoubleQuotedByteStringLiteral(str) => Ok(Data::String(str)),
        Value::RawStringLiteral(str) => Ok(Data::String(str)),
        Value::NationalStringLiteral(str) => Ok(Data::String(str)),
        Value::HexStringLiteral(str) => Ok(Data::String(str)),
        Value::DoubleQuotedString(str) => Ok(Data::String(str)),
        Value::Boolean(val) => Ok(Data::Bool(val)),
        Value::Null => {
            Err(PlanError::Unimplemented(UnimplementedFeature::Null))
        }
        Value::Placeholder(str) => Ok(Data::String(str)),
        Value::UnQuotedString(str) => Ok(Data::String(str)),
    }
}

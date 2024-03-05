use super::error::{PlanError, PlanResult, UnimplementedFeature};
use sqlparser::ast::ObjectName;

pub(crate) fn object_name_to_table_name(
    mut object_name: ObjectName,
) -> PlanResult<String> {
    if object_name.0.len() != 1 {
        return Err(PlanError::Unimplemented(
            UnimplementedFeature::MultiLevelTable { object_name },
        ));
    }

    Ok(object_name.0.pop().expect("should have 1 element").value)
}

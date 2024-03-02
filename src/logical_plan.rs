use crate::catalog::schema::Schema;

#[derive(Debug)]
pub enum LogicalPlan {
    Explain {
        plan: Box<LogicalPlan>,
    },
    CreateTable {
        name: String,
        schema: Schema,
        pk: usize,
    },
    TableScan {
        name: String,
    },
    ShowTables,
    DescribeTable {
        name: String,
    },
}

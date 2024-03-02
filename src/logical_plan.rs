use crate::{catalog::schema::Schema, data::tuple::Tuple};

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
    Insert {
        table: String,
        rows: Vec<Tuple>,
    },
}

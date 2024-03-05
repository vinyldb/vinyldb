use crate::{catalog::schema::Schema, data::tuple::Tuple, expr::Expr};

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
    Filter {
        predicate: Expr,
        input: Box<LogicalPlan>,
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
    Limit {
        // TODO: impl skip
        // skip: usize,
        fetch: usize,
        input: Box<LogicalPlan>,
    },
    Projection {
        expr: Vec<Expr>,
        schema: Schema,
        input: Box<LogicalPlan>,
    },
}

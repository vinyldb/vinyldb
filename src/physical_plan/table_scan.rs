use crate::{
    catalog::schema::Schema,
    ctx::Context,
    data::tuple::{Tuple, TupleStream},
    error::Result,
    physical_plan::Executor,
};

#[derive(Debug)]
pub struct TableScanExec {
    table: String,
    schema: Schema,
}

impl TableScanExec {
    pub fn new(table: String, schema: Schema) -> Self {
        Self { table, schema }
    }
}

impl Executor for TableScanExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self, ctx: &mut Context) -> Result<TupleStream> {
        let storage_engine = &ctx.storage;
        let tree = storage_engine.get_tree_of_table(&self.table)?;
        let mut tuples = Vec::with_capacity(tree.len());
        for res_item in tree {
            let (_, raw_tuple) = res_item?;
            let tuple = Tuple::decode(&raw_tuple, &self.schema);

            tuples.push(tuple);
        }

        Ok(Box::new(tuples.into_iter()))
    }

    fn next(&self) -> Option<&dyn Executor> {
        // should be None as TableScan should be the lowest operator.
        None
    }
}

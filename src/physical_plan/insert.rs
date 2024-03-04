use crate::{
    catalog::schema::Schema,
    ctx::Context,
    data::tuple::{Tuple, TupleStream},
    error::Result,
    physical_plan::Executor,
};

#[derive(Debug)]
pub struct InsertExec {
    table: String,
    rows: Vec<Tuple>,
}

impl InsertExec {
    pub fn new(table: String, rows: Vec<Tuple>) -> Self {
        Self { table, rows }
    }
}

impl Executor for InsertExec {
    fn schema(&self) -> Schema {
        Schema::empty()
    }

    fn execute(&self, ctx: &mut Context) -> Result<TupleStream> {
        let table_name = &self.table;
        let table_catalog = ctx.catalog.get_table(table_name)?;
        let pk = table_catalog.pk();

        let tree = ctx.storage.get_tree_of_table(&self.table)?;

        for row in self.rows.iter() {
            let pk = row.get(pk).unwrap();
            assert!(tree.insert(pk.encode(), row.encode())?.is_none());
        }

        Ok(Box::new(std::iter::empty()))
    }

    fn next(&self) -> Option<&dyn Executor> {
        None
    }
}

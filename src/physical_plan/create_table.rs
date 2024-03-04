use crate::{
    catalog::{schema::Schema, Table},
    ctx::Context,
    data::tuple::TupleStream,
    error::Result,
    physical_plan::Executor,
};

#[derive(Debug)]
pub struct CreateTableExec {
    name: String,
    schema: Schema,
    pk: usize,
}

impl CreateTableExec {
    pub fn new(name: String, schema: Schema, pk: usize) -> Self {
        Self { name, schema, pk }
    }
}

impl Executor for CreateTableExec {
    fn schema(&self) -> Schema {
        Schema::empty()
    }

    fn execute(&self, ctx: &mut Context) -> Result<TupleStream> {
        let table_name = self.name.clone();
        let table = Table::new(self.name.clone(), self.schema.clone(), self.pk);
        // check catalog first
        ctx.catalog.add_table(table)?;

        // create disk files
        ctx.storage.add_table(table_name)?;

        Ok(Box::new(std::iter::empty()))
    }

    fn next(&self) -> Option<&dyn Executor> {
        None
    }
}

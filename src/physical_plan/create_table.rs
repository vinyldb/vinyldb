use crate::{
    catalog::{catalog::Table, schema::Schema},
    ctx::Context,
    error::Result,
    physical_plan::{tuple::TupleStream, Executor},
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
        let table = Table::new(self.name.clone(), self.schema.clone(), self.pk);
        ctx.catalog.add_table(table)?;

        Ok(Box::new(std::iter::empty()))
    }

    fn next(&self) -> Option<&dyn Executor> {
        None
    }
}

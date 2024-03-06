use crate::{
    catalog::{schema::Schema, Table},
    ctx::Context,
    data::{
        tuple::{Tuple, TupleStream},
        types::Data,
    },
    error::Result,
    physical_plan::Executor,
};

#[derive(Debug)]
pub struct CreateTableExec {
    name: String,
    schema: Schema,
    pk: usize,
    sql: String,
}

impl CreateTableExec {
    pub fn new(name: String, schema: Schema, pk: usize, sql: String) -> Self {
        Self {
            name,
            schema,
            pk,
            sql,
        }
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
        ctx.storage.add_table(table_name.clone())?;

        // insert it into the `vinyl_table` table
        let tuple = Tuple::new([
            Data::String(table_name.clone()),
            Data::String(self.sql.clone()),
        ]);
        let vinyl_table_tree = ctx
            .storage
            .get_tree_of_table(crate::catalog::vinyl_table::TABLE_NAME)
            .unwrap();
        vinyl_table_tree.insert(table_name, tuple.encode())?;

        Ok(Box::new(std::iter::empty()))
    }

    fn next(&self) -> Option<&dyn Executor> {
        None
    }
}

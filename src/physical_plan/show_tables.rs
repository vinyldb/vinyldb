use crate::{
    catalog::schema::Schema,
    ctx::Context,
    data::{
        tuple::{Tuple, TupleStream},
        types::{Data, DataType},
    },
    error::Result,
    physical_plan::Executor,
};

#[derive(Debug, Copy, Clone)]
pub struct ShowTablesExec;

impl Executor for ShowTablesExec {
    fn schema(&self) -> Schema {
        Schema::new([(String::from("name"), DataType::String)]).unwrap()
    }

    fn execute(&self, ctx: &mut Context) -> Result<TupleStream> {
        let tables = ctx.catalog.tables();
        let iter = tables
            .iter()
            .map(|(name, _)| Tuple::new([Data::String(name.to_owned())]))
            .collect::<Vec<_>>();

        Ok(Box::new(iter.into_iter()))
    }

    fn next(&self) -> Option<&dyn Executor> {
        None
    }
}

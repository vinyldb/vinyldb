use crate::{
    as_variant,
    catalog::schema::Schema,
    ctx::Context,
    data::{tuple::TupleStream, types::Data},
    expr::Expr,
    physical_plan::Executor,
};
use std::ops::Deref;

#[derive(Debug)]
pub struct FilterExec {
    predicate: Expr,
    input: Box<dyn Executor>,
}

impl FilterExec {
    pub fn new(predicate: Expr, input: Box<dyn Executor>) -> FilterExec {
        FilterExec { predicate, input }
    }
}

impl Executor for FilterExec {
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn execute(&self, ctx: &mut Context) -> crate::error::Result<TupleStream> {
        let schema = self.schema();
        let predicate = self.predicate.clone();
        let stream = self.input.execute(ctx)?;

        Ok(Box::new(stream.filter(move |tuple| {
            as_variant!(
                Data::Bool,
                predicate.evaluate(&schema, tuple).expect("TODO")
            )
        })))
    }

    fn next(&self) -> Option<&dyn Executor> {
        Some(self.input.deref())
    }
}

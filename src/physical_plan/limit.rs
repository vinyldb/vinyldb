use crate::{
    catalog::schema::Schema, ctx::Context, data::tuple::TupleStream,
    physical_plan::Executor,
};
use std::ops::Deref;

#[derive(Debug)]
pub struct LimitExec {
    // skip: usize,
    fetch: usize,
    input: Box<dyn Executor>,
}

impl LimitExec {
    pub fn new(fetch: usize, input: Box<dyn Executor>) -> Self {
        Self { fetch, input }
    }
}

impl Executor for LimitExec {
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn execute(&self, ctx: &mut Context) -> crate::error::Result<TupleStream> {
        let iter = self.input.execute(ctx)?;
        Ok(Box::new(iter.into_iter().take(self.fetch)))
    }

    fn next(&self) -> Option<&dyn Executor> {
        Some(self.input.deref())
    }
}

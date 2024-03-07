use crate::{
    catalog::schema::Schema, ctx::Context, data::tuple::TupleStream,
    error::Result, physical_plan::Executor,
};
use std::{num::NonZeroUsize, ops::Deref};

#[derive(Debug)]
pub struct LimitExec {
    offset: Option<NonZeroUsize>,
    limit: Option<usize>,
    input: Box<dyn Executor>,
}

impl LimitExec {
    pub fn new(
        offset: Option<NonZeroUsize>,
        limit: Option<usize>,
        input: Box<dyn Executor>,
    ) -> Self {
        Self {
            offset,
            limit,
            input,
        }
    }
}

impl Executor for LimitExec {
    fn schema(&self) -> Schema {
        self.input.schema()
    }

    fn execute(&self, ctx: &mut Context) -> Result<TupleStream> {
        let mut iter = self.input.execute(ctx)?;
        if let Some(offset) = self.offset {
            iter = Box::new(iter.skip(offset.get()));
        }
        if let Some(limit) = self.limit {
            iter = Box::new(iter.take(limit))
        }

        Ok(iter)
    }

    fn next(&self) -> Option<&dyn Executor> {
        Some(self.input.deref())
    }
}

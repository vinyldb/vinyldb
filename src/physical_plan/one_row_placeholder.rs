use crate::{
    catalog::schema::Schema,
    ctx::Context,
    data::tuple::{Tuple, TupleStream},
    physical_plan::Executor,
};

#[derive(Debug, Copy, Clone)]
pub struct OneRowPlaceholderExec;

impl Executor for OneRowPlaceholderExec {
    fn schema(&self) -> Schema {
        Schema::empty()
    }

    fn execute(&self, _ctx: &mut Context) -> crate::error::Result<TupleStream> {
        let iter = std::iter::once(Tuple::empty());

        Ok(Box::new(iter))
    }

    fn next(&self) -> Option<&dyn Executor> {
        None
    }
}

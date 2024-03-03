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
use std::ops::Deref;

#[derive(Debug)]
pub struct ExplainExec {
    plan: Box<dyn Executor>,
}

impl ExplainExec {
    pub fn new(plan: Box<dyn Executor>) -> Self {
        Self { plan }
    }
}

impl Executor for ExplainExec {
    fn schema(&self) -> Schema {
        Schema::new([(String::from("Physical Plan"), DataType::String)])
            .expect("should never fail")
    }

    fn execute(&self, _ctx: &mut Context) -> Result<TupleStream> {
        let mut p = self.plan.deref();
        let mut execs = vec![p.name().to_string()];
        while let Some(next) = p.next() {
            execs.push(next.name().to_string());

            p = next;
        }

        Ok(Box::new(
            execs
                .into_iter()
                .map(|name| Tuple::new([Data::String(name)])),
        ))
    }

    fn next(&self) -> Option<&dyn Executor> {
        Some(self.plan.deref())
    }
}

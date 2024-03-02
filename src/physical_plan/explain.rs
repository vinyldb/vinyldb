use crate::{
    catalog::schema::Schema,
    ctx::Context,
    data_types::{Data, DataType},
    error::Result,
    physical_plan::{
        tuple::{Tuple, TupleStream},
        Executor,
    },
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
        const HEADER: &str = "┌─────────────────────────────┐\n│┌───────────────────────────┐│\n││       Physical Plan       ││\n│└───────────────────────────┘│\n└─────────────────────────────┘";
        fn wrap_in_box(name: &str) -> String {
            let line1 = "┌────────────────────────────┐";
            let line3 = "└────────────────────────────┘";

            format!("{line1}\n│{:^28}│\n{line3}", name)
        }

        let mut p = self.plan.deref();
        let mut execs = vec![HEADER.to_string(), wrap_in_box(p.name())];
        while let Some(next) = p.next() {
            execs.push(wrap_in_box(next.name()));

            p = next;
        }

        Ok(Box::new(
            execs
                .into_iter()
                .map(|name| Tuple(vec![Data::String(name)])),
        ))
    }

    fn next(&self) -> Option<&dyn Executor> {
        Some(self.plan.deref())
    }
}

use crate::{
    catalog::schema::Schema,
    ctx::Context,
    data::tuple::{Tuple, TupleStream},
    error::Result,
    expr::Expr,
    physical_plan::Executor,
};
use std::ops::Deref;

#[derive(Debug)]
pub struct ProjectionExec {
    expr: Vec<Expr>,
    schema: Schema,
    input: Box<dyn Executor>,
}

impl ProjectionExec {
    pub fn new(
        expr: Vec<Expr>,
        schema: Schema,
        input: Box<dyn Executor>,
    ) -> Self {
        Self {
            expr,
            schema,
            input,
        }
    }
}

impl Executor for ProjectionExec {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn execute(&self, ctx: &mut Context) -> Result<TupleStream> {
        let input_schema = self.input.schema();
        let stream = self.input.execute(ctx)?;
        let exprs = self.expr.clone();
        let projected_stream = stream.map(move |tuple| {
            Tuple::new(
                Expr::evaluate_batch(&exprs, &input_schema, &tuple)
                    .expect("TODO"),
            )
        });

        Ok(Box::new(projected_stream))
    }

    fn next(&self) -> Option<&dyn Executor> {
        Some(self.input.deref())
    }
}

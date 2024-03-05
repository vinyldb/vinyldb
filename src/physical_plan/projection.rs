use crate::{
    as_variant, catalog::schema::Schema, ctx::Context,
    data::tuple::TupleStream, error::Result, expr::Expr,
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
        let proj = self
            .expr
            .iter()
            // use as_variant because we only allow columns in projection now
            .map(|expr| as_variant!(Expr::Column, expr))
            .map(|col_name| input_schema.index_of_column(col_name).unwrap())
            .collect::<Vec<usize>>();

        let stream = self.input.execute(ctx)?;
        Ok(Box::new(
            stream.into_iter().map(move |tuple| tuple.project(&proj)),
        ))
    }

    fn next(&self) -> Option<&dyn Executor> {
        Some(self.input.deref())
    }
}

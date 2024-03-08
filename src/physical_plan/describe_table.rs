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

#[derive(Debug)]
pub struct DescribeTableExec {
    name: String,
}

impl DescribeTableExec {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl Executor for DescribeTableExec {
    fn schema(&self) -> Schema {
        let column_name = (String::from("column_name"), DataType::String);
        let column_type = (String::from("column_type"), DataType::String);
        let null = (String::from("null"), DataType::String);
        let key = (String::from("key"), DataType::String);

        Schema::new_with_duplicate_check([column_name, column_type, null, key])
            .unwrap()
    }

    fn execute(&self, ctx: &mut Context) -> Result<TupleStream> {
        let table = ctx.catalog.get_table(&self.name)?;
        let schema = table.schema();
        let mut ret = Vec::with_capacity(table.n_columns());
        for (idx, (name, datatype)) in schema.columns().enumerate() {
            let column_name = Data::String(name.to_string());
            let column_type = Data::String(datatype.to_string());
            let null = Data::String(String::from("YES"));
            let key = if idx == table.pk() { "YES" } else { "NO" };
            let key = Data::String(key.to_string());

            ret.push(Tuple::new([column_name, column_type, null, key]));
        }

        Ok(Box::new(ret.into_iter()))
    }

    fn next(&self) -> Option<&dyn Executor> {
        None
    }
}

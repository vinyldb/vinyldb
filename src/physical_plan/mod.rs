pub mod create_table;
pub mod describe_table;
pub mod explain;
pub mod filter;
pub mod insert;
pub mod limit;
pub mod show_tables;
pub mod table_scan;

use crate::{
    catalog::schema::Schema, ctx::Context, data::tuple::TupleStream,
    error::Result,
};
use std::fmt::Debug;

/// A node in a physical plan.
pub trait Executor: Debug {
    fn schema(&self) -> Schema;
    fn execute(&self, ctx: &mut Context) -> Result<TupleStream>;
    fn next(&self) -> Option<&dyn Executor>;

    fn name(&self) -> &str {
        let full_name = std::any::type_name::<Self>();
        let start_idx = full_name.rfind(':').unwrap() + 1;

        &full_name[start_idx..]
    }
}

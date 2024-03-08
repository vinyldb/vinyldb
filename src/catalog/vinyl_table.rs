//! Table `vinyl_table` is used to store the catalog of VinylDB.

use super::schema::Schema;
use crate::data::types::DataType;
use std::sync::LazyLock;

pub const TABLE_NAME: &str = "vinyl_table";
pub const PK: usize = 0;
pub static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    Schema::new_with_duplicate_check([
        ("name".into(), DataType::String),
        ("sql".into(), DataType::String),
    ])
    .unwrap()
});

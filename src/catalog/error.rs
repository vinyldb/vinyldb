use derive_more::{Display, Error};

#[derive(Error, Display, Debug)]
pub enum CatalogError {
    #[display(fmt = "Table with name '{}' already exists", name)]
    TableExists { name: String },
    #[display(fmt = "Table with name '{}' does not exist", name)]
    TableDoesNotExist { name: String },
    #[display(fmt = "Column with name '{}' already exists", name)]
    ColumnExists { name: String },
}

pub type CatalogResult<T> = Result<T, CatalogError>;

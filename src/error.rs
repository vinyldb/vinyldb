use crate::catalog::error::CatalogError;
use derive_more::{Display, Error, From};
use rustyline::error::ReadlineError;
use sqlparser::parser::ParserError;

#[derive(Debug, Display, Error, From)]
pub enum Error {
    SqlParserError(ParserError),
    ReplError(ReadlineError),
    CatalogError(CatalogError),
    #[display(fmt = "This feature has not been implemented yet")]
    NotImplemented,
}

pub type Result<T> = std::result::Result<T, Error>;

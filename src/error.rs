use crate::{catalog::error::CatalogError, plan::error::PlanError};
use derive_more::{Display, Error, From};
use rustyline::error::ReadlineError;
use sled::Error as SledError;
use sqlparser::parser::ParserError;
use std::io::Error as IoError;

#[derive(Debug, Display, Error, From)]
pub enum Error {
    SqlParserError(ParserError),
    ReplError(ReadlineError),
    PlanError(PlanError),
    CatalogError(CatalogError),
    SledError(SledError),
    IoError(IoError),
    #[display(fmt = "This feature has not been implemented yet")]
    NotImplemented,
}

pub type Result<T> = std::result::Result<T, Error>;

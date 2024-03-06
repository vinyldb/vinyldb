use crate::{
    catalog::error::CatalogError, meta_cmd::MetaCmdError,
    plan::error::PlanError,
};
use derive_more::{Display, Error, From};
use sled::Error as SledError;
use sqlparser::parser::ParserError;
use std::io::Error as IoError;

#[derive(Debug, Display, Error, From)]
pub enum Error {
    SqlParserError(ParserError),
    PlanError(PlanError),
    CatalogError(CatalogError),
    MetaCmdError(MetaCmdError),
    SledError(SledError),
    IoError(IoError),
}

pub type Result<T> = std::result::Result<T, Error>;

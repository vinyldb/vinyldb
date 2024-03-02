use derive_more::{Display, Error};
use rustyline::error::ReadlineError;
use sqlparser::parser::ParserError;

#[derive(Debug, Display, Error)]
pub enum Error {
    SqlParserError(ParserError),
    ReplError(ReadlineError),
}

pub type Result<T> = std::result::Result<T, Error>;

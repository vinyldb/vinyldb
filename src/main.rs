mod error;

use crate::error::{Error, Result};
use colored::Colorize;
use rustyline::{
    config::{BellStyle, Builder},
    error::ReadlineError,
    history::DefaultHistory,
    ColorMode, EditMode, Editor,
};
use sqlparser::{dialect::GenericDialect, parser::Parser};

const DIALECT: GenericDialect = GenericDialect {};

fn run_repl(repl: &mut Editor<(), DefaultHistory>) -> Result<()> {
    let line = repl.readline("V ").map_err(Error::ReplError)?;
    let ast = Parser::parse_sql(&DIALECT, &line)
        .map(|mut asts| asts.pop().unwrap())
        .map_err(Error::SqlParserError)?;

    println!("SQL: {:?}\n", ast);

    Ok(())
}

fn main() {
    let repl_cfg = Builder::new()
        .auto_add_history(true)
        .edit_mode(EditMode::Emacs)
        .bell_style(BellStyle::Visible)
        .color_mode(ColorMode::Enabled)
        .build();
    let mut repl: Editor<(), DefaultHistory> =
        Editor::with_config(repl_cfg).unwrap();

    println!("{} {}", "VinylDB".yellow(), env!("CARGO_PKG_VERSION"));
    loop {
        if let Err(e) = run_repl(&mut repl) {
            match e {
                Error::ReplError(ReadlineError::Eof) => std::process::exit(0),

                e => eprintln!("Error: {:?}", e),
            }
        }
    }
}

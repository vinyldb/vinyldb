#![deny(unused_imports)]

extern crate core;

mod catalog;
mod config;
mod ctx;
mod data;
mod error;
mod logical_plan;
mod meta_cmd;
mod physical_plan;
mod plan;
mod storage_engine;
#[macro_use]
mod utils;

use crate::{
    ctx::Context,
    error::{Error, Result},
    meta_cmd::MetaCmd,
};
use colored::Colorize;
use rustyline::{
    config::{BellStyle, Builder},
    error::ReadlineError,
    history::DefaultHistory,
    ColorMode, EditMode, Editor,
};
use std::{ops::Deref, time::SystemTime};

fn run_repl(
    repl: &mut Editor<(), DefaultHistory>,
    ctx: &mut Context,
) -> Result<()> {
    let line = repl
        .readline(&format!("{}", "V ".green()))
        .map_err(Error::ReplError)?;
    if line.is_empty() {
        return Ok(());
    }

    // this is a meta command
    if line.starts_with('.') {
        let meta_cmd = line.parse::<MetaCmd>()?;
        meta_cmd.execute(ctx)?;

        return Ok(());
    }

    let now = SystemTime::now();
    let statement = ctx.sql_to_statement(line)?;

    if ctx.config.show_ast {
        println!("Ast: {:?}", statement);
    }

    let logical_plan = ctx.statement_to_logical_plan(&statement)?;
    let physical_plan = ctx.create_physical_plan(&logical_plan)?;

    let iter = ctx.execute(physical_plan.deref())?;
    for tuple in iter {
        println!("{}", tuple);
    }
    println!();

    if ctx.config.timer {
        println!("Took {:?}", now.elapsed().unwrap());
    }

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
    let mut ctx = Context::new().expect("failed to create a context");

    println!("{} {}", "VinylDB".yellow(), env!("CARGO_PKG_VERSION"));
    loop {
        if let Err(e) = run_repl(&mut repl, &mut ctx) {
            match e {
                Error::ReplError(ReadlineError::Eof) => std::process::exit(0),
                Error::ReplError(ReadlineError::Interrupted) => continue,

                e => eprintln!("Error: {}", e),
            }
        }
    }
}

#![deny(unused_imports)]

mod catalog;
mod ctx;
mod data;
mod error;
mod logical_plan;
mod physical_plan;
mod plan;
mod storage_engine;
#[macro_use]
mod utils;

use crate::{
    ctx::Context,
    error::{Error, Result},
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

    let now = SystemTime::now();
    let logical_plan = ctx.create_logical_plan(line)?;
    println!("DBG: {:?}", logical_plan);
    let physical_plan = ctx.create_physical_plan(&logical_plan)?;
    println!("DBG: {:?}", physical_plan);

    let iter = ctx.execute(physical_plan.deref())?;
    for tuple in iter {
        println!("{}", tuple);
    }
    println!();

    println!("Took {:?}", now.elapsed().unwrap());

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

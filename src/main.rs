use colored::Colorize;
use rustyline::{
    config::{BellStyle, Builder},
    error::ReadlineError,
    history::DefaultHistory,
    ColorMode, EditMode, Editor,
};
use std::{ops::Deref, path::Path, time::SystemTime};
use vinyldb::{
    ctx::Context,
    error::{Error, Result},
    meta_cmd::MetaCmd,
};

fn run_repl(
    repl: &mut Editor<(), DefaultHistory>,
    ctx: &mut Context,
    succeed: bool,
) -> Result<()> {
    let prompt = if succeed { "V ".green() } else { "V ".red() };
    let line = repl
        .readline(&prompt.to_string())
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

    let logical_plan = ctx.statement_to_logical_plan(statement)?;
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
    let history = Path::new(".vinyldb_history");
    let repl_cfg = Builder::new()
        .auto_add_history(true)
        .edit_mode(EditMode::Emacs)
        .bell_style(BellStyle::Visible)
        .color_mode(ColorMode::Enabled)
        .build();
    let mut repl: Editor<(), DefaultHistory> =
        Editor::with_config(repl_cfg).unwrap();

    if history.exists() {
        repl.load_history(history).unwrap();
    }

    let mut ctx = Context::new("data").expect("failed to create a context");

    println!("{} {}", "VinylDB".yellow(), env!("CARGO_PKG_VERSION"));
    let mut success = true;
    loop {
        if let Err(e) = run_repl(&mut repl, &mut ctx, success) {
            match e {
                Error::ReplError(ReadlineError::Eof) => {
                    repl.save_history(history).unwrap();
                    std::process::exit(0);
                }
                Error::ReplError(ReadlineError::Interrupted) => continue,

                e => {
                    success = false;
                    eprintln!("Error: {}", e);
                }
            }
        } else {
            success = true;
        }
    }
}

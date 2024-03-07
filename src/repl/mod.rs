pub mod prompt;

use crate::{ctx::Context, error::Result, meta_cmd::MetaCmd};
use colored::Colorize;
use prompt::VinylPrompt;
use reedline_sql_highlighter::SQLKeywordHighlighter;
use std::{ops::Deref, path::PathBuf, time::SystemTime};

use reedline::{
    CursorConfig, DefaultHinter, Emacs, FileBackedHistory, Reedline, Signal,
};

pub fn run_repl(
    repl: &mut Reedline,
    ctx: &mut Context,
    success: bool,
) -> Result<()> {
    let prompt = VinylPrompt::new(success);
    let line = repl.read_line(&prompt)?;
    let line = match line {
        Signal::Success(line) => line,
        Signal::CtrlC => return Ok(()),
        Signal::CtrlD => {
            repl.sync_history()?;
            std::process::exit(0)
        }
    };

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

pub fn start() {
    let history =
        FileBackedHistory::with_file(5000, PathBuf::from(".vinyldb_history"))
            .unwrap();
    let cursor_cfg = CursorConfig {
        emacs: Some(crossterm::cursor::SetCursorStyle::SteadyBlock),
        ..CursorConfig::default()
    };
    let mut repl = Reedline::create()
        .with_history(Box::new(history))
        .with_highlighter(Box::new(SQLKeywordHighlighter::new()))
        .with_hinter(Box::new(DefaultHinter::default()))
        .with_edit_mode(Box::<Emacs>::default())
        .with_cursor_config(cursor_cfg)
        .with_ansi_colors(true);

    // repl.sync_history().unwrap();

    let mut ctx = Context::new("data").expect("failed to create a context");

    println!("{} {}", "VinylDB".yellow(), env!("CARGO_PKG_VERSION"));

    let mut success = true;
    loop {
        if let Err(e) = run_repl(&mut repl, &mut ctx, success) {
            success = false;
            eprintln!("Error: {}", e);
        } else {
            success = true;
        }
    }
}

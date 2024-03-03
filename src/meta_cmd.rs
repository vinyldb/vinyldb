//! Meta commands are commands starting with a dot.

use crate::{ctx::Context, error::Result};
use derive_more::{Display, Error};
use std::{fmt::write, str::FromStr};
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

#[derive(Debug, Display, Error)]
pub enum MetaCmdError {
    #[display(
        fmt = "unknown command or invalid arguments:  {cmd}. Enter \".help\" for help"
    )]
    UnknownCommand { cmd: String },
    #[display(fmt = "Invalid usage, usage: {correct_usage}")]
    InvalidUsage { correct_usage: &'static str },
}

#[derive(Copy, Clone, Debug, EnumIter)]
#[strum(serialize_all = "lowercase")]
pub enum MetaCmd {
    Help,

    /// If enabled, the AST of the following parsed SQLs will be printed out.
    Ast(bool),
    Timer(bool),
}

impl FromStr for MetaCmd {
    type Err = MetaCmdError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let mut tokens = s.split_whitespace();
        let cmd = tokens.next().expect("expect at least 1 token");

        if cmd == Self::HELP {
            return Ok(Self::Help);
        }

        if cmd == Self::AST {
            let val_str =
                tokens.next().ok_or_else(|| MetaCmdError::InvalidUsage {
                    correct_usage: Self::AST_USAGE,
                })?;
            let bool = match val_str {
                "on" => Ok(true),
                "off" => Ok(false),
                _ => Err(MetaCmdError::InvalidUsage {
                    correct_usage: Self::TIMER_USAGE,
                }),
            }?;

            return Ok(MetaCmd::Ast(bool));
        }

        if cmd == Self::TIMER {
            let val_str =
                tokens.next().ok_or_else(|| MetaCmdError::InvalidUsage {
                    correct_usage: Self::TIMER_USAGE,
                })?;
            let bool = match val_str {
                "on" => Ok(true),
                "off" => Ok(false),
                _ => Err(MetaCmdError::InvalidUsage {
                    correct_usage: Self::TIMER_USAGE,
                }),
            }?;

            return Ok(MetaCmd::Timer(bool));
        }

        Err(MetaCmdError::UnknownCommand {
            cmd: cmd.to_string(),
        })
    }
}

impl MetaCmd {
    const HELP: &'static str = ".help";
    const AST: &'static str = ".ast";
    const TIMER: &'static str = ".timer";

    const HELP_USAGE: &'static str = ".help";
    const AST_USAGE: &'static str = ".ast on | off";
    const TIMER_USAGE: &'static str = ".timer on | off";

    /// Return the output of `.help`.
    pub fn help_str(&self) -> String {
        let mut ret = String::new();
        for cmd in MetaCmd::iter() {
            write(
                &mut ret,
                format_args!("{:30} {}\n", cmd.usage(), cmd.desc()),
            )
            .unwrap();
        }

        ret
    }

    /// The usage string.
    pub fn usage(&self) -> &'static str {
        match self {
            MetaCmd::Help => Self::HELP_USAGE,
            MetaCmd::Ast(_) => Self::AST_USAGE,
            MetaCmd::Timer(_) => Self::TIMER_USAGE,
        }
    }

    /// The description string.
    pub fn desc(&self) -> &'static str {
        match self {
            MetaCmd::Help => "Show help text",
            MetaCmd::Ast(_) => "Print AST for each SQL if enabled",
            MetaCmd::Timer(_) => "Turn SQL timer on or off",
        }
    }

    /// Execute this [`MetCmd`].
    pub fn execute(&self, ctx: &mut Context) -> Result<()> {
        match self {
            MetaCmd::Help => {
                println!("{}", self.help_str());
            }
            MetaCmd::Ast(val) => ctx.config.show_ast = *val,
            MetaCmd::Timer(val) => ctx.config.timer = *val,
        }

        Ok(())
    }
}

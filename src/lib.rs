#![deny(unused_imports)]
#![deny(missing_debug_implementations)]
#![deny(missing_copy_implementations)]
#![allow(clippy::unnecessary_lazy_evaluations)]
#![deny(clippy::undocumented_unsafe_blocks)]
#![feature(box_into_inner)]
#![feature(lazy_cell)]

pub mod catalog;
pub mod config;
pub mod ctx;
pub mod data;
pub mod error;
pub mod expr;
pub mod logical_plan;
pub mod meta_cmd;
pub mod physical_plan;
pub mod plan;
pub mod repl;
pub mod storage_engine;
#[macro_use]
pub mod utils;

mod sqllogictest;

use crate::{ctx::Context, data::tuple::Tuple, error::Result};
use camino::Utf8Path;
use derive_more::{Deref, DerefMut};
use std::ops::Deref;

/// A VinylDB instance.
//
// We simply wrap the [`Context`] type here.
#[derive(Deref, DerefMut, Debug)]
pub struct VinylDB(Context);

impl VinylDB {
    /// Create a new instance.
    pub fn new<P: AsRef<Utf8Path>>(data_path: P) -> VinylDB {
        Self(Context::new(data_path).unwrap())
    }

    /// Execute SQL and return the result.
    pub fn sql<S: AsRef<str>>(&mut self, sql: S) -> Result<Vec<Tuple>> {
        let logical_plan = self.create_logical_plan(sql)?;
        let physical_plan = self.create_physical_plan(&logical_plan)?;
        let result = self.collect(physical_plan.deref())?;

        Ok(result)
    }
}

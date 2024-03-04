#![deny(unused_imports)]
#![deny(missing_debug_implementations)]
#![deny(missing_copy_implementations)]
#![allow(clippy::unnecessary_lazy_evaluations)]

pub mod catalog;
pub mod config;
pub mod ctx;
pub mod data;
pub mod error;
pub mod logical_plan;
pub mod meta_cmd;
pub mod physical_plan;
pub mod plan;
pub mod storage_engine;
#[macro_use]
pub mod utils;

mod sqllogictest;

use crate::ctx::Context;
use derive_more::{Deref, DerefMut};

/// A VinylDB instance.
//
// We simply wrap the [`Context`] type here.
#[derive(Deref, DerefMut, Debug)]
pub struct VinylDB(Context);

impl Default for VinylDB {
    fn default() -> Self {
        let ctx = Context::new().expect("failed to create a context");

        VinylDB(ctx)
    }
}

impl VinylDB {
    /// Create a new instance.
    pub fn new() -> VinylDB {
        Self::default()
    }
}

//! VinylDB configuration

use camino::Utf8PathBuf;
use derive_builder::Builder;

#[derive(Debug, Clone, Builder, Default)]
pub struct Config {
    pub show_ast: bool,
    pub timer: bool,
    pub data_path: Utf8PathBuf,
}

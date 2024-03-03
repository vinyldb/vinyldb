//! VinylDB configuration

#[derive(Debug, Default)]
pub struct Config {
    pub show_ast: bool,
    pub timer: bool,
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }
}

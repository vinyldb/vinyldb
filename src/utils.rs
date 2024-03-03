use camino::Utf8PathBuf;

/// The directory where VinylDB stores it data.
pub fn data_dir() -> Utf8PathBuf {
    Utf8PathBuf::from("data")
}

/// The directory where Sled stores it data.
pub fn sled_dir() -> Utf8PathBuf {
    let mut data = data_dir();
    data.push("sled");

    data
}

/// A helper macro to convert an enum to its variant.
#[macro_export]
macro_rules! as_variant {
    ($variant:path, $val:expr) => {
        match $val {
            $variant(t) => t,
            _ => panic!("expecting {}", stringify!($variant)),
        }
    };
}

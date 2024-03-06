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

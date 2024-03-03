#[derive(Debug, Clone)]
pub struct Column {
    name: String,
    is_pk: bool,
    null_able: bool,
}

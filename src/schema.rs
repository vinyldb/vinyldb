use crate::data_types::DataType;
use indexmap::{map::Entry, IndexMap};

/// Describes the metadata of an ordered sequence of relative types.
#[derive(Debug, Clone)]
pub struct Schema {
    fields: IndexMap<String, DataType>,
}

impl Schema {
    /// Create a new [`Schema`].
    pub fn new(fields: impl IntoIterator<Item = (String, DataType)>) -> Self {
        let mut ret = IndexMap::new();
        for (name, ty) in fields {
            match ret.entry(name) {
                Entry::Vacant(v) => {
                    v.insert(ty);
                }
                Entry::Occupied(old) => {
                    panic!(
                        "duplicate field '{}' with type '{}'",
                        old.key(),
                        old.get()
                    )
                }
            }
        }

        Self { fields: ret }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn schema_new() {
        let fields = vec![
            (String::from("name"), DataType::String),
            (String::from("age"), DataType::Int64),
        ];
        let schema = Schema::new(fields.clone());

        assert_eq!(
            schema.fields,
            fields.into_iter().collect::<IndexMap<_, _>>()
        );
    }

    #[test]
    #[should_panic(expected = "duplicate field 'name' with type 'String'")]
    fn schema_new_duplicate_fields() {
        let fields = vec![
            (String::from("name"), DataType::String),
            (String::from("name"), DataType::Int64),
        ];
        let schema = Schema::new(fields);
    }
}

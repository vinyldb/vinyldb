use super::error::{CatalogError, CatalogResult};
use crate::data_types::DataType;
use indexmap::{map::Entry, IndexMap};

/// Describes the metadata of an ordered sequence of relative types.
#[derive(Debug, Clone)]
pub struct Schema {
    fields: IndexMap<String, DataType>,
}

impl Schema {
    /// Create a new [`Schema`].
    pub fn new(
        fields: impl IntoIterator<Item = (String, DataType)>,
    ) -> CatalogResult<Self> {
        let mut ret = IndexMap::new();
        for (name, ty) in fields {
            match ret.entry(name.clone()) {
                Entry::Vacant(v) => {
                    v.insert(ty);
                }
                Entry::Occupied(_) => {
                    return Err(CatalogError::ColumnExists { name })
                }
            }
        }

        Ok(Self { fields: ret })
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
        let schema = Schema::new(fields.clone()).unwrap();

        assert_eq!(
            schema.fields,
            fields.into_iter().collect::<IndexMap<_, _>>()
        );
    }

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: ColumnExists { name: \"name\" }"
    )]
    fn schema_new_duplicate_fields() {
        let fields = vec![
            (String::from("name"), DataType::String),
            (String::from("name"), DataType::Int64),
        ];
        let _schema = Schema::new(fields).unwrap();
    }
}

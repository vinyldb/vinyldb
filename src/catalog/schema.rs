use super::error::{CatalogError, CatalogResult};
use crate::data::types::DataType;
use indexmap::{map::Entry, IndexMap};

/// Describes the metadata of an ordered sequence of relative types.
#[derive(Debug, Clone)]
pub struct Schema {
    columns: IndexMap<String, DataType>,
}

impl Schema {
    /// Create a new [`Schema`].
    pub fn new(fields: impl IntoIterator<Item = (String, DataType)>) -> Self {
        Schema {
            columns: fields.into_iter().collect(),
        }
    }

    /// Create a new [`Schema`], with duplicate columns check enabled.
    pub fn new_with_duplicate_check(
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

        Ok(Self { columns: ret })
    }

    pub fn empty() -> Self {
        Self {
            columns: IndexMap::new(),
        }
    }

    pub fn column_names(&self) -> impl Iterator<Item = &str> {
        self.columns.keys().map(|str| str as &str)
    }

    pub fn column_datatypes(&self) -> impl Iterator<Item = &DataType> {
        self.columns.values()
    }

    pub fn columns(&self) -> impl Iterator<Item = (&String, &DataType)> {
        self.columns.iter()
    }

    pub fn column_datatype(&self, name: &str) -> CatalogResult<&DataType> {
        self.columns
            .get(name)
            .ok_or_else(|| CatalogError::ColumnDoesNotExist {
                column: name.to_string(),
                candidate: self
                    .columns
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect(),
            })
    }

    pub fn n_columns(&self) -> usize {
        self.columns.len()
    }

    /// Get the index of the column specified by `name`, an error will be returned
    /// if not found.
    pub fn index_of_column(&self, name: &str) -> CatalogResult<usize> {
        self.columns.get_index_of(name).ok_or_else(|| {
            CatalogError::TableDoesNotExist {
                name: name.to_string(),
            }
        })
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
        let schema = Schema::new_with_duplicate_check(fields.clone()).unwrap();

        assert_eq!(
            schema.columns,
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
        let _schema = Schema::new_with_duplicate_check(fields).unwrap();
    }
}

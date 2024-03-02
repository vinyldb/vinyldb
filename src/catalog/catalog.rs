//! VinylDB catalogs

use super::{
    error::{CatalogError, CatalogResult},
    schema::Schema,
};
use std::collections::hash_map::{Entry, HashMap};

/// A VinylDB table.
#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    /// The schema of this table.
    schema: Schema,
    /// Index of the column that is the primary key.
    pk: usize,
}

impl Table {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn pk(&self) -> usize {
        self.pk
    }

    pub fn n_columns(&self) -> usize {
        self.schema.n_columns()
    }
}

impl Table {
    /// Create a new [`Table`].
    pub fn new(name: String, schema: Schema, pk: usize) -> Self {
        Self { name, schema, pk }
    }
}

/// VinylDB catalog
#[derive(Debug)]
pub struct Catalog {
    tables: HashMap<String, Table>,
}

impl Catalog {
    /// Create a catalog
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, table: Table) -> CatalogResult<()> {
        match self.tables.entry(table.name.clone()) {
            Entry::Vacant(v) => v.insert(table),
            Entry::Occupied(_) => {
                return Err(CatalogError::TableExists { name: table.name })
            }
        };

        Ok(())
    }

    pub fn tables(&self) -> &HashMap<String, Table> {
        &self.tables
    }

    pub fn get_table(&self, name: &str) -> CatalogResult<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| CatalogError::TableDoesNotExist {
                name: name.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::types::DataType;

    #[test]
    #[should_panic(
        expected = "called `Result::unwrap()` on an `Err` value: TableExists { name: \"take\" }"
    )]
    fn duplicate_table() {
        let mut catalog = Catalog::new();
        let table = Table::new(
            String::from("take"),
            Schema::new([(String::from("name"), DataType::String)]).unwrap(),
            0,
        );
        catalog.add_table(table.clone()).unwrap();

        catalog.add_table(table).unwrap();
    }
}

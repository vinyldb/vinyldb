//! VinylDB catalogs

pub mod column;
pub mod error;
pub mod schema;
pub mod vinyl_table;

use crate::{
    as_variant,
    data::{tuple::Tuple, types::Data},
    error::Result,
    plan::create_table::create_table_to_name_schema,
    storage_engine::StorageEngine,
};
use error::{CatalogError, CatalogResult};
use indexmap::map::{Entry, IndexMap};
use schema::Schema;
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

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
#[derive(Debug, Default)]
pub struct Catalog {
    tables: IndexMap<String, Table>,
}

impl Catalog {
    /// Create a catalog
    pub fn new(storage: &StorageEngine) -> Result<Self> {
        let tree = storage.get_tree_of_table(vinyl_table::TABLE_NAME).unwrap();
        let vinyl_table = Table::new(
            vinyl_table::TABLE_NAME.to_string(),
            vinyl_table::SCHEMA.clone(),
            0,
        );
        let mut tables = IndexMap::new();
        tables.insert(vinyl_table::TABLE_NAME.into(), vinyl_table);
        for res_tuple in tree.iter().values() {
            let tuple = Tuple::decode(res_tuple?, &vinyl_table::SCHEMA);
            let sql = as_variant!(Data::String, tuple.get(1).unwrap());
            let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)?;
            assert_eq!(statements.len(), 1);
            let statement = statements.pop().unwrap();
            // SAFETY:
            // The passed statement is  guaranteed to be a `Statement::CreateTable`
            let (name, schema) =
                unsafe { create_table_to_name_schema(statement)? };
            let table = Table::new(name.clone(), schema, 0);

            tables.insert(name, table);
        }

        Ok(Self { tables })
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

    pub fn tables(&self) -> &IndexMap<String, Table> {
        &self.tables
    }

    pub fn get_table(&self, name: &str) -> CatalogResult<&Table> {
        self.tables
            .get(name)
            .ok_or_else(|| CatalogError::TableDoesNotExist {
                name: name.to_string(),
            })
    }

    pub fn contains_table(&self, name: &str) -> bool {
        self.get_table(name).is_ok()
    }
}

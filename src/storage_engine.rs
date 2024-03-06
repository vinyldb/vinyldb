use crate::{catalog::error::CatalogError, error::Result, utils::sled_dir};
use sled::{Db, Tree};
use std::collections::{hash_map::Entry, HashMap};

#[derive(Debug)]
pub struct StorageEngine {
    db: Db,
    trees: HashMap<String, Tree>,
}

impl StorageEngine {
    /// Create a new [`StorageEngine`].
    ///
    /// State will be automatically restored according to the disk files.
    pub fn new() -> Result<Self> {
        let sled_dir = sled_dir();
        std::fs::create_dir_all(sled_dir.as_path())?;
        let db = sled::open(sled_dir.as_path())?;
        db.open_tree(crate::catalog::vinyl_table::TABLE_NAME)?;

        let mut trees = HashMap::new();
        for tree_name in db.tree_names() {
            let name = String::from_utf8(tree_name.to_vec())
                .expect("should be UTF-8 encoded");
            let tree = db.open_tree(&tree_name)?;
            assert!(trees.insert(name, tree).is_none());
        }

        Ok(Self { db, trees })
    }

    pub fn add_table(&mut self, name: String) -> Result<()> {
        match self.trees.entry(name.clone()) {
            Entry::Vacant(v) => {
                let tree = self.db.open_tree(name)?;
                v.insert(tree);
            }
            Entry::Occupied(_) => unreachable!(
                "should be unreachable as we will check catalog first"
            ),
        }

        Ok(())
    }

    pub fn get_tree_of_table(&self, name: &str) -> Result<&Tree> {
        let tree = self.trees.get(name).ok_or_else(|| {
            CatalogError::TableDoesNotExist {
                name: name.to_string(),
            }
        })?;

        Ok(tree)
    }
}

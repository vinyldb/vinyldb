use super::page::Page;
use camino::Utf8Path;
use std::{collections::HashMap, ops::Deref};
use tokio::{fs::File, io::AsyncFileExt};

struct Table {
    file: File,
    n_tuples: usize,
    pages: HashMap<usize, Page>,
}

impl Table {
    pub async fn new<P: AsRef<Utf8Path>>(path: P) -> Self {
        Self {
            file: File::create(path.as_ref()).await.expect("TODO"),
            n_tuples: 0,
            pages: HashMap::new(),
        }
    }

    pub async fn open<P: AsRef<Utf8Path>>(_path: P) -> Self {
        unimplemented!()
    }

    pub async fn commit(&mut self) -> tokio::io::Result<()> {
        for (idx, page) in
            self.pages.iter_mut().filter(|(_, page)| page.is_dirty())
        {
            self.file
                .write_at(
                    page.deref(),
                    u64::try_from(idx * Page::SIZE)
                        .expect("should not fail on a 64-bit machine"),
                )
                .await?;
            page.clear_dirty_sign();
        }

        Ok(())
    }
}

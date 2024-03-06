use camino_tempfile::Utf8TempDir;
use vinyldb::VinylDB;

fn run(test_file: &str) {
    let temp_dir = Utf8TempDir::new().unwrap();
    let data_path = temp_dir.path();
    let mut tester = sqllogictest::Runner::new(|| async {
        let db = VinylDB::new(data_path);
        Ok(db)
    });
    let script = std::fs::read_to_string(test_file).unwrap();
    tester.run_script(&script).unwrap();
}

#[test]
fn explain() {
    run("tests/explain.slt");
}

#[test]
fn show_tables() {
    run("tests/show_tables.slt");
}

#[test]
fn describe_table() {
    run("tests/describe_table.slt");
}

#[test]
fn create_table() {
    run("tests/create_table.slt");
}

#[test]
fn insert() {
    run("tests/insert.slt");
}

#[test]
fn query() {
    run("tests/query.slt");
}

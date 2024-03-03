use vinyldb::VinylDB;

fn run(test_file: &str) {
    let mut tester = sqllogictest::Runner::new(|| async {
        let db = VinylDB::new();
        Ok(db)
    });
    let script = std::fs::read_to_string(test_file).unwrap();
    tester.run_script(&script).unwrap();
}

#[test]
fn sqllogictest() {
    run("tests/script.slt");
}

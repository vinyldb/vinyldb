check:
    cargo fmt --all -- --check
    cargo clippy
    cargo test

clean:
    rm -rf data

fmt:
    cargo fmt --all

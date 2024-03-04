build:
    cargo build

check:
    cargo fmt --all -- --check
    cargo clippy
    cargo test
    just clean

clean:
    rm -rf data

fmt:
    cargo fmt --all

test:
    cargo test
    just clean

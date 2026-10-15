build:
    cargo build --all-features

alias t := test

test:
    cargo check
    cargo nextest run --all-features

tsan:
    RUSTFLAGS=-Zsanitizer=thread RUSTDOCFLAGS=-Zsanitizer=thread cargo test --all-features -Zbuild-std --target x86_64-unknown-linux-gnu

asan:
    RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address cargo test --all-features -Zbuild-std --target x86_64-unknown-linux-gnu

msan:
    RUSTFLAGS=-Zsanitizer=memory RUSTDOCFLAGS=-Zsanitizer=memory cargo test --all-features -Zbuild-std --target x86_64-unknown-linux-gnu

sanitize: tsan asan

changelog version:
    git cliff --tag {{ version }} --topo-order -o CHANGELOG.md

bench:
    cargo bench

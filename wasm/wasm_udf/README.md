# Rust UDF

## Install Rust

1. `brew install rustup`
2. `rustup-init`
3. `source "$HOME/.cargo/env"`
4. `rustup target add wasm32-unknown-unknown`

## Compile Rust to WASM

```
cargo build --target wasm32-unknown-unknown --release
```

## Test in browser

```
python -m http.server
```

Open `http://localhost:8000/test.html`
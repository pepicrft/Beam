# Beam

Beam is an open source launcher project inspired by Raycast, built in Rust with Zed's GPUI rendering stack.

The repository currently contains the first foundation slice:

- a cross-platform async file indexer
- a standalone `beam-indexer` process with CLI and JSON lines modes
- a minimal GPUI shell in `beam-app`
- GitHub Actions coverage for build, test, clippy, and format checks on macOS, Linux, and Windows

## Goals

- keep the UI thread free of blocking filesystem work
- support macOS, Linux, and Windows from the start
- separate long-running indexing work from the launcher UI
- use Rust-native async APIs throughout the indexing path

## Project Layout

- `src/indexer/`: shared indexing core
- `src/bin/beam-indexer.rs`: standalone indexer service and CLI entry point
- `src/bin/beam-app.rs`: GPUI desktop shell
- `.github/workflows/ci.yml`: cross-platform CI

## Getting Started

Install the toolchain with `mise`:

```sh
mise install
```

Run the desktop shell:

```sh
mise exec -- cargo run --bin beam-app
```

Run the indexer directly:

```sh
mise exec -- cargo run --bin beam-indexer -- scan --json
```

Start the JSON lines service mode:

```sh
mise exec -- cargo run --bin beam-indexer -- serve
```

## Development

Common commands:

```sh
mise exec -- cargo build --workspace --all-targets
mise exec -- cargo test --workspace --all-targets
mise exec -- cargo clippy --workspace --all-targets --all-features -- -D warnings
mise exec -- cargo fmt --all --check
```

On macOS, GPUI depends on Apple's Metal toolchain. If `xcrun metal` fails, install it with:

```sh
sudo xcodebuild -downloadComponent MetalToolchain
```

## Status

Beam is in early bootstrap. The current implementation focuses on file indexing so the launcher can grow on top of a non-blocking, cross-platform search foundation.

## License

MIT. See [LICENSE](LICENSE).

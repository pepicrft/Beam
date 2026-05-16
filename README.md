# Beam

Beam is an open source launcher project inspired by Raycast, built in Rust with Zed's GPUI rendering stack.

The repository currently contains the first foundation slice:

- a cross-platform persisted file indexer
- a standalone `beam-indexer` process with CLI and JSON lines modes
- a minimal GPUI shell in `beam-app`
- GitHub Actions coverage for build, test, clippy, and format checks on macOS, Linux, and Windows

## Goals

- keep the UI thread free of blocking filesystem work
- support macOS, Linux, and Windows from the start
- separate long-running indexing work from the launcher UI
- keep the public service boundary async while pushing heavy search/index work off the UI path

## Current Architecture

The indexer now mirrors the shape of a native launcher search service:

- a Tokio actor for commands, snapshots, refreshes, and filesystem events
- a persisted Tantivy search index for fast startup and fast search
- an async scanner with exclusion globs, hidden-file filtering, and ignore-file support
- event-based watching with a poll watcher kept alongside it for cross-platform reliability
- persisted queue, stats, watch, and version files

Small UTF-8 text files can also be content-indexed when `index_contents` is enabled in the `IndexConfig`.

## Project Layout

- `src/indexer/`: shared indexing core
- `src/bin/beam-indexer.rs`: standalone indexer service and CLI entry point
- `src/bin/beam-app.rs`: GPUI desktop shell
- `.github/workflows/ci.yml`: cross-platform CI

## Storage

Beam stores index data using platform-native application directories:

- Linux: XDG data/state directories
- macOS: `~/Library/Application Support/...`
- Windows: the equivalent local app data directories

Within that layout Beam persists:

- `index/db/`: Tantivy index segments
- `index/version`: index schema/version marker
- `index/queue.json`: persisted operation queue state
- `index/stats.json`: persisted indexing statistics
- `index/watch.json`: watcher metadata

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

Beam is in early bootstrap. The current implementation focuses on a persisted, restart-friendly file indexer so the launcher can grow on top of a non-blocking, cross-platform search foundation.

## License

MIT. See [LICENSE](LICENSE).

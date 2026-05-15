# Beam Agent Notes

## Project Direction

- Beam is a Rust launcher project using Zed's GPUI stack.
- The current implemented subsystem is the async file indexer.
- Keep the product direction cross-platform: macOS, Linux, and Windows are first-class targets.

## Engineering Rules

- Do not introduce blocking filesystem work on the UI path.
- Prefer Tokio and other async APIs for indexing, scanning, watching, and service orchestration.
- Keep the indexer usable as a standalone process, not only as an in-process library.
- Preserve the separation between the shared indexing core in `src/indexer/` and the UI shell in `src/bin/beam-app.rs`.

## Tooling

- Use `mise exec -- ...` if the Rust toolchain is not already on the shell path.
- Keep crate builds warning-free.
- If the crate license changes, update both `Cargo.toml` and `LICENSE`.

## Validation

When changing Rust code or CI, prefer running:

```sh
mise exec -- cargo build --workspace --all-targets
mise exec -- cargo test --workspace --all-targets
mise exec -- cargo clippy --workspace --all-targets --all-features -- -D warnings
mise exec -- cargo fmt --all --check
```

## Platform Notes

- GPUI on macOS requires Apple's Metal toolchain. If `xcrun metal` fails, install it with `sudo xcodebuild -downloadComponent MetalToolchain`.
- Keep the GitHub Actions matrix aligned with macOS, Linux, and Windows support.

#!/usr/bin/env bash
# Single quality gate used by CI and by build-lab before every push:
# format check, lint, tests.
set -euo pipefail
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets --quiet

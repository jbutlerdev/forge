# Forge development tasks.
#
# Requires `just` (https://github.com/casey/just) — e.g.
# `nix shell nixpkgs#just --command just build` on this host, or
# install via your package manager. Plain shell works too: every
# recipe is a one-liner you can copy out of this file.

# Release build of the API server.
build:
    cargo build --release -p forge-api

# Full test suite (needs Postgres; the pi-spawning suites also need
# the `pi` binary on PATH and a built forge-tools extension).
test:
    cargo test --workspace

# Clippy, denying warnings.
lint:
    cargo clippy --workspace --all-targets -- -D warnings

# Formatting check.
fmt:
    cargo fmt --all --check

# Build the forge-tools extension (pi loads dist/index.js, not the
# TypeScript source).
ext:
    cd extensions/forge-tools && npm run build

# Basic JS syntax check of the web UI (no external linter deps).
web:
    cd web && node --check app.js && node --check sw.js

# shellcheck the CLI; skips gracefully when shellcheck is missing.
shellcheck:
    if command -v shellcheck > /dev/null; then
        shellcheck cli/forge cli/forge.d/*.sh
    else
        echo "shellcheck not installed — skipping (install it to lint the CLI)"
    end

# Everything above.
all: fmt lint test ext web shellcheck

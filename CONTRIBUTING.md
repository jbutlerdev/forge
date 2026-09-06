# Contributing to Forge

Thanks for your interest in Forge! This guide covers what you need to
build, test, and lint the project, plus how we work on changes.

## Prerequisites

- **Rust** (stable; `rust-toolchain.toml` pins the channel and components —
  `rustup` picks it up automatically)
- **PostgreSQL 15+** — for the integration/e2e test suites. The tests create
  and drop their own `forge_test_<uuid>` database, so a scratch `postgres`
  role with `CREATE` privilege is enough. `pgcrypto` must be available
  (it is by default on most distro packages).
- **Node.js 20+** and **npm** — for building the `forge-tools` extension.
  The pi-spawning tests additionally run `pi` itself, which needs Node 22
  (CI pins `@earendil-works/pi-coding-agent@0.84.4`).
- **nix** — only if you build the sandbox package set (`sandbox/`).
- **`just`** (optional) — runs the `justfile` recipes that wrap the
  commands below (`just build`, `just test`, …).

## Building

Prefer the `justfile` at the repo root for common tasks: `just build`, `just test`, `just lint`, `just fmt`, `just ext`, `just web`, `just shellcheck`, or `just all` (requires [`just`](https://github.com/casey/just); every recipe is a one-liner you can copy out).

```bash
# API server (debug)
cargo build -p forge-api

# Release
cargo build --release -p forge-api

# forge-tools extension (loaded by pi at runtime; CI rebuilds it)
cd extensions/forge-tools && npm ci && npm run build
```

Migrations are embedded in the binary via `sqlx::migrate!` and run
automatically at startup — there is no separate migration step.

## Testing

Use `just test` (`cargo test --workspace`), or manually:

```bash
# Unit tests (no DB needed)
cargo test -p forge-api --lib

# Everything, incl. integration/e2e/pi-spawn suites (needs Postgres + pi)
export DATABASE_URL=postgres://user:pass@localhost/5432/postgres
cargo test --workspace --all-targets --no-fail-fast
```

The integration/e2e tests each create and drop their own database, so
running them against a dev Postgres is safe.

## Linting & formatting

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
shellcheck -S error cli/forge
find cli/forge.d scripts -name '*.sh' -print0 | xargs -0 -n1 shellcheck -S error
```

CI (`.github/workflows/ci.yml`) runs all of the above plus a TypeScript
build of the extension and a release-mode compile. A PR is expected to be
green across every job.

## Migrations

SQLx migrations live in `crates/forge-api/migrations/` — **not** at the
workspace root (`sqlx::migrate!("./migrations")` resolves relative to the
crate manifest dir). The full workflow, including the re-run-safety rules
(`CREATE OR REPLACE`, `IF NOT EXISTS`) and how to verify application, is in
[`docs/OPERATIONS.md`](docs/OPERATIONS.md#migrations).

## Commit conventions

- Imperative, scoped subject: `fix(router): drop clippy warnings`,
  `feat(voice): add TTS proxy`, `oss: scrub internal IPs`.
- One logical change per commit; keep commits bisectable.
- Reference issues in the body (`Fixes #123`).

## General notes

- Read [`AGENTS.md`](AGENTS.md) before touching the Rust code — it documents
  the executor/harness split, the `messages` audit-log invariants, and the
  operational quirks that bit previous contributors.
- `docs/` is the documentation source of truth; when the API surface, env
  vars, or operational workflow changes, update the relevant doc
  (`docs/API.md`, `docs/OPERATIONS.md`, …) and `AGENTS.md` together.
- The extension (`extensions/forge-tools/`) is compiled to
  `dist/index.js`, which is what pi loads — always rebuild after changing
  the TypeScript, and never hand-edit `dist/`.

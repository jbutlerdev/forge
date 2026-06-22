# The `search` tool

The forge sandbox ships the [`mule-ai/search`](https://github.com/mule-ai/search)
Go CLI as a top-level command. It's a thin SearXNG-backed
metasearch wrapper: one invocation, JSON / Markdown / plain
text output, no scraping, no rate-limit dances against a
single search engine.

The LLM discovers it as a regular command in the sandbox
(it lives at `/usr/local/bin/search` and is on `$PATH`),
and the bundled [`search-cli` skill](../skills/search-cli/SKILL.md)
gives it the full flag reference + best-practice patterns
for combining `search` with the other forge tools
(`read`, `curl`, `jq`).

This document is the operator's reference. The agent
sees the skill automatically; you only need to read this
file if you're configuring forge, debugging a "search
doesn't work" report from a session, or rolling the
upstream `search` forward.

## TL;DR

- **What it is:** a Go CLI binary, pinned to the
  [v1.0.1 release](https://github.com/mule-ai/search/releases/tag/v1.0.1)
  of `mule-ai/search`.
- **How it gets into the sandbox:** built via
  `sandbox/default.nix` and symlinked into the base
  rootfs's `/usr/local/bin` by `sandbox/build.sh`. New
  sessions pick it up on first bash call; existing
  sessions need `/new` (fresh session) or
  `POST /admin/sandbox-reset` to refresh.
- **How the LLM is told it exists:** the
  `skills/search-cli/SKILL.md` skill is
  passed to `pi` as `--no-skills --skill <skills-dir>`, so the model sees
  the skill in its skill list and can load the flag
  reference on demand.
- **How the LLM configures it:** `SEARCH_INSTANCE` and
  `SEARCH_API_KEY` env vars, threaded into the
  per-session nspawn container from
  `FORGE_SEARCH_INSTANCE` / `FORGE_SEARCH_API_KEY` on
  the forge-api process.

## Operator setup

The default instance the binary falls back to is
`https://search.butler.ooo` (baked in at build time by
the upstream project), so a fresh install already gives
the LLM a working `search` against that instance. You
only need the env vars when you want to point at a
private / auth-bearing instance, or want to make the
default explicit so the LLM doesn't see surprises if
upstream changes its compiled-in default.

Add the following lines to `/etc/forge/forge.env`
(mode 0600, the same file that holds `FORGE_API_KEY`,
`DATABASE_URL`, etc.):

```bash
# SearXNG instance the bundled `search` CLI talks to.
# Defaults to the upstream's compiled-in default
# (https://search.butler.ooo) if unset; set this to
# override.
FORGE_SEARCH_INSTANCE=https://search.butler.ooo

# Optional. Only required if your SearXNG instance has
# `server.secret_key` / `auth.methods` enabled. Leave
# unset (or set to an empty string) for public instances.
FORGE_SEARCH_API_KEY=
```

Then restart forge-api (`sudo systemctl restart forge-api`
is **not** the supported path — use the
`POST /admin/self-update` endpoint or the operator
workflow described in `AGENTS.md` §"Deploying a new
`forge-api` binary"). Any new session picks up the
env vars on its first bash call; existing sessions
get them on the next call too, because the env vars
are added to every `systemd-nspawn` invocation in
`crates/forge-api/src/sandbox.rs`, not baked into
the per-session rootfs.

The env vars land inside the container as
`SEARCH_INSTANCE` and `SEARCH_API_KEY` — those are
the names the upstream `search` binary reads (see
`internal/config/config.go::applyEnvironmentVariables`).
We're translating operator-side `FORGE_*` to
binary-side `SEARCH_*` at the sandbox boundary so the
operator namespace doesn't collide if the LLM also
wants to override the instance for a single query
(`-i <url>` flag).

## Verifying it works

After the env vars are set, the cheapest end-to-end
check is to open a session and ask the LLM to run:

```bash
search -f json -n 1 "hello world" | jq '.metadata.instance, .total_results'
```

A working setup prints something like:

```json
"https://search.butler.ooo"
42
```

If the instance is unreachable, `search` exits with a
non-zero status and a clear error message; the LLM
will see the error in its tool result and can either
try a different instance (`search -i https://searx.work "..."`)
or fall back to non-search strategies (the model's
training data, an explicit `read` against a known URL,
etc.).

## What goes where

```
┌──────────────────────────────┐
│  forge-api process           │
│  (env: FORGE_SEARCH_*)       │
└──────────────┬───────────────┘
               │  per-bash-call: --setenv=SEARCH_INSTANCE=...
               ▼
┌──────────────────────────────┐
│  systemd-nspawn container    │
│  (env: SEARCH_INSTANCE,      │
│         SEARCH_API_KEY)      │
│  PATH includes /usr/local/bin│
│                              │
│  /usr/local/bin/search ────► /nix/store/…-search-1.0.1/bin/search
│  (a symlink the sandbox      │   (the actual binary, shipped
│   build.sh dropped in)       │    in the upstream goreleaser
│                              │    tarball; ~14 MB, statically
│                              │    resolved, only links libc)
└──────────────────────────────┘
```

And independently of the binary:

```
┌──────────────────────────────┐
│  pi subprocess               │
│  --no-skills --skill         │
│   <repo>/skills/             │
│                              │
│  └─ search-cli/              │
│     └─ SKILL.md  ◄── the LLM │
│                     loads    │
│                     this when │
│                     it needs  │
│                     the flag  │
│                     reference │
└──────────────────────────────┘
```

## Bumping the upstream version

1. Pick the upstream tag (e.g. `v1.0.2`).
2. Edit `sandbox/default.nix` and change
   `searchVersion` to `1.0.2` (drop the `v`).
3. Update `searchTarball.hash` to the new SRI. The
   easiest way to compute it:

   ```bash
   curl -sL -o /tmp/search-new.tar.gz \
     https://github.com/mule-ai/search/releases/download/v1.0.2/search-linux-amd64.tar.gz
   sha256sum /tmp/search-new.tar.gz | awk '{print $1}' \
     | xargs -I{} nix --extra-experimental-features 'nix-command' \
         hash to-sri --type sha256 {}
   ```

   The output is the value to paste into
   `searchTarball.hash`.
4. Update `skills/search-cli/SKILL.md` if
   the upstream flag set changed (compare against the
   new `SYSTEM.md` and `README.md` in the new tag).
5. `git add` the two files, commit, push.
6. `./sandbox/build.sh` from a checkout of the new
   commit (this rebuilds the base rootfs and is what
   new sessions will see).
7. `POST /admin/sandbox-reset?session_id=<uuid>` for
   any existing long-running session that should
   pick up the change immediately (otherwise it sees
   the old binary until the next time the operator
   runs `/new` against the session).

## Why the binary and not a first-class forge tool?

You might reasonably ask: why not add `search` to the
`forge-tools` extension the same way `bash`, `read`,
`write`, and `edit` are registered? Two reasons:

1. **Tool surface area.** The forge-tools extension
   is the path tool calls take to reach a Rust handler
   in `tool_executor.rs` (or the streaming-bash
   handler in `api/sse.rs`). Each handler is responsible
   for writing a call row + a result row to the audit
   log, recording the `tool_call_id`, etc. — see
   `AGENTS.md` §7 and `recording.rs` for the
   contract. `search` doesn't need any of that. It
   runs inside an existing `bash` tool call (the
   harness already records the call + the result),
   produces stdout the LLM can read directly, and
   doesn't carry any per-call state that has to be
   audited beyond the bash invocation that wraps it.
2. **Skill over system prompt.** The user-facing
   documentation for `search` (the `SYSTEM.md` content
   from the upstream repo) is the kind of reference
   the model only needs when it's actively about to
   call `search`. Stuffing the whole thing into the
   system prompt would burn context on every turn
   for a tool most turns don't need. Skills solve
   that: the model sees the skill in its list, loads
   the reference on demand, and unloads it when it's
   done.

If a future change needs `search` to be a real
first-class tool (e.g. to capture the result as a
structured jsonb blob, or to enforce a per-profile
result-count cap), the migration path is to add
a `match` arm in `tool_executor.rs::execute` (the
"bash" / "read" / "write" / "edit" arm list) and
register the tool in `extensions/forge-tools/src/index.ts`.
The binary-in-sandbox + skill combo will keep working
in the meantime.

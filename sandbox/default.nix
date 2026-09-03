# Default user packages for the forge sandbox rootfs.
#
# The sandbox is a per-session Debian rootfs (debootstrapped
# from /forge/sandbox/base/) plus a set of user-level
# packages built with Nix and symlinked into
# /usr/local/bin. Debian provides libc, init, basic system
# tools; this list is for the modern, reproducible tooling
# the LLM actually uses (git for clone/push, curl/jq for
# API work, the Rust toolchain so the LLM can `cargo
# build`/`cargo test` inside a cloned repo without first
# having to install a compiler, etc.).
#
# To add or remove a package, edit the list below and run
#   ./sandbox/build.sh
# from the repo root. The build script `nix-build`s (or
# `nix build`s, when the flake is available) this
# expression, symlinks the resulting binaries into the
# base rootfs, and updates /etc/ssl/certs in the base from
# the Nix-built ca-certificates bundle. New sessions pick
# the changes up on their first bash call; existing
# sessions need /new (which creates a fresh session, whose
# rootfs is `cp -a`'d from the updated base) or
# POST /admin/sandbox-reset to wipe and re-cp.
#
# Why this isn't a `devShell`: devShells only run inside
# `nix develop`, not inside arbitrary processes. We need
# the binaries to be on PATH in the LLM's bash tool, which
# is invoked via `nspawn ... -- bash -c '...'`. A
# buildEnv result that gets symlinked into /usr/local/bin
# is the path of least resistance: PATH already includes
# /usr/local/bin, no shell init or wrappers required.
#
# Why this takes `pkgs` and `rustToolchain` as function
# arguments: the file is consumable in two ways.
#
#   1. Via the flake (`nix build .#sandbox-deps`): the
#      flake imports `rust-overlay` and passes the
#      resulting `pkgs` here, so the Rust toolchain is
#      pinned to whatever version the flake selects
#      (see `rustToolchain` in flake.nix).
#   2. Standalone (`nix-build sandbox/default.nix`):
#      `pkgs` defaults to plain `<nixpkgs>` and the
#      Rust toolchain is omitted. This keeps the
#      legacy invocation path working on hosts that
#      don't have the flake's `rust-overlay` input
#      available.
#
# Toolchain pinning: the Rust components mirror
# `rust-toolchain.toml` at the repo root (channel
# `stable`, components `rustfmt` / `clippy` /
# `rust-analyzer`). When the operator wants to bump the
# sandbox Rust version, they update the channel + date in
# the `rustToolchain` attrset in flake.nix (and run
# `nix flake lock --update-input rust-overlay`), then
# re-run `./sandbox/build.sh`.
#
# Note: the per-session rootfs gets `cp -a`'d from base,
# which means the /nix/store symlinks (and the binaries
# they point to, via the bind-mount of /nix into each
# session) are duplicated per session. For our scale
# (handful of LLM sessions at a time) this is fine; if
# the per-session overhead ever becomes a problem, the
# next move is to bind-mount /nix/store from the host
# read-only into every session so the store is shared.

{ pkgs ? import <nixpkgs> {}
# `rustToolchain` is supplied by flake.nix when building
# through the flake. The argument is a `rust-bin` from
# `oxalica/rust-overlay` and carries rustc, cargo, and
# the requested components. We don't default it here
# because a default would silently pull a toolchain from
# the operator's nixpkgs cache (which is whatever the
# stable channel happened to be at evaluation time) and
# defeat the point of pinning it in the flake. The
# standalone fallback just doesn't include the toolchain.
, rustToolchain ? null
# `search` is a thin wrapper around the upstream
# `mule-ai/search` Go binary. The default fetches the
# linux-amd64 build from the project's GitHub release
# tarball (which is what `goreleaser` produces — a single
# statically-resolved Go binary that only links libc,
# which the base rootfs already has). Override
# `searchTarball` / `searchTarballHash` in the flake to
# pin a different version, host, or a local tarball.
#
# Why a prebuilt instead of `buildGoModule`? `buildGoModule`
# requires a `vendorHash` that has to be computed against
# the upstream go.sum. We can't compute it without
# bootstrapping Go, and the alternative (`vendorHash =
# lib.fakeHash` + "build will tell you") leaves every
# operator who runs `./sandbox/build.sh` for the first
# time with a hash-update step before anything works. The
# upstream goreleaser tarball has a known hash, a known
# provenance (the maintainer's release workflow), and
# requires no Go toolchain in the Nix closure.
, searchVersion ? "1.0.1"
, searchTarball ? pkgs.fetchurl {
    url = "https://github.com/mule-ai/search/releases/download/v${searchVersion}/search-linux-amd64.tar.gz";
    sha256 = "sha256-zZk7TcWpJtjZxjhMAbvNNBCwOFyyFJFXvI4xi9RHiWo=";
  }
}:

let
  # `pkgs` is expected to already have `rust-overlay`
  # applied when `rustToolchain` is non-null (that's the
  # contract the flake follows). We don't apply the
  # overlay here: doing so twice is a no-op in nixpkgs,
  # but a standalone caller that wants the toolchain
  # would pass `pkgs` with the overlay baked in *and*
  # the `rustToolchain` attrset, which is the natural
  # way to compose `buildEnv` with overlays. The
  # historical name `pkgs'` is kept for readability of
  # the `with pkgs'; [...]` blocks below.
  pkgs' = pkgs;

  # Packages that always go in the sandbox, regardless of
  # whether the toolchain is being included.
  basePackages = with pkgs'; [
    # Version control. git is required for the
    # /usr/local/bin/git-credential-github helper to
    # actually have a `git` to drive.
    git

    # HTTP / API. curl for fetching; jq for parsing.
    # ca-certificates pulls in the bundle and is
    # extracted to /etc/ssl/certs in the base by
    # build.sh so HTTPS works out of the box.
    curl
    jq
    cacert

    # Common POSIX / core utilities. The Debian
    # base has its own versions, but the Nix ones
    # are pinned to a single nixpkgs revision, so
    # behavior is reproducible across hosts.
    gnused
    gawk
    gnugrep
    coreutils
    findutils
    diffutils
    procps

    # Terminal pager. The LLM shells out to `git log`,
    # `git diff`, `man`, etc. constantly; all of them
    # default to invoking a pager and will die with
    # "cannot run less: No such file or directory" if
    # less isn't on PATH. The Debian base debootstrap
    # doesn't include it (it's a Recommends, not a
    # Depends, of base-files), and the Nix set above
    # doesn't pull it transitively. Adding it here
    # costs ~250KB and removes a class of confusing
    # "git log" failures (one of which has already
    # hung a session for an hour because the LLM
    # wrapped the failing call in `2>&1; echo exit=$?`
    # and the wrapper stalled in the streaming-bash
    # path — see ops notes 2026-06-05).
    less

    # Interactive shell. The Debian base has
    # /bin/bash via the usr-merge, but pinning the
    # build here makes the LLM's interactive shell
    # behavior consistent with what was tested.
    bashInteractive

    # Build essentials (gcc, make, etc.) for the
    # common case of `cargo build` or `npm install`
    # inside a cloned repo. The LLM is allowed to
    # compile code; this just gives it the toolchain
    # without needing to apt install on first use.
    #
    # `stdenv` alone is not enough: it's a build
    # environment that *uses* a compiler, but it
    # doesn't put `cc`/`gcc` in its `bin` output
    # (the compiler is a separate derivation in
    # nixpkgs). Without `gcc` added explicitly,
    # `cargo build` fails with "linker `cc` not
    # found" on the first native dependency.
    stdenv
    gcc

    # The nix package manager itself, so the LLM can
    # do ad-hoc one-off installs (`nix shell
    # nixpkgs#htop -- htop`) inside a single
    # session. Installs do not persist across
    # sessions: `/nix/store` is bind-mounted
    # read-only and `/nix/var/nix` is not
    # bind-mounted at all (the host-isolation
    # guarantee the sandbox is supposed to provide).
    # Persistent installs are an operator decision
    # via this file + sandbox/build.sh.
    nix

    # --- Rust toolchain build-time helpers ---
    #
    # The Rust compiler is a binary that the LLM
    # invokes directly, but compiling a non-trivial
    # Rust crate (which is the common case in a
    # forge session: the LLM is iterating on
    # forge-api itself) pulls in a long tail of
    # C-side tools and headers. The most common
    # ones are:
    #
    # - `pkg-config`: the `*-sys` crates (openssl-sys,
    #   pq-sys, libz-sys, ring's deps, etc.) all shell
    #   out to `pkg-config` to find header and library
    #   paths. Without it, `cargo build` fails with
    #   "pkg-config not found" on the first native
    #   dependency.
    # - `cmake`: the modern `aws-lc-sys`, `libgit2-sys`,
    #   and a few `ring` builds use CMake as their
    #   build system. Without it, builds of these
    #   crates fail with "No CMAKE_CXX_COMPILER
    #   could be found" or equivalent.
    # - `perl`: OpenSSL 3.x's `Configure` script is
    #   perl, and several `*-sys` build.rs scripts
    #   pipe their output through perl for
    #   substitution. `cargo`'s own gix-based registry
    #   also shells out to perl for some operations.
    # - `which`: ubiquitous in build.rs scripts that
    #   probe for sibling tools.
    # - `lld`: dramatically faster linker than
    #   binutils' `ld.bfd`; matters for `cargo test`
    #   in particular, which relinks test binaries
    #   for every integration test.
    pkg-config
    cmake
    perl
    which
    lld

    # --- System libraries for native dependencies ---
    #
    # These are the libraries that the Rust `*-sys`
    # crates link against. We add the `.dev` outputs
    # explicitly (when available) so headers and
    # `*.pc` files are visible to pkg-config inside
    # the merged `/lib/pkgconfig` tree; the runtime
    # `.so` files come through the default output.
    #
    # - `openssl` / `openssl.dev`: `reqwest` with the
    #   default `native-tls` feature builds against
    #   OpenSSL. The `dev` output carries the headers
    #   and `openssl.pc`; the default output carries
    #   `libssl.so` / `libcrypto.so` for runtime
    #   linking.
    # - `libpq`: the C client library for PostgreSQL.
    #   `sqlx`'s `postgres` feature uses `pq-sys`
    #   (the `pkg-config` discovery variant) to bind
    #   to it. `libpq` is the `dev` output of
    #   `postgresql`; adding the default output of
    #   `postgresql` would also pull the server, which
    #   is wasteful.
    # - `zlib` / `zlib.dev`: `libz-sys` (used by
    #   `flate2` and indirectly by `reqwest`/`cargo`)
    #   builds against this.
    # - `zstd` / `zstd.dev`: `zstd-sys` is used by
    #   Cargo's `gix`-based registry client and by
    #   several compression crates.
    #
    # When `pathsToLink` includes `/lib` and
    # `/include`, `buildEnv` symlinks each package's
    # `lib/` and `include/` into the merged tree, so
    # `pkg-config --cflags openssl` and the dynamic
    # linker both find what they need.
    openssl
    openssl.dev
    libpq
    zlib
    zlib.dev
    zstd
    zstd.dev
  ];

  # The Rust toolchain, when one is available. We pass
  # the whole `rust-bin` to `paths` because `buildEnv`
  # accepts any derivation, and `rust-bin` from
  # `oxalica/rust-overlay` aggregates rustc, cargo,
  # rustfmt, clippy, and rust-analyzer into a single
  # output whose `bin/` contains all the symlinks
  # (`rustc`, `cargo`, `cargo-fmt`, `cargo-clippy`,
  # `rust-analyzer`).
  rustPackages = if rustToolchain == null then [] else [
    rustToolchain
  ];

  # The `search` Go CLI from https://github.com/mule-ai/search.
  # We unwrap the upstream goreleaser tarball (a single
  # `search-linux-amd64` binary) and place it at
  # `$out/bin/search`. The base rootfs already has glibc,
  # which is the only library the binary links against
  # (`ldd search-linux-amd64` reports just `libc.so.6`).
  #
  # Pinned to the upstream release tarball rather than
  # built from source with `buildGoModule` because the
  # release tarball has a known sha256 we can pin
  # up-front, and a `buildGoModule` derivation would need
  # a `vendorHash` that has to be computed by a Go
  # toolchain that the sandbox doesn't ship. The
  # upstream tarball is what `goreleaser` produces in
  # `.github/workflows/release.yml`, so the
  # "build the binary on a maintainer's box and ship the
  # tarball" path is the supported distribution channel
  # for this tool anyway.
  searchPackage = pkgs'.stdenv.mkDerivation {
    pname = "search";
    version = searchVersion;
    src = searchTarball;
    nativeBuildInputs = [ pkgs'.gnutar ];
    sourceRoot = ".";
    dontConfigure = true;
    dontBuild = true;
    installPhase = ''
      mkdir -p "$out/bin"
      install -m 0755 search-linux-amd64 "$out/bin/search"
    '';
    meta = with pkgs'.lib; {
      description = "SearXNG-backed CLI for ad-hoc web search (mule-ai/search)";
      homepage = "https://github.com/mule-ai/search";
      license = licenses.mit;
      platforms = [ "x86_64-linux" ];
      mainProgram = "search";
    };
  };

  # All extras beyond the base package set. The build
  # script (`sandbox/build.sh`) symlinks
  # `$BUILD_OUT/bin/*` into the base rootfs's
  # `/usr/local/bin`, so anything in `extraPackages`
  # becomes a top-level command in the LLM's bash
  # tool.
  extraPackages = [
    searchPackage
  ];
in

pkgs'.buildEnv {
  name = "forge-sandbox-defaults";

  paths = basePackages ++ rustPackages ++ extraPackages;

  # buildEnv gives a symlink-farm directory; pathsToLink
  # controls which top-level dirs to populate. We need:
  #
  # - `/bin`    — every package's user-facing binaries
  #   (cargo, rustc, git, curl, …) get symlinked into
  #   `$out/bin`, which the build script then symlinks
  #   into the base rootfs's `/usr/local/bin`.
  # - `/lib`    — system libraries (`libssl.so`,
  #   `libpq.so`, `libz.so`, …). The dynamic linker
  #   inside the container finds them via the
  #   `/nix/store/<hash>-lib/lib` path that the merged
  #   buildEnv output points to.
  # - `/include` — C headers (`openssl/ssl.h`,
  #   `libpq-fe.h`, `zlib.h`, …) and pkg-config
  #   metadata (`lib/pkgconfig/*.pc`).
  # - `/etc`    — the ca-certificates bundle, picked up
  #   by `sandbox/build.sh` and copied into the base.
  # - `/share`  — pkg-config's auxiliary search path and
  #   any man pages / completions.
  #
  # We do NOT symlink `/sbin` (operator-only) or `/libexec`
  # (the base's init doesn't know about it). The base's
  # existing `/etc/{passwd,group,resolv.conf}` stay
  # untouched.
  pathsToLink = [ "/bin" "/lib" "/include" "/etc" "/share" ];
}

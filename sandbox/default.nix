# Default user packages for the forge sandbox rootfs.
#
# The sandbox is a per-session Debian rootfs (debootstrapped
# from /forge/sandbox/base/) plus a set of user-level
# packages built with Nix and symlinked into
# /usr/local/bin. Debian provides libc, init, basic system
# tools; this list is for the modern, reproducible tooling
# the LLM actually uses (git for clone/push, curl/jq for
# API work, etc.).
#
# To add or remove a package, edit the list below and run
#   ./sandbox/build.sh
# from the repo root. The build script `nix-build`s this
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
# Note: the per-session rootfs gets `cp -a`'d from base,
# which means the /nix/store symlinks (and the binaries
# they point to, via the bind-mount of /nix into each
# session) are duplicated per session. For our scale
# (handful of LLM sessions at a time) this is fine; if
# the per-session overhead ever becomes a problem, the
# next move is to bind-mount /nix/store from the host
# read-only into every session so the store is shared.

{ pkgs ? import <nixpkgs> {} }:

pkgs.buildEnv {
  name = "forge-sandbox-defaults";

  paths = with pkgs; [
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
    stdenv
  ];

  # buildEnv gives a symlink-farm directory; pathsToLink
  # controls which top-level dirs to populate. We only
  # need /bin (the user-facing binaries — symlinked into
  # the base at /usr/local/bin) and /etc (for the
  # ca-certificates bundle). The base's existing
  # /etc/{passwd,group,resolv.conf} etc. stay untouched.
  pathsToLink = [ "/bin" "/etc" "/share" ];
}

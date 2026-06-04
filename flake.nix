{
  description = "Forge - AI Agent Platform";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
          # Allow unfree packages (e.g. CUDA toolchains, etc.)
          # the operator might add later. Doesn't change
          # the default set in any way.
          config.allowUnfree = true;
        };

        # Pinned Rust toolchain for both the dev shell
        # and the sandbox. `stable` here means whatever
        # the latest stable release is in
        # `oxalica/rust-overlay` at evaluation time —
        # `rust-overlay` tracks rust-lang/rust releases
        # on a roughly-daily cadence, so a fresh
        # `nix flake update` will roll this forward.
        # To pin to a specific date, replace `stable`
        # with `stable.<YYYY-MM-DD>`.
        #
        # The component list mirrors `rust-toolchain.toml`
        # at the repo root. Adding a component here
        # without adding it to `rust-toolchain.toml` (or
        # vice versa) is harmless — `cargo` doesn't care
        # which manifest specified which component — but
        # it'll surprise humans, so keep them in sync.
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [
            "rustfmt"
            "clippy"
            "rust-analyzer"
          ];
        };
      in
      {
        # The default development shell: Rust toolchain
        # (pinned to whatever the flake selects above,
        # not whatever `rustup` happens to have cached
        # locally), PostgreSQL, sqlx-cli, watchexec, and
        # the usual utilities.
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            # Pinned toolchain — same version that the
            # sandbox ships to the LLM, so "works in my
            # dev shell" implies "works in the sandbox".
            (rustToolchain)

            # Database
            postgresql_16

            # Dev tools
            sqlx-cli
            watchexec

            # Utilities
            curl
            jq
          ];

          # Environment
          DATABASE_URL = "postgres://postgres@localhost/forge";

          shellHook = ''
            echo "=== Forge dev shell ==="
            echo "Rust: $(rustc --version) (channel stable, pinned via rust-overlay)"
            echo "DATABASE_URL: $DATABASE_URL"
            echo ""
            echo "Quick start:"
            echo "  createdb forge"
            echo "  sqlx migrate run"
            echo "  cargo run -p forge-api"
            echo ""
            echo "Rebuild the sandbox default package set (operator-only):"
            echo "  nix build .#sandbox-deps && ./sandbox/build.sh"
          '';
        };

        # `nix build .#sandbox-deps` produces the same
        # symlink farm that the legacy
        # `nix-build sandbox/default.nix` invocation did,
        # but with the Rust toolchain included. The
        # build script (`sandbox/build.sh`) prefers the
        # flake output when it's available and falls back
        # to `nix-build` for hosts that don't have the
        # flake checked out.
        packages.sandbox-deps = pkgs.callPackage ./sandbox/default.nix {
          rustToolchain = rustToolchain;
        };
      }
    );
}

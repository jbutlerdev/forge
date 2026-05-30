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
        pkgs = import nixpkgs { inherit system overlays; };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            # Rust toolchain
            rustc
            cargo
            rustfmt
            clippy
            rust-analyzer

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
            echo "DATABASE_URL: $DATABASE_URL"
            echo ""
            echo "Quick start:"
            echo "  createdb forge"
            echo "  sqlx migrate run"
            echo "  cargo run -p forge-api"
          '';
        };
      }
    );
}

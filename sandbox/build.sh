#!/bin/bash
# Build the sandbox default package set with Nix and install
# the resulting binaries into /forge/sandbox/base/.
#
# The base is a debootstrapped Debian rootfs (libc, init,
# basic system tools) plus a user-level package set built
# by Nix and symlinked into /usr/local/bin. Edit
# sandbox/default.nix to add/remove packages, then run this
# script. New sessions see the new packages on their first
# bash call (the per-session rootfs is `cp -a` from base);
# existing sessions need a /new (or a manual
# /admin/sandbox-reset) to pick up changes.
#
# Idempotent: re-running is safe. Old Nix-store symlinks in
# /usr/local/bin are removed before the new ones are added,
# so packages that were removed from default.nix go away.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="${BASE_DIR:-/forge/sandbox/base}"
TARGET_BIN="$BASE_DIR/usr/local/bin"
TARGET_ETC_SSL="$BASE_DIR/etc/ssl"

# Locate a Nix install. nixuser owns the canonical one
# on this host; allow override via NIX_PROFILE for ops
# that moved Nix somewhere else.
if [ -z "${NIX_PROFILE:-}" ]; then
    for candidate in \
        /home/nixuser/.nix-profile/etc/profile.d/nix.sh \
        /etc/profile.d/nix.sh \
        /nix/var/nix/profiles/default/etc/profile.d/nix.sh; do
        if [ -f "$candidate" ]; then
            NIX_PROFILE="$candidate"
            break
        fi
    done
fi
if [ -n "${NIX_PROFILE:-}" ]; then
    # shellcheck disable=SC1090
    source "$NIX_PROFILE"
fi
if ! command -v nix-build >/dev/null 2>&1; then
    echo "nix-build not on PATH; install Nix or set NIX_PROFILE." >&2
    exit 1
fi

echo "==> Building default.nix (this can take a while on first run)..."
BUILD_OUT=$(nix-build "$SCRIPT_DIR" --no-out-link)
echo "    Nix build output: $BUILD_OUT"

echo "==> Installing binaries into $TARGET_BIN"
mkdir -p "$TARGET_BIN"
# Wipe any prior /nix/store symlinks we manage (so removed
# packages disappear from the base). Don't touch other
# symlinks in /usr/local/bin in case the operator has
# manually added something.
removed=0
added=0
while IFS= read -r link; do
    rm -f "$link"
    removed=$((removed + 1))
done < <(find "$TARGET_BIN" -maxdepth 1 -type l -lname '/nix/store/*' 2>/dev/null || true)

if [ -d "$BUILD_OUT/bin" ]; then
    for bin in "$BUILD_OUT/bin/"*; do
        [ -e "$bin" ] || continue  # skip broken symlinks in the build
        name=$(basename "$bin")
        ln -sfn "$bin" "$TARGET_BIN/$name"
        added=$((added + 1))
    done
fi
echo "    removed $removed old /nix/store symlinks, added $added new ones"

# ca-certificates: the Nix cacert package's /etc/ssl/certs
# bundle is the right one to ship in the base. We replace
# the base's /etc/ssl/certs wholesale (Debian's bundle is
# regenerated from /usr/share/ca-certificates; the Nix
# bundle is a flat copy of pre-validated PEM files).
if [ -d "$BUILD_OUT/etc/ssl/certs" ]; then
    echo "==> Installing ca-certificates to $TARGET_ETC_SSL/certs"
    rm -rf "$TARGET_ETC_SSL/certs"
    mkdir -p "$TARGET_ETC_SSL"
    cp -a "$BUILD_OUT/etc/ssl/certs" "$TARGET_ETC_SSL/certs"
    cert_count=$(find "$TARGET_ETC_SSL/certs" -type f -o -type l | wc -l)
    echo "    $cert_count certificate files"
fi

# BASH_ENV shim. bash sources $BASH_ENV on every
# `bash -c "..."` invocation, so this file gets sourced
# before the LLM's command runs. The shim sets up the
# per-user nix profile (if it exists) so that packages
# the LLM has installed via `nix profile install` are
# on PATH without the LLM having to source anything.
# The `zz-` prefix sorts it after the other profile.d
# files so a future addition that sets up
# /etc/profile.d/nix.sh first (e.g. for bash
# completions) doesn't get clobbered.
#
# We don't source `$PROFILE/etc/profile.d/nix.sh` like a
# standard Nix install would, because our nix (the one
# in this base's /usr/local/bin) was built from a
# tarball install, not the official installer — the
# user profile doesn't have an `etc/profile.d/nix.sh`
# of its own. We do the setup directly: prepend the
# profile's bin to PATH and export the standard
# NIX_USER_PROFILE_DIR / NIX_USER_PROFILE / NIX_PROFILES
# variables so child processes inherit them.
mkdir -p "$BASE_DIR/etc/profile.d"
cat > "$BASE_DIR/etc/profile.d/zz-nix-user-profile.sh" <<'SHIM_EOF'
# Auto-sourced by bash via BASH_ENV (see forge's
# `--setenv=BASH_ENV=/etc/profile.d/zz-nix-user-profile.sh`
# in the nspawn invocation). Sets up the per-user nix
# profile (if it exists) so packages the LLM installed
# via `nix profile install` are on PATH without the LLM
# having to source anything manually. The profile lives
# at /nix/var/nix/profiles/per-user/root/profile on the
# host (bind-mounted into the container), so installs in
# one session show up in the next.
export NIX_USER_PROFILE_DIR="/nix/var/nix/profiles/per-user/${USER:-root}"
export NIX_PROFILES="/nix/var/nix/profiles/per-user/${USER:-root}/profile"
if [ -L "$NIX_PROFILES" ]; then
    export NIX_USER_PROFILE="$NIX_PROFILES"
    case ":$PATH:" in
        *":$NIX_PROFILES/bin:"*) ;;
        *) export PATH="$NIX_PROFILES/bin:$PATH" ;;
    esac
fi
SHIM_EOF
chmod 0644 "$BASE_DIR/etc/profile.d/zz-nix-user-profile.sh"
echo "==> Installed BASH_ENV shim at $BASE_DIR/etc/profile.d/zz-nix-user-profile.sh"

echo ""
echo "Done. The base rootfs at $BASE_DIR has been updated."
echo "Existing sessions need a /new (or /admin/sandbox-reset)"
echo "to pick up the changes; new sessions will have them"
echo "on first bash call."

#!/usr/bin/env bash
# Renew the forge dd-cli (DoorDash) access token.
#
# dd-cli's exported access token is SHORT-LIVED (a few days). When a forge
# sandbox `dd-cli` command starts failing auth on an authenticated command
# (e.g. `dd-cli <service> ...` returns an auth/401 error), re-run this script
# to mint a fresh token and push it through the whole chain.
#
# Pipe/diagram of the persistence chain:
#   minted on mini (macOS keychain, logged in as jbutler)
#     -> `dd-cli export-token` (v0.2.2+; prints a fresh token, no keychain here)
#   stored in lab Vaultwarden org vault as `dd-cli/access-token` (source of truth)
#   -> DD_CLI_ACCESS_TOKEN in /etc/forge/forge.env
#     -> forge-api ContainerEnv passthrough -> `--setenv` on every sandbox bash
#       -> dd-cli inside the forge sandbox authenticates headlessly.
#
# Requires: `ssh` access to mini, `bw` + the lab vault master password (read
# from ct/_nixos/vaultwarden.env), and root to restart forge-api.
set -euo pipefail

MINI_USER="${MINI_USER:-jbutler}"
MINI_HOST="${MINI_HOST:-10.10.199.29}"
ENV_FILE="${ENV_FILE:-/etc/forge/forge.env}"
LAB="${LAB:-/data/jbutler/git/jbutlerdev/lab}"

# Vault org/collection the note lives in (the `lab` org vault).
V_ORG="${V_ORG:-0c39bf32-e9ee-45df-95dc-ddd0214861b4}"
V_COL="${V_COL:-b63a48ec-eaee-4176-8be8-5fab13a46184}"

for b in ssh bw jq base64 python3 systemctl grep sed cut; do
  command -v "$b" >/dev/null 2>&1 || { echo "missing required tool: $b" >&2; exit 1; }
done

# 1) Mint a fresh token on mini (it's the keychain-authenticated origin).
echo "==> minting token on $MINI_USER@$MINI_HOST ..."
TOK="$(ssh -o ConnectTimeout=10 "$MINI_USER@$MINI_HOST" '~/.local/bin/dd-cli export-token 2>/dev/null' | tail -1 | tr -d '[:space:]' || true)"
if [ -z "${TOK:-}" ]; then
  echo "ERROR: export-token returned an empty token on mini." >&2
  echo "       Is dd-cli logged in there (run `~/.local/bin/dd-cli login` on the mini)?" >&2
  exit 1
fi

# 2) Persist in the lab Vaultwarden org vault as the source of truth.
echo "==> storing in Vaultwarden (dd-cli/access-token) ..."
export BW_SESSION="${BW_SESSION:-$(cat ~/.cache/bw/session 2>/dev/null || true)}"
if ! bw getUser --session "$BW_SESSION" >/dev/null 2>&1; then
  PASS="$(grep '^VAULT_MASTER_PASSWORD=' "$LAB/ct/_nixos/vaultwarden.env" | cut -d= -f2-)"
  BW_SESSION="$(bw unlock "$PASS" 2>&1 | grep -oP 'BW_SESSION="\K[^"]+' | head -1)" \
    && echo "$BW_SESSION" > ~/.cache/bw/session && chmod 600 ~/.cache/bw/session
fi
OLD="$(bw list items --search 'dd-cli/access-token' --session "$BW_SESSION" 2>/dev/null | jq -r '.[0].id' | head -1 || true)"
if [ -n "${OLD:-}" ] && [ "$OLD" != null ] && [ "$OLD" != "" ]; then
  bw delete item "$OLD" --session "$BW_SESSION" >/dev/null
fi
python3 - "$TOK" "$V_ORG" "$V_COL" <<'PY' > /tmp/ddc.json
import json, sys
tok, org, col = sys.argv[1], sys.argv[2], sys.argv[3]
d = {"type":2, "name":"dd-cli/access-token", "notes":tok, "secureNote":{"type":0}}
if org: d["organizationId"]=org
if col: d["collectionIds"]=[col]
print(json.dumps(d))
PY
base64 -w0 /tmp/ddc.json | bw create item --session "$BW_SESSION" >/dev/null
rm -f /tmp/ddc.json

# 3) Point the forge-api process at the new token.
echo "==> updating $ENV_FILE ..."
if grep -q '^DD_CLI_ACCESS_TOKEN=' "$ENV_FILE"; then
  sed -i "s|^DD_CLI_ACCESS_TOKEN=.*|DD_CLI_ACCESS_TOKEN=$TOK|" "$ENV_FILE"
else
  printf 'DD_CLI_ACCESS_TOKEN=%s\n' "$TOK" >> "$ENV_FILE"
fi
chmod 600 "$ENV_FILE"
unset TOK

# 4) Restart forge-api so every new sandbox bash call gets the fresh token.
echo "==> restarting forge-api ..."
systemctl restart forge-api

echo "done: dd-cli token rotated (vault + $ENV_FILE + forge-api)."
echo "Sanity: run inside a forge sandbox: dd-cli --json-output find-nearby-stores --intent x"

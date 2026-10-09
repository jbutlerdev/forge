#!/usr/bin/env bash
# Herd H5.2 — wake-matrix acceptance scenario (MANUAL lab tool).
#
# WHAT THIS IS
#   A MANUAL acceptance script for the lab/dev fleet. It is NOT a unit
#   test and it is not run by CI: it drives the three H5.2 wake paths
#   end-to-end against a LIVE mule and a LIVE forge, then prints
#   PASS/FAIL per hop with the wake_log evidence. Run it from a host
#   that can reach both services.
#
# PREREQUISITES (on the target fleet)
#   1. Mule and forge up and reachable from this host.
#   2. The FORGE host has the H5.2 turn-end forwarder enabled for
#      AGENT_1 (so hop 2 fires):
#        FORGE_TURNEND_MULE_BASE / FORGE_TURNEND_MULE_KEY
#        FORGE_TURNEND_MULE_AGENTS=AGENT_1   (or unset = all agents)
#   3. The MULE host has the H4.5 memory-trigger forwarder lane
#      enabled for AGENT_2 (so hop 3 fires):
#        FORGE_TRIGGER_BASE / FORGE_TRIGGER_KEY
#        FORGE_TRIGGER_AGENTS=AGENT_2
#   4. Three agents (AGENT_1, AGENT_2, AGENT_3) owned by the key's user.
#   5. curl + jq on this host.
#
# ENV VARS (all required)
#   FORGE_BASE    forge API base, e.g. http://forge:8080/api/v1
#   FORGE_KEY     forge API key (sk_forge_…)
#   MULE_BASE     mule API base,   e.g. http://mule:8090/api/v1
#   MULE_KEY      mule API key (sk_mule_…)
#   AGENT_1       forge agent UUID — the webhook-wake target
#   AGENT_2       forge agent UUID — the event-wake target + episode writer
#   AGENT_3       forge agent UUID — the memory-wake target
#
# THE THREE HOPS (PLAN-HERD §H5.2 acceptance)
#   hop 1 — webhook wake → agent turn:
#           create a mule WEBHOOK wake (spec.source=herd-h52, tag
#           h52) targeting AGENT_1's forge conversation; fire it with
#           POST /api/v1/wakes/fire; assert a wake_log row lands and
#           the rendered prompt reaches AGENT_1's active conversation.
#   hop 2 — turn_end → mule event wake:
#           a pre-created mule EVENT wake (spec.type=
#           agent.turn_ended) targets AGENT_2. When AGENT_1's hop-1
#           turn ends, forge's forwarder POSTs
#           {kind:"event", source:"agent.turn_ended"} to mule; assert
#           the event wake's wake_log row with payload.agent_id =
#           AGENT_1.
#   hop 3 — memory trigger → agent wake:
#           AGENT_2's hop-2 wake prompt contains the watch token in an
#           explicit-feedback sentence ("Actually, … <token> …"), so
#           AGENT_2's H4.2 episode capture deterministically carries
#           it. AGENT_2 holds an ACTIVE belief with a watch
#           {match:<token>, wake_id:<AGENT_3's memory wake>} (v1 scans
#           the episode OWNER's active watch beliefs; the wake target
#           may be any agent's conversation). When AGENT_2's hop-2
#           turn ends: watch scan → memory_trigger_queue row → the
#           mule forwarder lane (30 s poll) fires AGENT_3's memory
#           wake → assert the queue row + AGENT_3's wake_log row.
#
# Usage:
#   FORGE_BASE=… FORGE_KEY=… MULE_BASE=… MULE_KEY=… \
#   AGENT_1=… AGENT_2=… AGENT_3=… \
#   scripts/herd-wake-scenario.sh
#
# Exit code: 0 iff all three hops PASS. Wakes created by this run are
# deleted on exit (best-effort) so repeated runs stay clean.

set -euo pipefail

# --------------------------------------------------------------- config
: "${FORGE_BASE:?set FORGE_BASE (forge API base)}"
: "${FORGE_KEY:?set FORGE_KEY (forge API key)}"
: "${MULE_BASE:?set MULE_BASE (mule API base)}"
: "${MULE_KEY:?set MULE_KEY (mule API key)}"
: "${AGENT_1:?set AGENT_1 (forge agent UUID)}"
: "${AGENT_2:?set AGENT_2 (forge agent UUID)}"
: "${AGENT_3:?set AGENT_3 (forge agent UUID)}"
FORGE_BASE="${FORGE_BASE%/}"
MULE_BASE="${MULE_BASE%/}"

# Polling budgets (the LLM turns are the slow part, not the wakes).
HOP1_DEADLINE=60     # webhook fire → prompt in agent-1's conversation
HOP2_DEADLINE=180    # agent-1 turn end → event wake log row
HOP3_DEADLINE=240    # agent-2 turn end → queue row → 30s-poll fire

command -v curl >/dev/null
command -v jq >/dev/null

CREATED_WAKES=()
CREATED_BELIEF=""

cleanup() {
  for w in "${CREATED_WAKES[@]}"; do
    curl -sS --max-time 10 -X DELETE -H "Authorization: Bearer ${MULE_KEY}" \
      "${MULE_BASE}/wakes/${w}" >/dev/null 2>&1 || true
  done
  if [[ -n "${CREATED_BELIEF}" ]]; then
    curl -sS --max-time 10 -X POST -H "X-API-Key: ${FORGE_KEY}" \
      "${FORGE_BASE}/agents/${AGENT_2}/memory/beliefs/${CREATED_BELIEF}/forget" \
      >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# --------------------------------------------------------------- helpers
mule() { # mule METHOD PATH [JSON]
  curl -sS --max-time 15 -X "$1" -H "Authorization: Bearer ${MULE_KEY}" \
    -H 'Content-Type: application/json' \
    ${3:+-d "$3"} "${MULE_BASE}$2"
}
forge() { # forge METHOD PATH [JSON]
  curl -sS --max-time 15 -X "$1" -H "X-API-Key: ${FORGE_KEY}" \
    -H 'Content-Type: application/json' \
    ${3:+-d "$3"} "${FORGE_BASE}$2"
}

die() { echo "FATAL: $*" >&2; exit 1; }

# wait_for DEADLINE_S LABEL CMD… — run CMD every 2 s until it exits 0.
wait_for() {
  local deadline=$1 label=$2; shift 2
  local start now
  start=$(date +%s)
  while :; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    now=$(date +%s)
    if (( now - start >= deadline )); then
      echo "  … timeout after ${deadline}s: ${label}"
      return 1
    fi
    sleep 2
  done
}

result() { # result PASS|FAIL HOP TEXT
  if [[ "$1" == PASS ]]; then
    echo "  ${1}: ${3}"
  else
    echo "  ${1}: ${3}"
    HOP_FAILED=1
  fi
}

HOP_FAILED=0

# agent's active conversation id ("" when none)
active_conv() {
  forge GET "/agents/$1/active" | jq -r '.current_conversation // empty'
}

# mule wake W has an ok log row (optionally whose payload.agent_id = A)?
wake_log_ok() {
  local w=$1 a=${2:-}
  if [[ -n "$a" ]]; then
    mule GET "/wakes/${w}/log" | jq -e --arg a "$a" \
      '[.log[]? | select(.ok==true and (.payload.agent_id? == $a))] | length > 0'
  else
    mule GET "/wakes/${w}/log" | jq -e '[.log[]? | select(.ok==true)] | length > 0'
  fi
}

show_log() {
  echo "  wake_log evidence ($1):"
  mule GET "/wakes/$1/log" | jq -r \
    '.log[]? | "    fired_at=\(.fired_at // "?") ok=\(.ok) error=\(.error // "none") payload=\(.payload | tostring)"'
}

# --------------------------------------------------------------- preflight
echo "=== Herd H5.2 wake-matrix scenario (manual lab tool) ==="
echo "forge=${FORGE_BASE}  mule=${MULE_BASE}"
echo "agent1=${AGENT_1}  agent2=${AGENT_2}  agent3=${AGENT_3}"

curl -sS --max-time 10 "${MULE_BASE%/api/v1}/health" >/dev/null 2>&1 || true
forge GET "/agents/${AGENT_1}" >/dev/null || die "cannot reach forge /agents/${AGENT_1} (check FORGE_BASE/FORGE_KEY)"
mule GET /wakes >/dev/null || die "cannot reach mule /wakes (check MULE_BASE/MULE_KEY)"
echo "preflight ok"

# Deterministic watch token for hop 3.
TOKEN="HERDH52-$(date +%s)-$RANDOM"
echo "watch token: ${TOKEN}"
echo

# ------------------------------------------------------- wake + belief setup
echo "--- setup: create the three wakes + agent-2's watch belief"

# hop 1's webhook wake → agent-1.
H1_JSON=$(jq -n --arg f "$FORGE_BASE" --arg k "$FORGE_KEY" --arg a "$AGENT_1" \
  '{name:"herd-h52-hop1", kind:"webhook",
    spec:{source:"herd-h52", tag:"h52"},
    target:{forge_conversation:{forge_url:$f, api_key:$k, agent_id:$a}},
    prompt_template:"H52-HOP1-MARKER: acknowledge this webhook wake in one line."}')
H1_WAKE=$(mule POST /wakes "$H1_JSON" | jq -r '.id // empty')
[[ -n "$H1_WAKE" ]] || die "setup: could not create hop-1 webhook wake"
CREATED_WAKES+=("$H1_WAKE")
echo "  hop-1 webhook wake:   ${H1_WAKE}"

# hop 2's event wake → agent-2. Its prompt embeds the watch token in an
# "Actually …" sentence so the H4.2 capture's explicit-feedback
# extraction carries it into agent-2's episode deterministically.
H2_JSON=$(jq -n --arg f "$FORGE_BASE" --arg k "$FORGE_KEY" --arg a "$AGENT_2" --arg t "$TOKEN" \
  '{name:"herd-h52-hop2", kind:"event", spec:{type:"agent.turn_ended"},
    target:{forge_conversation:{forge_url:$f, api_key:$k, agent_id:$a}},
    prompt_template:("H52-HOP2-MARKER: a forge turn just ended. " +
      "Actually, record the marker \($t) in your episode notes. " +
      "Summarize the turn-end payload in one line.")}')
H2_WAKE=$(mule POST /wakes "$H2_JSON" | jq -r '.id // empty')
[[ -n "$H2_WAKE" ]] || die "setup: could not create hop-2 event wake"
CREATED_WAKES+=("$H2_WAKE")
echo "  hop-2 event wake:     ${H2_WAKE}"

# hop 3's memory wake → agent-3.
H3_JSON=$(jq -n --arg f "$FORGE_BASE" --arg k "$FORGE_KEY" --arg a "$AGENT_3" \
  '{name:"herd-h52-hop3", kind:"memory",
    spec:{agent_id:$a, predicate:"herd h52 scenario watch"},
    target:{forge_conversation:{forge_url:$f, api_key:$k, agent_id:$a}},
    prompt_template:"H52-HOP3-MARKER: a memory watch matched; summarize the payload in one line."}')
H3_WAKE=$(mule POST /wakes "$H3_JSON" | jq -r '.id // empty')
[[ -n "$H3_WAKE" ]] || die "setup: could not create hop-3 memory wake"
CREATED_WAKES+=("$H3_WAKE")
echo "  hop-3 memory wake:    ${H3_WAKE}"

# agent-2's ACTIVE watch belief (proposals → pending → keep). v1 scans
# the episode owner's own active watch beliefs, so the watch lives on
# the agent whose episode it matches (agent-2 here); its wake target
# is agent-3's conversation.
B_JSON=$(jq -n --arg t "$TOKEN" --arg w "$H3_WAKE" \
  '{beliefs:[{content:("H5.2 scenario watch: episodes containing the marker \($t)."),
              kind:"fact",
              watch:{match:$t, cooldown_hours:0.001, wake_id:$w}}]}')
CREATED_BELIEF=$(forge POST "/agents/${AGENT_2}/memory/beliefs/proposals" "$B_JSON" \
  | jq -r '.results[0].belief_id // empty')
[[ -n "$CREATED_BELIEF" ]] || die "setup: watch belief not accepted on agent-2"
forge POST "/agents/${AGENT_2}/memory/beliefs/${CREATED_BELIEF}/keep" >/dev/null \
  || die "setup: could not keep the watch belief active"
echo "  agent-2 watch belief: ${CREATED_BELIEF} (active)"
echo

# ------------------------------------------------------- hop 1: webhook → agent-1
echo "--- hop 1: webhook wake → agent-1's turn"
CONV1_BEFORE=$(active_conv "$AGENT_1")

H1_FIRE=$(mule POST /wakes/fire \
  '{"kind":"webhook","source":"herd-h52","event_tag":"h52"}')
H1_N=$(jq -r '.fired // 0' <<<"$H1_FIRE")
if [[ "$H1_N" -ge 1 ]]; then
  echo "  fire response: fired=${H1_N}"
else
  show_log "$H1_WAKE"
  result FAIL 1 "webhook fire matched no wakes (fired=$(jq -c . <<<"$H1_FIRE"))"
fi

h1_prompt_delivered() {
  local conv=${1:-}
  [[ -n "$conv" ]] || conv=$(active_conv "$AGENT_1")
  [[ -n "$conv" ]] || return 1
  forge GET "/messages?session_id=${conv}" | jq -e \
    '[.messages[]? | select(.role=="user")] | any(.content | tostring | contains("H52-HOP1-MARKER"))'
}
if wait_for "$HOP1_DEADLINE" "hop-1 prompt in agent-1's conversation" \
    h1_prompt_delivered "${CONV1_BEFORE:-}"; then
  result PASS 1 "webhook fired; H52-HOP1-MARKER prompt reached agent-1's conversation"
else
  show_log "$H1_WAKE"
  result FAIL 1 "prompt never reached agent-1's conversation (wake fire may have failed — see wake_log above)"
fi
echo

# ------------------------------------------------------- hop 2: turn_end → event wake
echo "--- hop 2: agent.turn_ended → agent-2's event wake"
if wait_for "$HOP2_DEADLINE" "hop-2 wake_log row (agent-1 turn end)" \
    wake_log_ok "$H2_WAKE" "$AGENT_1"; then
  show_log "$H2_WAKE"
  result PASS 2 "agent.turn_ended fired agent-2's event wake (payload.agent_id = agent-1)"
else
  show_log "$H2_WAKE"
  result FAIL 2 "agent-1's turn end never fired the event wake (check FORGE_TURNEND_* on the forge host)"
fi
echo

# ------------------------------------------------------- hop 3: memory trigger → agent-3
echo "--- hop 3: agent-2's episode → memory trigger → agent-3's wake"
h3_queue_row() {
  forge GET "/agents/${AGENT_2}/memory/triggers/pending" | jq -e \
    --arg t "$TOKEN" \
    '[.triggers[]? | select(.payload.watch.match? == $t)] | length > 0'
}
if wait_for "$HOP3_DEADLINE" "hop-3 trigger-queue row" h3_queue_row; then
  echo "  trigger-queue row evidence:"
  forge GET "/agents/${AGENT_2}/memory/triggers/pending" \
    | jq -c --arg t "$TOKEN" '.triggers[]? | select(.payload.watch.match? == $t)'
else
  forge GET "/agents/${AGENT_2}/memory/beliefs" | jq '.beliefs // .' | head -5
  result FAIL 3 "no trigger-queue row for the watch token (episode capture or watch scan failed)"
fi

if wait_for "$HOP3_DEADLINE" "hop-3 memory-wake fire (mule forwarder, 30 s poll)" \
    wake_log_ok "$H3_WAKE"; then
  show_log "$H3_WAKE"
  result PASS 3 "memory trigger fired agent-3's memory wake via the mule forwarder lane"
else
  show_log "$H3_WAKE"
  result FAIL 3 "queue row never fired (check FORGE_TRIGGER_* on the mule host; the lane must list AGENT_2)"
fi
echo

# ------------------------------------------------------- summary
echo "=== summary ==="
if [[ "$HOP_FAILED" -eq 0 ]]; then
  echo "ALL THREE HOPS PASS"
  exit 0
else
  echo "ONE OR MORE HOPS FAILED"
  exit 1
fi

#!/usr/bin/env bash
# Forge API Test Script
# 
# Tests the Forge API endpoints
# Run: bash scripts/test-api.sh

set -e

FORGE_API_URL="${FORGE_API_URL:-http://localhost:8080/api/v1}"

echo "========================================"
echo "Forge API Test"
echo "========================================"
echo "API URL: $FORGE_API_URL"
echo ""

# Test health endpoint
echo "[1/8] Testing health endpoint..."
HEALTH=$(curl -s -o /dev/null -w "%{http_code}" "$FORGE_API_URL/health")
if [ "$HEALTH" = "200" ]; then
    echo "  ✓ Health check passed (HTTP $HEALTH)"
else
    echo "  ✗ Health check failed (HTTP $HEALTH)"
    exit 1
fi

# Test metrics endpoint
echo ""
echo "[2/8] Testing metrics endpoint..."
METRICS=$(curl -s "$FORGE_API_URL/metrics")
if echo "$METRICS" | jq -e '.metrics' > /dev/null 2>&1; then
    echo "  ✓ Metrics endpoint working"
else
    echo "  ✗ Metrics endpoint failed"
fi

# Test create profile
echo ""
echo "[3/8] Testing profile creation..."
PROFILE=$(curl -s -X POST "$FORGE_API_URL/profiles" \
    -H "Content-Type: application/json" \
    -d '{
        "name": "test-agent",
        "provider": "anthropic",
        "model": "claude-sonnet-4-20250514",
        "working_dir": "/tmp/forge-test",
        "system_prompt": "You are a helpful assistant."
    }')

PROFILE_ID=$(echo "$PROFILE" | jq -r '.profile.id // empty' 2>/dev/null)
if [ -n "$PROFILE_ID" ]; then
    echo "  ✓ Profile created (ID: $PROFILE_ID)"
else
    echo "  ✗ Profile creation failed"
    echo "  Response: $PROFILE"
    exit 1
fi

# Test list profiles
echo ""
echo "[4/8] Testing list profiles..."
PROFILES=$(curl -s "$FORGE_API_URL/profiles")
COUNT=$(echo "$PROFILES" | jq -r '.profiles | length' 2>/dev/null)
if [ "$COUNT" -gt 0 ] 2>/dev/null; then
    echo "  ✓ List profiles working ($COUNT profile(s))"
else
    echo "  ✗ List profiles failed"
fi

# Test create session
echo ""
echo "[5/8] Testing session creation..."
SESSION=$(curl -s -X POST "$FORGE_API_URL/sessions" \
    -H "Content-Type: application/json" \
    -d "{\"profile_id\": \"$PROFILE_ID\", \"title\": \"Test Session\"}")

SESSION_ID=$(echo "$SESSION" | jq -r '.session.id // empty' 2>/dev/null)
WORKING_DIR=$(echo "$SESSION" | jq -r '.working_dir // empty' 2>/dev/null)
if [ -n "$SESSION_ID" ]; then
    echo "  ✓ Session created (ID: $SESSION_ID)"
    echo "  Working dir: $WORKING_DIR"
else
    echo "  ✗ Session creation failed"
    echo "  Response: $SESSION"
    exit 1
fi

# Test get session status
echo ""
echo "[6/8] Testing session status..."
STATUS=$(curl -s "$FORGE_API_URL/sessions/$SESSION_ID/status")
if echo "$STATUS" | jq -e '.status' > /dev/null 2>&1; then
    echo "  ✓ Session status working"
    echo "  Active: $(echo "$STATUS" | jq -r '.status.active')"
    echo "  Agent: $(echo "$STATUS" | jq -r '.status.has_agent')"
else
    echo "  ✗ Session status failed"
fi

# Test tool execution (if session is active)
echo ""
echo "[7/8] Testing tool execution..."
TOOL_RESULT=$(curl -s -X POST "$FORGE_API_URL/tools/execute" \
    -H "Content-Type: application/json" \
    -d "{
        \"session_id\": \"$SESSION_ID\",
        \"tool\": \"bash\",
        \"input\": {\"command\": \"echo hello\"},
        \"tool_call_id\": \"test-123\"
    }")

if echo "$TOOL_RESULT" | jq -e '.success' > /dev/null 2>&1; then
    echo "  ✓ Tool execution working"
    echo "  Output: $(echo "$TOOL_RESULT" | jq -r '.output // empty')"
else
    echo "  ⚠ Tool execution returned unexpected result"
    echo "  Response: $TOOL_RESULT"
fi

# Test session resume
echo ""
echo "[8/8] Testing session resume..."
RESUME=$(curl -s -X POST "$FORGE_API_URL/sessions/$SESSION_ID/resume")
if echo "$RESUME" | jq -e '.resumed' > /dev/null 2>&1; then
    echo "  ✓ Session resume working"
else
    echo "  ⚠ Session resume returned unexpected result"
fi

# Cleanup
echo ""
echo "[Cleanup] Deleting test session and profile..."
curl -s -X DELETE "$FORGE_API_URL/sessions/delete?id=$SESSION_ID" > /dev/null
curl -s -X DELETE "$FORGE_API_URL/profiles/delete?id=$PROFILE_ID" > /dev/null
echo "  ✓ Cleanup complete"

echo ""
echo "========================================"
echo "All tests passed!"
echo "========================================"

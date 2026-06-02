#!/usr/bin/env bash
# Common functions for Forge CLI

FORGE_API_URL="${FORGE_API_URL:-http://localhost:8080}"
export FORGE_API_URL

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

error() { echo -e "${RED}Error: $1${NC}" >&2; exit 1; }
success() { echo -e "${GREEN}$1${NC}"; }
warn() { echo -e "${YELLOW}Warning: $1${NC}"; }

api_get() {
    local -a auth_args=()
    if [ -n "$FORGE_API_KEY" ]; then
        auth_args=(-H "X-API-Key: $FORGE_API_KEY")
    fi
    curl -s -X GET "${FORGE_API_URL}$1" -H "Content-Type: application/json" "${auth_args[@]}" -w "|%{http_code}"
}

api_post() {
    local -a auth_args=()
    if [ -n "$FORGE_API_KEY" ]; then
        auth_args=(-H "X-API-Key: $FORGE_API_KEY")
    fi
    curl -s -X POST "${FORGE_API_URL}$1" -H "Content-Type: application/json" "${auth_args[@]}" -d "$2" -w "|%{http_code}"
}

api_patch() {
    local -a auth_args=()
    if [ -n "$FORGE_API_KEY" ]; then
        auth_args=(-H "X-API-Key: $FORGE_API_KEY")
    fi
    curl -s -X PATCH "${FORGE_API_URL}$1" -H "Content-Type: application/json" "${auth_args[@]}" -d "$2" -w "|%{http_code}"
}

api_delete() {
    local -a auth_args=()
    if [ -n "$FORGE_API_KEY" ]; then
        auth_args=(-H "X-API-Key: $FORGE_API_KEY")
    fi
    curl -s -X DELETE "${FORGE_API_URL}$1" -H "Content-Type: application/json" "${auth_args[@]}" -w "|%{http_code}"
}

is_success() { [[ "$1" =~ ^2[0-9][0-9]$ ]]; }

pretty_json() {
    if command -v jq &> /dev/null; then
        jq .
    else
        cat
    fi
}

show_help() {
    cat << 'EOF'
Forge - AI Agent Platform (API Server)

Usage: forge <command> [options]

NOTE: This CLI is an example client. Forge is primarily an API server.
      Build your own client or use curl/httpx to interact with the API.

Commands:
  Auth:
    register <email> <name> <password>  Create account
    login <email> <password>            Login

  Profiles:
    profile create <name> [opts]       Create profile
    profile list                        List profiles
    profile get <id>                    Get profile details
    profile update <id> [opts]          Update profile
    profile delete <id>                Delete profile

  Sessions:
    session create <profile_id>        Create session
    session list                        List sessions
    session get <id>                    Get session details
    session delete <id>                 Delete session

  Messages:
    message send <session_id> <text>    Send a message to the agent
    message ask <session_id> <text>     Send a message and stream the response
    message watch <session_id>          Stream new messages from a session
    message list <session_id>           List all messages in a session
    messages <session_id>               List messages in session (alias)

  Utilities:
    health                              Check API health
    status                              Show API status
    metrics                             Show API metrics

Profile options:
    --provider <provider>               anthropic, openai, proxy-anthropic
    --model <model>                     Model name
    --working-dir <path>                Working directory
    --base-url <url>                    API base URL (for proxies)
    --api-key <key>                     API key
    --system-prompt <text>              System prompt
    --nix-shell <expr>                  Nix shell expression

Environment:
    FORGE_API_URL                       API URL (default: http://localhost:8080)
    FORGE_API_KEY                        API key for authentication

Quick Start:
    # Register and get API key
    forge register user@example.com "John" password123
    export FORGE_API_KEY='your-api-key'
    
    # Create profile
    forge profile create my-agent \
        --provider anthropic \
        --model claude-sonnet-4-20250514 \
        --working-dir /tmp/my-project
    
    # Create session
    forge session create <profile-id>
    
    # Send messages to the session
    # (The API spawns pi to process each message)

API Base URL: http://localhost:8080

For more details on the API, see AGENTS.md
EOF
}

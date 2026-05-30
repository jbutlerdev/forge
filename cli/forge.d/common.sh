#!/usr/bin/env bash
# Common functions for Forge CLI

# Make sure FORGE_API_URL is set
FORGE_API_URL="${FORGE_API_URL:-http://localhost:8080/api/v1}"
export FORGE_API_URL

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print error and exit
error() {
    echo -e "${RED}Error: $1${NC}" >&2
    exit 1
}

# Print success
success() {
    echo -e "${GREEN}$1${NC}"
}

# Print warning
warn() {
    echo -e "${YELLOW}Warning: $1${NC}"
}

# Make API request
api_get() {
    local auth_header=""
    if [ -n "$FORGE_API_KEY" ]; then
        auth_header="-H 'X-API-Key: $FORGE_API_KEY'"
    fi
    curl -s -X GET "${FORGE_API_URL}$1" \
        -H "Content-Type: application/json" \
        $auth_header \
        -w "|%{http_code}"
}

api_post() {
    local auth_header=""
    if [ -n "$FORGE_API_KEY" ]; then
        auth_header="-H 'X-API-Key: $FORGE_API_KEY'"
    fi
    curl -s -X POST "${FORGE_API_URL}$1" \
        -H "Content-Type: application/json" \
        $auth_header \
        -d "$2" \
        -w "|%{http_code}"
}

api_patch() {
    local auth_header=""
    if [ -n "$FORGE_API_KEY" ]; then
        auth_header="-H 'X-API-Key: $FORGE_API_KEY'"
    fi
    curl -s -X PATCH "${FORGE_API_URL}$1" \
        -H "Content-Type: application/json" \
        $auth_header \
        -d "$2" \
        -w "|%{http_code}"
}

api_delete() {
    local auth_header=""
    if [ -n "$FORGE_API_KEY" ]; then
        auth_header="-H 'X-API-Key: $FORGE_API_KEY'"
    fi
    curl -s -X DELETE "${FORGE_API_URL}$1" \
        -H "Content-Type: application/json" \
        $auth_header \
        -w "|%{http_code}"
}

# Parse response - extracts body and status code
# Usage: parse_response "$(api_get /profiles)"
parse_response() {
    local response="$1"
    local http_code
    local body
    
    # Extract HTTP code (after |)
    http_code="${response##*|}"
    # Extract body (before |)
    body="${response%|*}"
    
    echo "$http_code|$body"
}

# Check if response is success (2xx)
is_success() {
    local http_code="$1"
    [[ "$http_code" =~ ^2[0-9][0-9]$ ]]
}

# Pretty print JSON
pretty_json() {
    if command -v jq &> /dev/null; then
        jq .
    else
        cat
    fi
}

# Show help
show_help() {
    cat << 'EOF'
Forge - AI Agent Platform

Usage: forge <command> [options]

Commands:
  Authentication:
    register <email> <name> <password>
                              Create a new account
    login <email> <password>  Authenticate with the API

  Profiles:
    profile create <name> [opts]  Create a new profile
    profile list                 List all profiles
    profile get <id>             Get profile details
    profile update <id> [opts]   Update a profile
    profile delete <id>          Delete a profile

  Sessions:
    session create <profile_id> [opts]  Create a new session
    session list [--profile-id <id>]    List sessions
    session get <id>                    Get session details
    session delete <id>                 Delete a session
    session status <id>                 Get session status (agent, sandbox)
    session resume <id>                 Resume a session

  Messages:
    send <session_id> <msg>     Send a message to a session
    messages <session_id>       List messages in a session

  Utilities:
    health                      Check API health
    status                      Show API status and stats
    metrics                     Show API metrics and statistics

Options:
    -h, --help                  Show this help message

Profile options:
    --provider <openai|anthropic>
    --model <model-name>
    --working-dir <path>
    --git-url <url>
    --git-ref <ref>
    --nix-shell <expr|path>
    --system-prompt <text>
    --api-key <key>

Session options:
    --title <title>

Environment:
    FORGE_API_URL              API base URL (default: http://localhost:8080/api/v1)
    FORGE_API_KEY              API key for authentication

Quick Start:
    forge register user@example.com "John" password123
    forge profile create my-agent --provider anthropic --model claude-sonnet-4-20250514
    forge session create <profile_id>
    forge send <session_id> "Hello, world!"
    forge messages <session_id>
EOF
}

# Parse key-value arguments
parse_kv_args() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --*)
                local key="${1#--}"
                local value="$2"
                if [[ "$value" == --* ]] || [ -z "$value" ]; then
                    echo "Error: --$key requires a value"
                    return 1
                fi
                echo "${key}=${value}"
                shift 2
                ;;
            *)
                echo "positional=$1"
                shift
                ;;
        esac
    done
}

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
    curl -s -X GET "${FORGE_API_URL}$1" \
        -H "Content-Type: application/json" \
        -w "|%{http_code}"
}

api_post() {
    curl -s -X POST "${FORGE_API_URL}$1" \
        -H "Content-Type: application/json" \
        -d "$2" \
        -w "|%{http_code}"
}

api_patch() {
    curl -s -X PATCH "${FORGE_API_URL}$1" \
        -H "Content-Type: application/json" \
        -d "$2" \
        -w "|%{http_code}"
}

api_delete() {
    curl -s -X DELETE "${FORGE_API_URL}$1" \
        -H "Content-Type: application/json" \
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
    profile                    Manage profiles
        create <name> [opts]   Create a new profile
        list                   List all profiles
        get <id>               Get profile details
        update <id> [opts]     Update a profile
        delete <id>            Delete a profile

    session                    Manage sessions
        create <profile_id> [opts]  Create a new session
        list [--profile-id <id>]    List sessions
        get <id>               Get session details
        delete <id>            Delete a session
        status <id>            Get session status (agent, sandbox)
        resume <id>             Resume a session

    send <session_id> <msg>   Send a message to a session
    messages <session_id>     List messages in a session
    metrics                   Show API metrics and statistics

Options:
    -h, --help                Show this help message

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
    FORGE_API_URL             API base URL (default: http://localhost:8080/api/v1)

Examples:
    forge profile create my-agent --provider openai --model gpt-4o --working-dir /tmp/test
    forge session create <profile_id> --title "My session"
    forge session status <session_id>
    forge session resume <session_id>
    forge send <session_id> "Read the main.rs file and explain it"
    forge messages <session_id>
    forge metrics
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

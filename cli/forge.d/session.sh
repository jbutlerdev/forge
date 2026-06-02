#!/usr/bin/env bash
# Session management commands

cmd_session() {
    local subcommand="${1:-}"
    shift || true
    
    case "$subcommand" in
        create)
            cmd_session_create "$@"
            ;;
        list)
            cmd_session_list "$@"
            ;;
        get)
            cmd_session_get "$@"
            ;;
        delete)
            cmd_session_delete "$@"
            ;;
        -h|--help|help)
            cat << 'EOF'
forge session - Manage sessions

Usage:
    forge session create <profile_id> [--title <title>]
    forge session list [--profile-id <id>]
    forge session get <id>
    forge session delete <id>
EOF
            ;;
        *)
            echo "Unknown session command: $subcommand"
            echo "Usage: forge session <create|list|get|delete>"
            exit 1
            ;;
    esac
}

cmd_session_create() {
    local profile_id="$1"
    [ -z "$profile_id" ] && error "Profile ID is required"
    shift
    
    local title=""
    
    while [ $# -gt 0 ]; do
        case "$1" in
            --title) title="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    
    local payload=$(echo '{}' | jq --arg profile_id "$profile_id" '. + {profile_id: $profile_id}')
    [ -n "$title" ] && payload=$(echo "$payload" | jq --arg v "$title" '. + {title: $v}')
    
    local response
    response=$(api_post "/sessions" "$payload")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        success "Session created successfully"
        echo "$body" | pretty_json
    else
        error "Failed to create session: $body"
    fi
}

cmd_session_list() {
    local response
    response=$(api_get "/sessions")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        echo "$body" | pretty_json
    else
        error "Failed to list sessions: $body"
    fi
}

cmd_session_get() {
    local id="$1"
    [ -z "$id" ] && error "Session ID is required"
    
    local response
    response=$(api_get "/sessions/get?id=$id")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        echo "$body" | pretty_json
    else
        error "Failed to get session: $body"
    fi
}

cmd_session_delete() {
    local id="$1"
    [ -z "$id" ] && error "Session ID is required"
    
    local response
    response=$(api_delete "/sessions/delete?id=$id")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        success "Session deleted successfully"
    else
        error "Failed to delete session: $body"
    fi
}

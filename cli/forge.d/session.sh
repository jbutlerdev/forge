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
        status)
            cmd_session_status "$@"
            ;;
        resume)
            cmd_session_resume "$@"
            ;;
        -h|--help|help)
            cat << 'EOF'
forge session - Manage sessions

Usage:
    forge session create <profile_id> [options]
    forge session list [--profile-id <id>]
    forge session get <id>
    forge session delete <id>
    forge session status <id>
    forge session resume <id>

Options for create:
    --title <title>

Commands:
    create      Create a new session
    list        List all sessions
    get         Get session details
    delete      Delete a session
    status      Get session status (agent, sandbox, active state)
    resume      Resume a session (recreate pi agent with message history)
EOF
            ;;
        *)
            echo "Unknown session command: $subcommand"
            echo "Usage: forge session <create|list|get|delete|status|resume>"
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
    
    # Build JSON payload
    local payload=$(echo '{}' | jq --arg profile_id "$profile_id" '. + {profile_id: $profile_id}')
    [ -n "$title" ] && payload=$(echo "$payload" | jq --arg v "$title" '. + {title: $v}')
    
    local response
    response=$(api_post "/sessions" "$payload")
    
    local http_code="${response##*|}";
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        success "Session created successfully"
        echo "$body" | pretty_json
    else
        error "Failed to create session: $body"
    fi
}

cmd_session_list() {
    local profile_id=""
    
    while [ $# -gt 0 ]; do
        case "$1" in
            --profile-id) profile_id="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    
    local endpoint
    if [ -n "$profile_id" ]; then
        endpoint="/profiles/$profile_id/sessions"
    else
        endpoint="/sessions"
    fi
    
    local response
    response=$(api_get "$endpoint")
    
    local http_code="${response##*|}";
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
    
    # Use query param endpoint for better compatibility
    local response
    response=$(api_get "/sessions/get?id=$id")
    
    local http_code="${response##*|}";
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
    
    # Use query param endpoint for better compatibility
    local response
    response=$(api_delete "/sessions/delete?id=$id")
    
    local http_code="${response##*|}";
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        success "Session deleted successfully"
    else
        error "Failed to delete session: $body"
    fi
}

cmd_session_status() {
    local id="$1"
    [ -z "$id" ] && error "Session ID is required"
    
    local response
    response=$(api_get "/sessions/$id/status")
    
    local http_code="${response##*|}";
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        echo "Session Status:"
        echo "=============="
        
        local has_agent=$(echo "$body" | jq -r '.status.has_agent')
        local has_sandbox=$(echo "$body" | jq -r '.status.has_sandbox')
        local active=$(echo "$body" | jq -r '.status.active')
        local working_dir=$(echo "$body" | jq -r '.status.working_dir // "N/A"')
        local title=$(echo "$body" | jq -r '.status.title')
        local last_active=$(echo "$body" | jq -r '.status.last_active')
        
        echo "  ID:          $id"
        echo "  Title:       $title"
        echo "  Working Dir: $working_dir"
        echo "  Agent:       $( [ "$has_agent" = "true" ] && echo "Running" || echo "Not running" )"
        echo "  Sandbox:     $( [ "$has_sandbox" = "true" ] && echo "Active" || echo "Not created" )"
        echo "  Active:      $( [ "$active" = "true" ] && echo "Yes" || echo "No" )"
        echo "  Last Active: $last_active"
    else
        error "Failed to get session status: $body"
    fi
}

cmd_session_resume() {
    local id="$1"
    [ -z "$id" ] && error "Session ID is required"
    
    echo "Resuming session $id..."
    echo "This will recreate the pi agent and replay message history."
    
    local response
    response=$(api_post "/sessions/$id/resume" '{}')
    
    local http_code="${response##*|}";
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        local resumed=$(echo "$body" | jq -r '.resumed')
        local message=$(echo "$body" | jq -r '.message')
        
        if [ "$resumed" = "true" ]; then
            success "Session resumed: $message"
        else
            echo "$body" | pretty_json
        fi
    else
        error "Failed to resume session: $body"
    fi
}

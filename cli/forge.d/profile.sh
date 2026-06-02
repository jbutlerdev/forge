#!/usr/bin/env bash
# Profile management commands

cmd_profile() {
    local subcommand="${1:-}"
    shift || true
    
    case "$subcommand" in
        create)
            cmd_profile_create "$@"
            ;;
        list)
            cmd_profile_list
            ;;
        get)
            cmd_profile_get "$@"
            ;;
        update)
            cmd_profile_update "$@"
            ;;
        delete)
            cmd_profile_delete "$@"
            ;;
        -h|--help|help)
            cat << 'EOF'
forge profile - Manage profiles

Usage:
    forge profile create <name> [opts]
    forge profile list
    forge profile get <id>
    forge profile update <id> [opts]
    forge profile delete <id>

Options:
    --provider <provider>               anthropic, openai, proxy-anthropic
    --model <model>                     Model name
    --working-dir <path>                Working directory
    --base-url <url>                    API base URL
    --api-key <key>                     API key
    --system-prompt <text>              System prompt
    --nix-shell <expr>                  Nix shell expression
    --git-url <url>                     Git repository URL
    --git-ref <ref>                     Git branch/tag
EOF
            ;;
        *)
            echo "Unknown profile command: $subcommand"
            echo "Usage: forge profile <create|list|get|update|delete>"
            exit 1
            ;;
    esac
}

cmd_profile_create() {
    local name="$1"
    [ -z "$name" ] && error "Profile name is required"
    shift
    
    # Defaults
    local provider="anthropic"
    local model="claude-sonnet-4-20250514"
    local working_dir="/tmp"
    local git_url=""
    local git_ref=""
    local nix_shell=""
    local system_prompt="You are a helpful coding assistant."
    local api_key=""
    
    while [ $# -gt 0 ]; do
        case "$1" in
            --provider) provider="$2"; shift 2 ;;
            --model) model="$2"; shift 2 ;;
            --working-dir) working_dir="$2"; shift 2 ;;
            --git-url) git_url="$2"; shift 2 ;;
            --git-ref) git_ref="$2"; shift 2 ;;
            --nix-shell) nix_shell="$2"; shift 2 ;;
            --system-prompt) system_prompt="$2"; shift 2 ;;
            --api-key) api_key="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    
    # Build JSON payload
    local payload=$(jq -n \
        --arg name "$name" \
        --arg provider "$provider" \
        --arg model "$model" \
        --arg working_dir "$working_dir" \
        --arg system_prompt "$system_prompt" \
        '{
            name: $name,
            provider: $provider,
            model: $model,
            working_dir: $working_dir,
            system_prompt: $system_prompt
        }')
    
    [ -n "$git_url" ] && payload=$(echo "$payload" | jq --arg v "$git_url" '. + {git_url: $v}')
    [ -n "$git_ref" ] && payload=$(echo "$payload" | jq --arg v "$git_ref" '. + {git_ref: $v}')
    [ -n "$nix_shell" ] && payload=$(echo "$payload" | jq --arg v "$nix_shell" '. + {nix_shell: $v}')
    [ -n "$api_key" ] && payload=$(echo "$payload" | jq --arg v "$api_key" '. + {api_key: $v}')
    
    local response
    response=$(api_post "/profiles" "$payload")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        success "Profile created successfully"
        echo "$body" | pretty_json
    else
        error "Failed to create profile: $body"
    fi
}

cmd_profile_list() {
    local response
    response=$(api_get "/profiles")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        echo "$body" | pretty_json
    else
        error "Failed to list profiles: $body"
    fi
}

cmd_profile_get() {
    local id="$1"
    [ -z "$id" ] && error "Profile ID is required"
    
    local response
    response=$(api_get "/profiles/get?id=$id")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        echo "$body" | pretty_json
    else
        error "Failed to get profile: $body"
    fi
}

cmd_profile_update() {
    local id="$1"
    [ -z "$id" ] && error "Profile ID is required"
    shift
    
    local payload="{}"
    
    while [ $# -gt 0 ]; do
        case "$1" in
            --provider) payload=$(echo "$payload" | jq --arg v "$2" '. + {provider: $v}'); shift 2 ;;
            --model) payload=$(echo "$payload" | jq --arg v "$2" '. + {model: $v}'); shift 2 ;;
            --working-dir) payload=$(echo "$payload" | jq --arg v "$2" '. + {working_dir: $v}'); shift 2 ;;
            --git-url) payload=$(echo "$payload" | jq --arg v "$2" '. + {git_url: $v}'); shift 2 ;;
            --git-ref) payload=$(echo "$payload" | jq --arg v "$2" '. + {git_ref: $v}'); shift 2 ;;
            --nix-shell) payload=$(echo "$payload" | jq --arg v "$2" '. + {nix_shell: $v}'); shift 2 ;;
            --system-prompt) payload=$(echo "$payload" | jq --arg v "$2" '. + {system_prompt: $v}'); shift 2 ;;
            --api-key) payload=$(echo "$payload" | jq --arg v "$2" '. + {api_key: $v}'); shift 2 ;;
            *) shift ;;
        esac
    done
    
    local response
    response=$(api_patch "/profiles/update?id=$id" "$payload")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        success "Profile updated successfully"
        echo "$body" | pretty_json
    else
        error "Failed to update profile: $body"
    fi
}

cmd_profile_delete() {
    local id="$1"
    [ -z "$id" ] && error "Profile ID is required"
    
    local response
    response=$(api_delete "/profiles/delete?id=$id")
    
    local http_code="${response##*|}"
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        success "Profile deleted successfully"
    else
        error "Failed to delete profile: $body"
    fi
}

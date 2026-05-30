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
            cmd_profile_list "$@"
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
    forge profile create <name> [options]
    forge profile list
    forge profile get <id>
    forge profile update <id> [options]
    forge profile delete <id>

Options for create/update:
    --provider <openai|anthropic>
    --model <model-name>
    --working-dir <path>
    --git-url <url>
    --git-ref <ref>
    --nix-shell <expr|path>
    --system-prompt <text>
    --api-key <key>
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
    
    # Parse options
    local provider="openai"
    local model="gpt-4o"
    local working_dir="/workspace"
    local git_url=""
    local git_ref=""
    local nix_shell=""
    local system_prompt="You are a helpful coding assistant."
    local api_key=""
    local description=""
    
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
            --description) description="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    
    # Build JSON payload using jq for proper JSON handling
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
        }' \
        $([ -n "$description" ] && echo "--arg description \"$description\"") \
        $([ -n "$git_url" ] && echo "--arg git_url \"$git_url\"") \
        $([ -n "$git_ref" ] && echo "--arg git_ref \"$git_ref\"") \
        $([ -n "$nix_shell" ] && echo "--arg nix_shell \"$nix_shell\"") \
        $([ -n "$api_key" ] && echo "--arg api_key \"$api_key\"") \
    )
    
    # Add optional fields if present
    if [ -n "$description" ]; then
        payload=$(echo "$payload" | jq --arg v "$description" '. + {description: $v}')
    fi
    if [ -n "$git_url" ]; then
        payload=$(echo "$payload" | jq --arg v "$git_url" '. + {git_url: $v}')
    fi
    if [ -n "$git_ref" ]; then
        payload=$(echo "$payload" | jq --arg v "$git_ref" '. + {git_ref: $v}')
    fi
    if [ -n "$nix_shell" ]; then
        payload=$(echo "$payload" | jq --arg v "$nix_shell" '. + {nix_shell: $v}')
    fi
    if [ -n "$api_key" ]; then
        payload=$(echo "$payload" | jq --arg v "$api_key" '. + {api_key: $v}')
    fi
    
    local response
    response=$(api_post "/profiles" "$payload")
    
    local http_code="${response##*|}";
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
    
    local http_code="${response##*|}";
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
    # Try path param first, fallback to query param
    response=$(api_get "/profiles/get?id=$id")
    
    local http_code="${response##*|}";
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
    
    # Parse options (same as create)
    local provider="" model="" working_dir="" git_url="" git_ref=""
    local nix_shell="" system_prompt="" api_key="" description=""
    
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
            --description) description="$2"; shift 2 ;;
            *) shift ;;
        esac
    done
    
    # Build JSON with only provided fields
    local payload="{}"
    [ -n "$provider" ] && payload=$(echo "$payload" | jq --arg v "$provider" '. + {provider: $v}')
    [ -n "$model" ] && payload=$(echo "$payload" | jq --arg v "$model" '. + {model: $v}')
    [ -n "$working_dir" ] && payload=$(echo "$payload" | jq --arg v "$working_dir" '. + {working_dir: $v}')
    [ -n "$git_url" ] && payload=$(echo "$payload" | jq --arg v "$git_url" '. + {git_url: $v}')
    [ -n "$git_ref" ] && payload=$(echo "$payload" | jq --arg v "$git_ref" '. + {git_ref: $v}')
    [ -n "$nix_shell" ] && payload=$(echo "$payload" | jq --arg v "$nix_shell" '. + {nix_shell: $v}')
    [ -n "$system_prompt" ] && payload=$(echo "$payload" | jq --arg v "$system_prompt" '. + {system_prompt: $v}')
    [ -n "$api_key" ] && payload=$(echo "$payload" | jq --arg v "$api_key" '. + {api_key: $v}')
    [ -n "$description" ] && payload=$(echo "$payload" | jq --arg v "$description" '. + {description: $v}')
    
    # Use query param endpoint for better compatibility
    local response
    response=$(api_patch "/profiles/update?id=$id" "$payload")
    
    local http_code="${response##*|}";
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
    
    # Use query param endpoint for better compatibility
    local response
    response=$(api_delete "/profiles/delete?id=$id")
    
    local http_code="${response##*|}";
    local body="${response%|*}"
    
    if is_success "$http_code"; then
        success "Profile deleted successfully"
    else
        error "Failed to delete profile: $body"
    fi
}

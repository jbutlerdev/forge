#!/usr/bin/env bash
# Message commands: send a message to a session and (optionally)
# stream the agent's response by polling /messages and printing new
# rows as they appear.
#
# Forge's API has no GET /messages/stream endpoint - the only SSE
# endpoint is /tools/execute/stream, which is for tool execution.
# Streaming the agent's text response is therefore done client-side:
# POST /messages returns immediately (the API spawns pi in the
# background), then we poll the message list on a tight loop and
# print rows whose sequence is newer than what we last saw.

cmd_message() {
    local subcommand="${1:-}"
    shift || true

    case "$subcommand" in
        send)   cmd_message_send "$@" ;;
        watch)  cmd_message_watch "$@" ;;
        ask)    cmd_message_ask "$@" ;;
        list)   cmd_message_list "$@" ;;
        -h|--help|help)
            cat << 'EOF'
forge message - Send a message to an agent and watch the response

Usage:
    forge message send <session_id> <text>     Send a message (fire-and-forget)
    forge message watch <session_id>           Poll and print new messages
    forge message ask <session_id> <text>      Send + watch (streaming UX)
    forge message list <session_id>            List all messages
EOF
            ;;
        *)
            echo "Unknown message command: $subcommand"
            cmd_message -h
            exit 1
            ;;
    esac
}

# Render a single message row from the API in a human-friendly form.
# Expects a JSON object on stdin. Distinguishes:
#   - tool-call rows (role=assistant, tool_call_id set, content is "[tool_call:<name>]")
#   - tool-result rows (role=tool)
#   - text rows (everything else)
_msg_render() {
    jq -r '
        def color:
            if .role == "user"      then "\u001b[36m"   # cyan
            elif .role == "tool"    then "\u001b[33m"   # yellow
            elif .role == "assistant" and .tool_call_id != null
                                       then "\u001b[35m" # magenta
            else                          "\u001b[32m" # green
            end;
        def reset: "\u001b[0m";
        "\(color)[\(.role)\(if .tool_name != null then " (\(.tool_name))" else "" end)]\(reset) \(.created_at // "")",
        (
            if .tool_call_id != null and .role == "assistant" then
                "  call_id:  \(.tool_call_id)",
                "  input:    \(.tool_input // "null" | tostring)"
            elif .role == "tool" then
                "  call_id:  \(.tool_call_id)",
                "  duration: \(.duration_ms // "?" )ms",
                "  output:   \(.tool_output // "null" | tostring)",
                "  text:     \(.content // "")"
            else
                .content // ""
            end
        )
    '
}

# Poll the messages endpoint and print any row whose sequence is
# greater than $1 (the last-seen sequence). Times out after $2
# seconds total (NOT seconds of silence). A turn is considered
# "complete" when the agent has emitted a text response row AND
# then gone quiet for `_GRACE_SECS` seconds. This handles multi-step
# turns where the model runs many tool calls between text responses.
_msg_poll_new() {
    local session_id="$1"
    local last_seq="$2"
    local timeout_secs="${3:-300}"
    local _GRACE_SECS=5

    local deadline=$(( $(date +%s) + timeout_secs ))

    # Track the most recent assistant text row we've SEEN on the
    # server (not yet printed). When the API's max sequence is past
    # this row by a few polls of silence, we know the agent is done.
    local last_text_seq=0
    local last_new_row_at=0

    while [ "$(date +%s)" -lt "$deadline" ]; do
        local response
        response=$(api_get "/messages?session_id=$session_id")
        local http_code="${response##*|}"
        local body="${response%|*}"

        if ! is_success "$http_code"; then
            echo "(error: $body)" >&2
            return 1
        fi

        # New rows in this batch
        local new_rows
        new_rows=$(echo "$body" | jq -c --argjson after "$last_seq" \
            '[.messages[] | select(.sequence > $after)] | sort_by(.sequence)')

        local new_count
        new_count=$(echo "$new_rows" | jq 'length')

        if [ "$new_count" -gt 0 ]; then
            # Print each new row in order.
            while IFS= read -r row; do
                [ -z "$row" ] && continue
                local row_seq
                row_seq=$(echo "$row" | jq -r '.sequence')
                local is_text
                is_text=$(echo "$row" | jq -r '
                    (.role == "assistant" and .tool_call_id == null
                     and (.content // "") != "")
                ')
                echo "$row" | _msg_render
                echo
                last_seq="$row_seq"
                [ "$is_text" = "true" ] && last_text_seq="$row_seq"
            done < <(echo "$new_rows" | jq -c '.[]')
            last_new_row_at=$(date +%s)
        else
            # No new rows this poll. If we've already seen an
            # assistant text row, AND we've gone quiet for
            # _GRACE_SECS, the turn is done.
            if [ "$last_text_seq" -gt 0 ]; then
                local now
                now=$(date +%s)
                if [ $((now - last_new_row_at)) -ge "$_GRACE_SECS" ]; then
                    return 0
                fi
            fi
            sleep 1
        fi
    done

    return 0
}

cmd_message_list() {
    local session_id="$1"
    [ -z "$session_id" ] && error "Session ID is required"
    local response
    response=$(api_get "/messages?session_id=$session_id")
    local http_code="${response##*|}"
    local body="${response%|*}"
    if is_success "$http_code"; then
        echo "$body" | jq -r '.messages[] | "[\(.role)] seq=\(.sequence) \(.created_at // "")\n  \(.content // ("[tool_call:" + (.tool_name // "?") + "]"))\n"'
    else
        error "Failed to list messages: $body"
    fi
}

cmd_message_send() {
    local session_id="$1"
    shift
    [ -z "$session_id" ] && error "Session ID is required"
    [ $# -eq 0 ] && error "Message text is required"

    local text="$*"
    local payload
    payload=$(jq -n --arg sid "$session_id" --arg c "$text" \
        '{session_id: $sid, content: $c}')

    local response
    response=$(api_post "/messages" "$payload")
    local http_code="${response##*|}"
    local body="${response%|*}"

    if is_success "$http_code"; then
        success "Message accepted by API (pi spawned in background)"
        echo "$body" | jq -r '"  echo sequence: \(.message.sequence)\n  message id:    \(.message.id)"'
    else
        error "Failed to send message: $body"
    fi
}

cmd_message_watch() {
    local session_id="$1"
    [ -z "$session_id" ] && error "Session ID is required"

    # Print the current max sequence first, then poll for anything
    # newer.
    local initial
    initial=$(api_get "/messages?session_id=$session_id")
    local last_seq
    last_seq=$(echo "${initial%|*}" | jq -c '.messages // [] | (if length == 0 then 0 else [.[].sequence] | max end)')

    echo "Watching session $session_id (last seq = $last_seq). Ctrl-C to stop."
    _msg_poll_new "$session_id" "$last_seq" 300
}

cmd_message_ask() {
    local session_id="$1"
    shift
    [ -z "$session_id" ] && error "Session ID is required"
    [ $# -eq 0 ] && error "Message text is required"

    local text="$*"
    local payload
    payload=$(jq -n --arg sid "$session_id" --arg c "$text" \
        '{session_id: $sid, content: $c}')

    local response
    response=$(api_post "/messages" "$payload")
    local http_code="${response##*|}"
    local body="${response%|*}"

    if ! is_success "$http_code"; then
        error "Failed to send message: $body"
    fi

    local sent_seq
    sent_seq=$(echo "$body" | jq -r '.message.sequence')
    success "Sent (seq $sent_seq) - streaming response:"

    # Start polling from one less than the user message we just
    # sent, so we see the user row printed too.
    _msg_poll_new "$session_id" "$((sent_seq - 1))" 300
}

#!/usr/bin/env bash
# Scheduled-agent subcommands.
#
# These wrap the host-level forge-agent-setup and forge-heartbeat
# scripts (installed to /usr/local/bin by scripts/install.sh).
# Use them to provision new agents, list existing ones, and watch
# a heartbeat run in the journal.

cmd_agent() {
    local subcommand="${1:-}"
    shift || true

    case "$subcommand" in
        setup)   cmd_agent_setup   "$@" ;;
        list)    cmd_agent_list    "$@" ;;
        status)  cmd_agent_status  "$@" ;;
        logs)    cmd_agent_logs    "$@" ;;
        disable) cmd_agent_disable "$@" ;;
        enable)  cmd_agent_enable  "$@" ;;
        remove)  cmd_agent_remove  "$@" ;;
        -h|--help|help)
            cat << 'EOF'
forge agent - Manage scheduled forge agents

Usage:
    forge agent setup <name>           Provision a new agent (runs forge-agent-setup)
    forge agent list                   List configured agents and their timers
    forge agent status <name>          Show timer + service state for an agent
    forge agent logs <name>            Follow journal for an agent
    forge agent enable <name>          Enable and start the timer
    forge agent disable <name>         Stop the timer
    forge agent remove <name>          Disable timer, remove systemd units

Scheduled agents run on a systemd timer (forge-agent@<name>.timer)
and POST a heartbeat prompt to a long-lived forge session. See
docs/SCHEDULED-AGENTS.md for the full design.
EOF
            ;;
        *)
            echo "Unknown agent command: $subcommand"
            cmd_agent -h
            exit 1
            ;;
    esac
}

cmd_agent_setup() {
    local name="${1:-}"
    if [[ -z "$name" ]]; then
        error "Usage: forge agent setup <name>"
    fi
    if ! command -v forge-agent-setup >/dev/null 2>&1; then
        error "forge-agent-setup not on PATH; install it with 'sudo bash scripts/install.sh'"
    fi
    exec sudo forge-agent-setup "$name"
}

cmd_agent_list() {
    local agents_dir="/etc/forge/agents"
    if [[ ! -d "$agents_dir" ]]; then
        echo "No agents directory at $agents_dir. Run 'forge agent setup <name>' to create one."
        return 0
    fi
    local name
    for name in "$agents_dir"/*; do
        [[ -d "$name" ]] || continue
        local base
        base="$(basename "$name")"
        local timer_state="(no timer)"
        if command -v systemctl >/dev/null 2>&1; then
            if systemctl list-unit-files "forge-agent@${base}.timer" >/dev/null 2>&1; then
                if systemctl is-enabled --quiet "forge-agent@${base}.timer" 2>/dev/null; then
                    if systemctl is-active --quiet "forge-agent@${base}.timer" 2>/dev/null; then
                        timer_state="active"
                    else
                        timer_state="enabled (waiting)"
                    fi
                else
                    timer_state="loaded (not enabled)"
                fi
            fi
        fi
        printf "  %-24s %s\n" "$base" "$timer_state"
    done
}

cmd_agent_status() {
    local name="${1:-}"
    [[ -z "$name" ]] && error "Usage: forge agent status <name>"
    if ! command -v systemctl >/dev/null 2>&1; then
        error "systemctl not on PATH; run on the forge host"
    fi
    systemctl status "forge-agent@${name}.timer" "forge-agent@${name}.service" --no-pager || true
}

cmd_agent_logs() {
    local name="${1:-}"
    [[ -z "$name" ]] && error "Usage: forge agent logs <name>"
    if ! command -v journalctl >/dev/null 2>&1; then
        error "journalctl not on PATH; run on the forge host"
    fi
    exec journalctl -u "forge-agent@${name}.service" -f
}

cmd_agent_enable() {
    local name="${1:-}"
    [[ -z "$name" ]] && error "Usage: forge agent enable <name>"
    if ! command -v systemctl >/dev/null 2>&1; then
        error "systemctl not on PATH; run on the forge host"
    fi
    sudo systemctl enable --now "forge-agent@${name}.timer"
}

cmd_agent_disable() {
    local name="${1:-}"
    [[ -z "$name" ]] && error "Usage: forge agent disable <name>"
    if ! command -v systemctl >/dev/null 2>&1; then
        error "systemctl not on PATH; run on the forge host"
    fi
    sudo systemctl disable --now "forge-agent@${name}.timer"
}

cmd_agent_remove() {
    local name="${1:-}"
    [[ -z "$name" ]] && error "Usage: forge agent remove <name>"
    if ! command -v systemctl >/dev/null 2>&1; then
        error "systemctl not on PATH; run on the forge host"
    fi
    sudo systemctl disable --now "forge-agent@${name}.timer" 2>/dev/null || true
    sudo rm -f "/etc/systemd/system/forge-agent@${name}.timer" \
              "/etc/systemd/system/forge-agent@${name}.service"
    sudo systemctl daemon-reload
    success "Removed forge-agent@${name} (agent dir at /etc/forge/agents/${name} left intact)"
}

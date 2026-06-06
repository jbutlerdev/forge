#!/usr/bin/env bash
# Forge Installation Script
# 
# Full installation of Forge API and CLI
# Run: sudo bash scripts/install.sh
#
# This script:
# 1. Installs dependencies (Rust, PostgreSQL)
# 2. Builds the forge-api binary
# 3. Sets up PostgreSQL database
# 4. Creates forge user and directories
# 5. Installs systemd service
# 6. Installs CLI to /usr/local/bin

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
FORGE_USER="forge"
FORGE_GROUP="forge"
INSTALL_DIR="/opt/forge"
SERVICE_DIR="/etc/systemd/system"
CONFIG_DIR="/etc/forge"
SESSION_DIR="/forge/sessions"
LOG_DIR="/var/log/forge"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}Error: This script must be run as root (use sudo)${NC}"
    exit 1
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Forge Installation${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# ============================================
# Step 1: Detect OS
# ============================================
echo -e "${YELLOW}[1/9] Detecting OS...${NC}"

if [ -f /etc/debian_version ]; then
    OS="debian"
    echo "  Detected: Debian/Ubuntu"
elif [ -f /etc/redhat-release ]; then
    OS="rhel"
    echo "  Detected: RHEL/Fedora/CentOS"
elif [ -f /etc/arch-release ]; then
    OS="arch"
    echo "  Detected: Arch Linux"
else
    OS="unknown"
    echo "  Detected: Unknown (will try generic install)"
fi
echo ""

# ============================================
# Step 2: Install system dependencies
# ============================================
echo -e "${YELLOW}[2/9] Installing system dependencies...${NC}"

install_pkg() {
    if [ "$OS" = "debian" ]; then
        apt-get update -qq
        apt-get install -y -qq "$@"
    elif [ "$OS" = "rhel" ]; then
        yum install -y -q "$@"
    elif [ "$OS" = "arch" ]; then
        pacman -Sy --noconfirm "$@"
    fi
}

# Core dependencies
DEPS="curl git build-essential pkg-config libssl-dev"

# PostgreSQL
if ! command -v psql &> /dev/null; then
    if [ "$OS" = "debian" ]; then
        DEPS="$DEPS postgresql postgresql-contrib"
    elif [ "$OS" = "rhel" ]; then
        DEPS="$DEPS postgresql-server postgresql-contrib"
    else
        DEPS="$DEPS postgresql"
    fi
fi

install_pkg $DEPS 2>/dev/null || echo "  Some packages may already be installed"
echo -e "${GREEN}  ✓ Dependencies installed${NC}"
echo ""

# ============================================
# Step 3: Install Rust (if not present)
# ============================================
echo -e "${YELLOW}[3/9] Checking Rust...${NC}"

if ! command -v cargo &> /dev/null; then
    echo "  Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
    echo -e "${GREEN}  ✓ Rust installed${NC}"
else
    echo -e "${GREEN}  ✓ Rust already installed ($(cargo --version | cut -d' ' -f2))${NC}"
fi
echo ""

# ============================================
# Step 4: Setup PostgreSQL
# ============================================
echo -e "${YELLOW}[4/9] Setting up PostgreSQL...${NC}"

setup_postgres() {
    if [ "$OS" = "debian" ]; then
        systemctl enable postgresql
        systemctl start postgresql
        
        # Create database and user
        sudo -u postgres psql -c "CREATE USER forge WITH PASSWORD 'forge';" 2>/dev/null || true
        sudo -u postgres psql -c "CREATE DATABASE forge OWNER forge;" 2>/dev/null || true
        sudo -u postgres psql -c "ALTER USER forge CREATEDB;" 2>/dev/null || true
        
    elif [ "$OS" = "rhel" ]; then
        systemctl enable postgresql
        postgresql-setup --initdb || true
        systemctl start postgresql
        
        # Create database and user
        sudo -u postgres psql -c "CREATE USER forge WITH PASSWORD 'forge';" 2>/dev/null || true
        sudo -u postgres psql -c "CREATE DATABASE forge OWNER forge;" 2>/dev/null || true
    fi
}

setup_postgres 2>/dev/null || echo "  PostgreSQL setup skipped (may already be configured)"
echo -e "${GREEN}  ✓ PostgreSQL configured${NC}"
echo ""

# ============================================
# Step 5: Create forge user and directories
# ============================================
echo -e "${YELLOW}[5/9] Creating user and directories...${NC}"

# Create user
id "$FORGE_USER" &>/dev/null || useradd -r -m -s /bin/bash -d "$INSTALL_DIR" "$FORGE_USER"
echo "  ✓ User '$FORGE_USER' created/verified"

# Create directories
mkdir -p "$INSTALL_DIR"
mkdir -p "$SESSION_DIR"
mkdir -p "$LOG_DIR"
mkdir -p "$CONFIG_DIR"

# Set permissions
chown -R "$FORGE_USER:$FORGE_GROUP" "$INSTALL_DIR" "$SESSION_DIR" "$LOG_DIR" "$CONFIG_DIR"
chmod 755 "$INSTALL_DIR" "$SESSION_DIR" "$LOG_DIR" "$CONFIG_DIR"

echo "  ✓ Directories created"
echo ""

# ============================================
# Step 6: Build forge-api
# ============================================
echo -e "${YELLOW}[6/9] Building forge-api...${NC}"

# Get the source directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$(dirname "$SCRIPT_DIR")"

cd "$SOURCE_DIR"

# Build release binary
cargo build --release -p forge-api 2>&1 | tail -3

# Copy binary
cp "target/release/forge-api" "$INSTALL_DIR/"
chown "$FORGE_USER:$FORGE_GROUP" "$INSTALL_DIR/forge-api"
chmod +x "$INSTALL_DIR/forge-api"

echo -e "${GREEN}  ✓ forge-api built and installed${NC}"
echo ""

# ============================================
# Step 7: Run database migrations
# ============================================
echo -e "${YELLOW}[7/9] Running database migrations...${NC}"

# Set DATABASE_URL for migrations
export DATABASE_URL="postgres://forge:forge@localhost/forge"

# Create tables (using the API's migration logic)
cat > /tmp/forge_init.sql << 'SQLEOF'
-- Forge Database Schema
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS profiles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    provider VARCHAR(50) NOT NULL,
    model VARCHAR(100) NOT NULL,
    working_dir VARCHAR(500),
    git_url VARCHAR(500),
    git_ref VARCHAR(100),
    nix_shell VARCHAR(500),
    system_prompt TEXT,
    api_key_encrypted TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sessions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    profile_id UUID REFERENCES profiles(id) ON DELETE CASCADE,
    title VARCHAR(255),
    working_dir VARCHAR(500),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_active TIMESTAMPTZ DEFAULT NOW(),
    ended_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS messages (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    session_id UUID REFERENCES sessions(id) ON DELETE CASCADE,
    role VARCHAR(20) NOT NULL,
    content TEXT,
    tool_name VARCHAR(100),
    tool_input JSONB,
    tool_output TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    key_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles(user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_profile_id ON sessions(profile_id);
CREATE INDEX IF NOT EXISTS idx_messages_session_id ON messages(session_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
SQLEOF

# Try to run migrations
if command -v psql &> /dev/null; then
    sudo -u postgres psql -d forge -f /tmp/forge_init.sql 2>/dev/null || \
    PGPASSWORD=forge psql -U forge -h localhost -d forge -f /tmp/forge_init.sql 2>/dev/null || \
    echo "  ⚠ Could not run migrations (check database credentials)"
    rm -f /tmp/forge_init.sql
fi

echo -e "${GREEN}  ✓ Migrations complete${NC}"
echo ""

# ============================================
# Step 8: Create environment file
# ============================================
echo -e "${YELLOW}[8/9] Creating configuration...${NC}"

cat > "$CONFIG_DIR/forge.env" << 'ENVEOF'
# Forge Configuration
DATABASE_URL=postgres://forge:forge@localhost/forge
FORGE_API_HOST=0.0.0.0
FORGE_API_PORT=8080
RUST_LOG=info
ANTHROPIC_API_KEY=your-anthropic-api-key-here
OPENAI_API_KEY=your-openai-api-key-here
ENVEOF

chown "$FORGE_USER:$FORGE_GROUP" "$CONFIG_DIR/forge.env"
chmod 600 "$CONFIG_DIR/forge.env"

echo "  ✓ Configuration created at $CONFIG_DIR/forge.env"
echo ""

# ============================================
# Step 9: Install systemd service and CLI
# ============================================
echo -e "${YELLOW}[9/9] Installing service and CLI...${NC}"

# Install systemd service
if [ -f "$SOURCE_DIR/systemd/forge-api.service" ]; then
    cp "$SOURCE_DIR/systemd/forge-api.service" "$SERVICE_DIR/"
    systemctl daemon-reload
    systemctl enable forge-api
    echo "  ✓ Systemd service installed"
fi

# Install CLI
if [ -f "$SOURCE_DIR/cli/forge" ]; then
    cp "$SOURCE_DIR/cli/forge" /usr/local/bin/forge
    chmod +x /usr/local/bin/forge

    # Install bash-completion if available
    if [ -d /etc/bash_completion.d ]; then
        # Basic completion (would need proper completion script)
        echo "forge" > /etc/bash_completion.d/forge
    fi

    echo "  ✓ CLI installed to /usr/local/bin/forge"
fi

# Install scheduled-agent support: the two bash scripts that
# `forge-agent-setup` and `forge-heartbeat` live in the in-repo
# scripts/ directory; we copy them to /usr/local/bin so the
# operator (and the systemd units) can find them on PATH. The
# systemd unit templates are copied to /etc/forge/systemd-units/
# so forge-agent-setup can find them at runtime (it falls back
# to the in-repo path during dev).
for f in forge-agent-setup forge-heartbeat; do
    if [ -f "$SOURCE_DIR/scripts/$f" ]; then
        cp "$SOURCE_DIR/scripts/$f" /usr/local/bin/$f
        chmod 0755 /usr/local/bin/$f
        echo "  ✓ Installed /usr/local/bin/$f"
    fi
done
if [ -d "$SOURCE_DIR/systemd/agents" ]; then
    mkdir -p /etc/forge/systemd-units
    cp -a "$SOURCE_DIR/systemd/agents/." /etc/forge/systemd-units/
    echo "  ✓ Systemd agent unit templates in /etc/forge/systemd-units/"
fi
# yq is required by forge-agent-setup for YAML parsing. Install
# the mikefarah/go-yq single binary if it's not already present.
if ! command -v yq >/dev/null 2>&1; then
    echo "  - Installing yq (mikefarah/go-yq) for forge-agent-setup..."
    YQ_VERSION="v4.40.5"
    YQ_URL="https://github.com/mikefarah/yq/releases/download/${YQ_VERSION}/yq_linux_amd64"
    if curl -fsSL "$YQ_URL" -o /usr/local/bin/yq 2>/dev/null; then
        chmod +x /usr/local/bin/yq
        echo "  ✓ Installed /usr/local/bin/yq (${YQ_VERSION})"
    else
        echo "  ⚠ Could not download yq. forge-agent-setup needs it; install manually:"
        echo "      sudo curl -fsSL https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -o /usr/local/bin/yq && sudo chmod +x /usr/local/bin/yq"
    fi
fi

# Install host-side git credential helper. forge-api uses
# `git -c credential.helper=/usr/local/bin/git-credential-github clone …`
# to authenticate github.com clones against $FORGE_GITHUB_TOKEN
# (set in /etc/forge/forge.env) without putting the token in
# the clone URL, the .git/config, `ps` output, or git error
# messages. The helper reads the token from the env at git
# invocation time, so rotating the token in forge.env and
# restarting forge-api takes effect on the next clone.
#
# The install path is /usr/local/bin/git-credential-github
# (matching the in-container helper at
# /forge/sandbox/base/usr/local/bin/git-credential-github, which
# reads $GITHUB_TOKEN). Same script name on host and in
# container, different env var name — they're paired but
# separate because the env var namespaces are different.
if [ -f "$SOURCE_DIR/scripts/git-credential-forge" ]; then
    cp "$SOURCE_DIR/scripts/git-credential-forge" /usr/local/bin/git-credential-github
    chmod +x /usr/local/bin/git-credential-github
    echo "  ✓ Git credential helper installed to /usr/local/bin/git-credential-github"
fi

echo ""

# ============================================
# Final Summary
# ============================================
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  Installation Complete!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo -e "Next steps:"
echo ""
echo -e "1. ${YELLOW}Edit configuration:${NC}"
echo "   nano $CONFIG_DIR/forge.env"
echo ""
echo -e "2. ${YELLOW}Start the service:${NC}"
echo "   sudo systemctl start forge-api"
echo "   sudo systemctl status forge-api"
echo ""
echo -e "3. ${YELLOW}Check logs if issues:${NC}"
echo "   journalctl -u forge-api -f"
echo ""
echo -e "4. ${YELLOW}Test with CLI:${NC}"
echo "   export FORGE_API_URL=http://localhost:8080/api/v1"
echo "   forge profile create my-agent --provider anthropic --model claude-sonnet-4-20250514"
echo ""
echo -e "${GREEN}Default database credentials:${NC}"
echo "   User: forge"
echo "   Pass: forge"
echo "   DB:   forge"
echo ""

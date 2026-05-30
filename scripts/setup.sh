#!/usr/bin/env bash
# Forge Quick Setup Script
# 
# This script sets up a basic Forge environment for testing.
# Run: bash scripts/setup.sh

set -e

FORGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$FORGE_DIR"

echo "========================================"
echo "Forge Quick Setup"
echo "========================================"
echo ""

# Check prerequisites
echo "[1/6] Checking prerequisites..."

# Check for Rust
if ! command -v cargo &> /dev/null; then
    echo "  ERROR: Rust/Cargo not found"
    echo "  Install: curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi
echo "  ✓ Rust/Cargo found"

# Check for Node.js
if ! command -v node &> /dev/null; then
    echo "  ERROR: Node.js not found"
    echo "  Install: https://nodejs.org/"
    exit 1
fi
echo "  ✓ Node.js found ($(node --version))"

# Check for pi CLI
if ! command -v pi &> /dev/null; then
    echo "  ⚠ pi CLI not found, installing..."
    npm install -g @mariozechner/pi-coding-agent
fi
echo "  ✓ pi CLI found ($(pi --version 2>/dev/null || echo 'version unknown'))"

# Check for PostgreSQL
if ! command -v psql &> /dev/null; then
    echo "  ⚠ PostgreSQL client not found"
else
    echo "  ✓ PostgreSQL client found"
fi

echo ""

# Build the project
echo "[2/6] Building forge-api..."
cargo build -p forge-api 2>&1 | tail -5
echo "  ✓ Build complete"

# Setup database
echo ""
echo "[3/6] Setting up database..."
if command -v psql &> /dev/null && systemctl is-active postgresql &>/dev/null; then
    # Check if database exists
    if ! psql -lqt | cut -d \| -f 1 | grep -qw "forge"; then
        echo "  Creating database 'forge'..."
        sudo -u postgres psql -c "CREATE DATABASE forge;"
        sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'forge';"
        echo "  ✓ Database created"
    else
        echo "  ✓ Database 'forge' already exists"
    fi
else
    echo "  ⚠ PostgreSQL not running (skipping DB setup)"
    echo "  Start with: sudo systemctl start postgresql"
fi

# Setup session directory
echo ""
echo "[4/6] Setting up session directory..."
if [ ! -d "/forge/sessions" ]; then
    sudo mkdir -p /forge/sessions
    sudo chmod 777 /forge/sessions
    echo "  ✓ Created /forge/sessions"
else
    echo "  ✓ /forge/sessions already exists"
fi

# Build forge-tools extension
echo ""
echo "[5/6] Building forge-tools extension..."
cd extensions/forge-tools
if [ ! -d "node_modules" ]; then
    npm install
fi
npm run build 2>&1 | tail -3
cd ../..
echo "  ✓ Extension built"

# Create .env file
echo ""
echo "[6/6] Creating environment file..."
cat > .env << 'EOF'
# Forge Configuration
DATABASE_URL=postgres://postgres:forge@localhost/forge
FORGE_API_URL=http://localhost:8080/api/v1
ANTHROPIC_API_KEY=your-api-key-here
EOF
echo "  ✓ Created .env file"
echo "  ⚠ Update ANTHROPIC_API_KEY with your actual API key"

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Update your API key:"
echo "   nano .env"
echo ""
echo "2. Start the API server:"
echo "   source .env && cargo run -p forge-api"
echo ""
echo "3. In another terminal, use the CLI:"
echo "   export FORGE_API_URL=http://localhost:8080/api/v1"
echo "   forge profile create my-agent --provider anthropic --model claude-sonnet-4-20250514"
echo "   forge session create <profile-id>"
echo "   forge send <session-id> 'Hello, world!'"
echo ""

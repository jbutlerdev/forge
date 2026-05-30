#!/usr/bin/env bash
# Forge Uninstallation Script
# 
# Removes Forge installation
# Run: sudo bash scripts/uninstall.sh [--purge]

set -e

# Configuration
INSTALL_DIR="/opt/forge"
CONFIG_DIR="/etc/forge"
SESSION_DIR="/forge/sessions"
LOG_DIR="/var/log/forge"
SERVICE_NAME="forge-api"

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Error: This script must be run as root (use sudo)"
    exit 1
fi

echo "========================================"
echo "  Forge Uninstallation"
echo "========================================"
echo ""

# Stop and disable service
echo "[1/4] Stopping service..."
systemctl stop $SERVICE_NAME 2>/dev/null || true
systemctl disable $SERVICE_NAME 2>/dev/null || true
rm -f /etc/systemd/system/$SERVICE_NAME.service
systemctl daemon-reload
echo "  ✓ Service stopped and removed"

# Remove binaries
echo ""
echo "[2/4] Removing binaries..."
rm -f /usr/local/bin/forge
rm -rf "$INSTALL_DIR"
rm -f /etc/bash_completion.d/forge
echo "  ✓ Binaries removed"

# Ask about data
echo ""
echo "[3/4] Data directories:"
echo "  - $SESSION_DIR"
echo "  - $LOG_DIR"
echo "  - $CONFIG_DIR"

if [ "$1" = "--purge" ]; then
    echo ""
    echo "Purging all data..."
    rm -rf "$SESSION_DIR"
    rm -rf "$LOG_DIR"
    rm -rf "$CONFIG_DIR"
    echo "  ✓ Data purged"
else
    echo ""
    echo "  (Data preserved. Use --purge to remove all data)"
fi

# Ask about database
echo ""
echo "[4/4] PostgreSQL database 'forge'"
echo "  To remove: sudo -u postgres psql -c 'DROP DATABASE forge;'"
echo "  To remove user: sudo -u postgres psql -c 'DROP USER forge;'"

echo ""
echo "========================================"
echo "  Uninstall Complete"
echo "========================================"
echo ""

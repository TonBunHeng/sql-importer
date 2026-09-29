#!/usr/bin/env bash
set -e

# Always run from the project root directory
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

echo "========================================================"
echo "          🚀 SQL Importer - Starting Server             "
echo "========================================================"

# Find Python 3 binary
PYTHON_CMD=""
if [ -f "venv/bin/python" ]; then
    PYTHON_CMD="venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    echo "❌ Error: Python 3 was not found on your system."
    echo "Please install Python 3 (e.g. brew install python3 on Mac)."
    exit 1
fi

# Ensure venv exists
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment in ./venv..."
    $PYTHON_CMD -m venv venv
    PYTHON_CMD="venv/bin/python"
fi

# Ensure requirements are installed
if [ -f "requirements.txt" ]; then
    echo "🔍 Checking dependencies..."
    ./venv/bin/python -m pip install --quiet --upgrade pip
    ./venv/bin/python -m pip install --quiet -r requirements.txt
fi

PORT=5000
echo ""
echo "✅ Server is running!"
echo "👉 Open in your browser: http://127.0.0.1:${PORT}"
echo "👉 Or: http://localhost:${PORT}"
echo "Press Ctrl+C to stop the server."
echo "========================================================"
echo ""

exec ./venv/bin/python app.py

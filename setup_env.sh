#!/bin/bash

# Setup script for landbruget.dk development environment
# This ensures your virtual environment is always ready

set -e

echo "🚀 Setting up landbruget.dk development environment..."

# Check if we're in the right directory
if [ ! -f "pyproject.toml" ]; then
    echo "❌ Error: Please run this script from the project root directory"
    exit 1
fi

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "📦 Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    source $HOME/.cargo/env
fi

# Create/update virtual environment with uv
echo "🐍 Setting up Python virtual environment with uv..."
uv venv .venv --python 3.13

# Install dependencies
echo "📚 Installing dependencies..."
uv pip install "ibis-framework[duckdb]~=10.6.0" "duckdb~=1.3.1"

# Activate virtual environment
echo "✅ Virtual environment ready!"
echo ""
echo "To activate the virtual environment, run:"
echo "  source .venv/bin/activate"
echo ""
echo "Or use uv to run commands directly:"
echo "  uv run python your_script.py"
echo "  uv run jupyter lab"
echo ""
echo "�� Setup complete!" 
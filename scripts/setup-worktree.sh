#!/bin/bash

# Worktree Setup Script for Conductor - landbruget.dk
# Automates dependency installation and environment configuration for worktrees

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FRONTEND_DIR="$PROJECT_ROOT/frontend"
BACKEND_DIR="$PROJECT_ROOT/backend"

require_command() {
    local command_name="$1"
    local install_hint="$2"

    if ! command -v "$command_name" >/dev/null 2>&1; then
        echo "❌ Required command not found: $command_name"
        echo "   $install_hint"
        exit 1
    fi
}

# Find the main repository (where the original .env lives)
GIT_COMMON_DIR="$(cd "$PROJECT_ROOT" && git rev-parse --git-common-dir 2>/dev/null || echo "")"
if [ -n "$GIT_COMMON_DIR" ]; then
    MAIN_REPO_ROOT="$(cd "$PROJECT_ROOT" && cd "$GIT_COMMON_DIR/.." && pwd)"
else
    MAIN_REPO_ROOT=""
fi

echo "🚀 Setting up landbruget.dk worktree..."
echo "📁 Worktree root: $PROJECT_ROOT"
if [ -n "$MAIN_REPO_ROOT" ] && [ "$MAIN_REPO_ROOT" != "$PROJECT_ROOT" ]; then
    echo "📁 Main repo root: $MAIN_REPO_ROOT"
fi
echo ""

require_command "node" "Install Node.js 22+ before running this script."
require_command "npm" "Install npm with Node.js before running this script."
require_command "uv" "Install uv first: https://docs.astral.sh/uv/getting-started/installation/"

# Step 1: Symlink root .env (single source of truth for all secrets)
ROOT_ENV_FILE="$PROJECT_ROOT/.env"

echo "🔧 Setting up environment variables..."
if [ -L "$ROOT_ENV_FILE" ]; then
    LINK_TARGET="$(readlink "$ROOT_ENV_FILE")"
    echo "✅ .env is already a symlink → $LINK_TARGET"
elif [ -f "$ROOT_ENV_FILE" ]; then
    echo "✅ .env file already exists"
else
    if [ -n "$MAIN_REPO_ROOT" ] && [ "$MAIN_REPO_ROOT" != "$PROJECT_ROOT" ] && [ -f "$MAIN_REPO_ROOT/.env" ]; then
        ln -s "$MAIN_REPO_ROOT/.env" "$ROOT_ENV_FILE"
        echo "✅ Created symlink to main repo .env"
        echo "   $ROOT_ENV_FILE → $MAIN_REPO_ROOT/.env"
    elif [ -f "$PROJECT_ROOT/.env.example" ]; then
        cp "$PROJECT_ROOT/.env.example" "$ROOT_ENV_FILE"
        echo "⚠️  Created .env from .env.example — fill in your credentials"
    else
        echo "⚠️  No .env found. Copy .env.example and fill in credentials."
    fi
fi
echo ""

# Step 2: Run direnv allow
echo "🔧 Setting up direnv..."
if command -v direnv &> /dev/null; then
    if [ -f "$PROJECT_ROOT/.envrc" ]; then
        cd "$PROJECT_ROOT"
        direnv allow
        echo "✅ direnv allow executed"
    else
        echo "⚠️  No .envrc found at project root"
    fi
else
    echo "⚠️  direnv not installed (install with: brew install direnv)"
fi
echo ""

# Step 3: Install Frontend dependencies
echo "📦 Installing Frontend dependencies..."
cd "$FRONTEND_DIR"
if [ -f "package-lock.json" ]; then
    npm ci
else
    npm install
fi
echo "✅ Frontend dependencies installed"
echo ""

# Step 4: Install Playwright browser binaries
echo "🎭 Installing Playwright browsers..."
cd "$FRONTEND_DIR"
npm exec -- playwright install
echo "✅ Playwright browsers ready"
echo ""

# Step 5: Set up Backend Python environment using uv (workspace-aware)
echo "🐍 Setting up Backend Python environment..."
cd "$PROJECT_ROOT"
echo "📦 Installing Python dependencies with uv..."
uv sync --python 3.11 --all-packages --group dev
echo "✅ Python dependencies installed via uv workspace"
echo ""

# Step 6: Verify developer tools resolve locally
echo "🧪 Verifying local toolchain..."
cd "$FRONTEND_DIR"
npm exec -- oxlint --version >/dev/null
npm exec -- playwright --version >/dev/null
echo "✅ Frontend verification commands are available"

cd "$PROJECT_ROOT"
uv run pytest --version >/dev/null
echo "✅ Backend pytest is available through uv"
echo ""

# Step 7: Verify .env is gitignored
GITIGNORE_FILE="$PROJECT_ROOT/.gitignore"
if [ -f "$GITIGNORE_FILE" ]; then
    if grep -q "\.env" "$GITIGNORE_FILE"; then
        echo "✅ .env files are gitignored"
    else
        echo "⚠️  Warning: .env may not be gitignored"
    fi
fi
echo ""

# Summary
echo "✨ Worktree setup complete!"
echo ""
echo "Next steps:"
echo "  1. Verify .env has your credentials (see .env.example)"
echo "  2. cd frontend && npm run dev        # Start dev server"
echo "  3. cd frontend && npm run lint       # Run oxlint"
echo "  4. cd frontend && npm test           # Run Playwright E2E"
echo "  5. uv run pytest                     # Run backend tests"
echo ""

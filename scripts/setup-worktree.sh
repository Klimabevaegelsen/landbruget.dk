#!/bin/bash

# Worktree Setup Script for Conductor - landbruget.dk
# Automates dependency installation and environment configuration for worktrees

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FRONTEND_DIR="$PROJECT_ROOT/frontend"
BACKEND_DIR="$PROJECT_ROOT/backend"

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

# Step 4: Set up Backend Python environment
echo "🐍 Setting up Backend Python environment..."
cd "$BACKEND_DIR"

if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
fi

echo "📦 Installing Python dependencies..."
source venv/bin/activate

if ! pip show pip-tools > /dev/null 2>&1; then
    pip install pip-tools
fi

if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
    echo "✅ Python dependencies installed"
elif [ -f "pyproject.toml" ]; then
    pip install -e .
    echo "✅ Python package installed in editable mode"
else
    echo "⚠️  No requirements.txt or pyproject.toml found"
fi
deactivate
echo ""

# Step 5: Verify .env is gitignored
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
echo "  2. cd frontend && npm run dev     # Start dev server"
echo "  3. cd backend && source venv/bin/activate && python -m pytest"
echo ""

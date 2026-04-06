#!/bin/bash

# Worktree Setup Script for Conductor - landbruget.dk
# Automates dependency installation and environment configuration for landbruget.dk worktrees

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FRONTEND_DIR="$PROJECT_ROOT/frontend"
BACKEND_DIR="$PROJECT_ROOT/backend"

# Find the main repository (where the original .env lives)
GIT_COMMON_DIR="$(cd "$PROJECT_ROOT" && git rev-parse --git-common-dir 2>/dev/null || echo "")"
if [ -n "$GIT_COMMON_DIR" ]; then
    MAIN_REPO_ROOT="$(cd "$PROJECT_ROOT" && cd "$GIT_COMMON_DIR/.." && pwd)"
    MAIN_FRONTEND_ENV="$MAIN_REPO_ROOT/frontend/.env"
    MAIN_BACKEND_ENV="$MAIN_REPO_ROOT/backend/.env"
else
    MAIN_REPO_ROOT=""
    MAIN_FRONTEND_ENV=""
    MAIN_BACKEND_ENV=""
fi

echo "🚀 Setting up landbruget.dk worktree..."
echo "📁 Worktree root: $PROJECT_ROOT"
if [ -n "$MAIN_REPO_ROOT" ] && [ "$MAIN_REPO_ROOT" != "$PROJECT_ROOT" ]; then
    echo "📁 Main repo root: $MAIN_REPO_ROOT"
fi
echo ""

# Step 1: Install Frontend Node.js dependencies
echo "📦 Installing Frontend Node.js dependencies..."
cd "$FRONTEND_DIR"
if [ -f "package-lock.json" ]; then
    npm ci
else
    npm install
fi
echo "✅ Frontend dependencies installed"
echo ""

# Step 2: Set up Frontend environment variables
FRONTEND_ENV_FILE="$FRONTEND_DIR/.env"
FRONTEND_ENV_EXAMPLE="$FRONTEND_DIR/.env.example"

echo "🔧 Setting up Frontend environment..."
if [ -L "$FRONTEND_ENV_FILE" ]; then
    # Already a symlink
    LINK_TARGET="$(readlink "$FRONTEND_ENV_FILE")"
    echo "✅ Frontend .env is already a symlink"
    echo "   → $LINK_TARGET"
elif [ -f "$FRONTEND_ENV_FILE" ]; then
    echo "✅ Frontend .env file already exists (regular file)"
else
    # Try to symlink from main repository first (worktree scenario)
    if [ -n "$MAIN_FRONTEND_ENV" ] && [ -f "$MAIN_FRONTEND_ENV" ] && [ "$MAIN_REPO_ROOT" != "$PROJECT_ROOT" ]; then
        ln -s "$MAIN_FRONTEND_ENV" "$FRONTEND_ENV_FILE"
        echo "✅ Created symlink to main repository frontend .env"
        echo "   $FRONTEND_ENV_FILE → $MAIN_FRONTEND_ENV"
        echo "   (Any changes to credentials in main repo will automatically apply here)"
    elif [ -f "$FRONTEND_ENV_EXAMPLE" ]; then
        cp "$FRONTEND_ENV_EXAMPLE" "$FRONTEND_ENV_FILE"
        echo "⚠️  Created frontend .env from .env.example (template only)"
        echo "   You'll need to add your credentials manually"
    else
        echo "⚠️  Warning: Neither main repo .env nor .env.example found for frontend"
    fi
fi
echo ""

# Step 3: Set up Backend Python environment
echo "🐍 Setting up Backend Python environment..."
cd "$BACKEND_DIR"

# Check if Python virtual environment exists
if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
fi

# Activate virtual environment and install dependencies
echo "📦 Installing Python dependencies..."
source venv/bin/activate

# Install pip-tools if not present
if ! pip show pip-tools > /dev/null 2>&1; then
    pip install pip-tools
fi

# Install dependencies if requirements file exists
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

# Step 4: Set up Backend environment variables
BACKEND_ENV_FILE="$BACKEND_DIR/.env"
BACKEND_ENV_EXAMPLE="$BACKEND_DIR/.env.example"

echo "🔧 Setting up Backend environment..."
if [ -L "$BACKEND_ENV_FILE" ]; then
    # Already a symlink
    LINK_TARGET="$(readlink "$BACKEND_ENV_FILE")"
    echo "✅ Backend .env is already a symlink"
    echo "   → $LINK_TARGET"
elif [ -f "$BACKEND_ENV_FILE" ]; then
    echo "✅ Backend .env file already exists (regular file)"
else
    # Try to symlink from main repository first (worktree scenario)
    if [ -n "$MAIN_BACKEND_ENV" ] && [ -f "$MAIN_BACKEND_ENV" ] && [ "$MAIN_REPO_ROOT" != "$PROJECT_ROOT" ]; then
        ln -s "$MAIN_BACKEND_ENV" "$BACKEND_ENV_FILE"
        echo "✅ Created symlink to main repository backend .env"
        echo "   $BACKEND_ENV_FILE → $MAIN_BACKEND_ENV"
        echo "   (Any changes to credentials in main repo will automatically apply here)"
    elif [ -f "$BACKEND_ENV_EXAMPLE" ]; then
        cp "$BACKEND_ENV_EXAMPLE" "$BACKEND_ENV_FILE"
        echo "⚠️  Created backend .env from .env.example (template only)"
        echo "   You'll need to add your credentials manually"
    else
        echo "⚠️  Warning: Neither main repo .env nor .env.example found for backend"
    fi
fi
echo ""

# Step 5: Set up Backend Pipeline environment variables (Google Cloud secrets etc.)
PIPELINES_DIR="$BACKEND_DIR/pipelines"
PIPELINES_ENV_FILE="$PIPELINES_DIR/.env"
PIPELINES_ENV_EXAMPLE="$PIPELINES_DIR/.env.example"

if [ -n "$MAIN_REPO_ROOT" ] && [ "$MAIN_REPO_ROOT" != "$PROJECT_ROOT" ]; then
    MAIN_PIPELINES_ENV="$MAIN_REPO_ROOT/backend/pipelines/.env"
else
    MAIN_PIPELINES_ENV=""
fi

echo "🔧 Setting up Pipeline environment (Google Cloud secrets etc.)..."
if [ -L "$PIPELINES_ENV_FILE" ]; then
    LINK_TARGET="$(readlink "$PIPELINES_ENV_FILE")"
    echo "✅ Pipeline .env is already a symlink"
    echo "   → $LINK_TARGET"
elif [ -f "$PIPELINES_ENV_FILE" ]; then
    echo "✅ Pipeline .env file already exists (regular file)"
else
    if [ -n "$MAIN_PIPELINES_ENV" ] && [ -f "$MAIN_PIPELINES_ENV" ]; then
        ln -s "$MAIN_PIPELINES_ENV" "$PIPELINES_ENV_FILE"
        echo "✅ Created symlink to main repository pipeline .env"
        echo "   $PIPELINES_ENV_FILE → $MAIN_PIPELINES_ENV"
    elif [ -f "$PIPELINES_ENV_EXAMPLE" ]; then
        cp "$PIPELINES_ENV_EXAMPLE" "$PIPELINES_ENV_FILE"
        echo "⚠️  Created pipeline .env from .env.example (template only)"
        echo "   You'll need to add your Google Cloud credentials manually"
    else
        echo "⚠️  Warning: Neither main repo .env nor .env.example found for pipelines"
    fi
fi
echo ""

# Step 5b: Run direnv allow if .envrc exists and direnv is installed
echo "🔧 Setting up direnv..."
if command -v direnv &> /dev/null; then
    if [ -f "$PROJECT_ROOT/.envrc" ]; then
        cd "$PROJECT_ROOT"
        direnv allow
        echo "✅ direnv allow executed for $PROJECT_ROOT/.envrc"
    else
        echo "⚠️  No .envrc found at project root"
    fi
else
    echo "⚠️  direnv not installed (install with: brew install direnv)"
fi
echo ""

# Step 6: Check for required Supabase environment variables
echo "🔐 Checking Supabase environment variables..."
MISSING_VARS=()

if [ -f "$FRONTEND_ENV_FILE" ]; then
    if ! grep -q "^NEXT_PUBLIC_API_URL=" "$FRONTEND_ENV_FILE" 2>/dev/null || grep -q "^NEXT_PUBLIC_API_URL=$" "$FRONTEND_ENV_FILE" 2>/dev/null; then
        MISSING_VARS+=("NEXT_PUBLIC_API_URL (Supabase API URL)")
    fi

    if ! grep -q "^NEXT_PUBLIC_API_KEY=" "$FRONTEND_ENV_FILE" 2>/dev/null || grep -q "^NEXT_PUBLIC_API_KEY=$" "$FRONTEND_ENV_FILE" 2>/dev/null; then
        MISSING_VARS+=("NEXT_PUBLIC_API_KEY (Supabase anon/public key)")
    fi
fi

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
    echo "⚠️  Missing Supabase environment variables detected!"
    echo ""
    echo "Required variables not configured in $FRONTEND_ENV_FILE:"
    for var in "${MISSING_VARS[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "📚 To configure these variables:"
    echo "   1. Go to https://supabase.com/dashboard"
    echo "   2. Select your landbruget.dk project"
    echo "   3. Go to Settings > API"
    echo "   4. Copy the following values:"
    echo "      - Project URL → NEXT_PUBLIC_API_URL"
    echo "      - Project API keys > anon/public → NEXT_PUBLIC_API_KEY"
    echo ""
    echo "   5. Edit $FRONTEND_ENV_FILE"
    echo "   6. Replace the placeholder values with your actual keys"
    echo ""
    echo "⚠️  DO NOT commit real secrets to git!"
    echo ""
else
    echo "✅ All Supabase environment variables are configured"
    echo ""
fi

# Step 7: Verify .env files are gitignored
GITIGNORE_FILE="$PROJECT_ROOT/.gitignore"
if [ -f "$GITIGNORE_FILE" ]; then
    if grep -q "\.env" "$GITIGNORE_FILE"; then
        echo "✅ .env files are gitignored (secrets protected)"
    else
        echo "⚠️  Warning: .env may not be gitignored - check $GITIGNORE_FILE"
    fi
else
    echo "⚠️  Warning: .gitignore not found"
fi
echo ""

# Step 8: Install oxlint if not present
echo "🔍 Checking oxlint installation..."
cd "$FRONTEND_DIR"
if npm list oxlint > /dev/null 2>&1; then
    echo "✅ oxlint is installed"
else
    echo "⚠️  oxlint not found in package.json dependencies"
fi
echo ""

# Step 9: Summary
echo "✨ Worktree setup complete!"
echo ""
echo "Next steps:"
echo "  Frontend:"
echo "    1. Configure any missing environment variables in frontend/.env"
echo "    2. Run tests: cd frontend && npm test"
echo "    3. Start dev server: cd frontend && npm run dev"
echo ""
echo "  Backend:"
echo "    1. Configure any missing environment variables in backend/.env"
echo "    2. Activate Python venv: cd backend && source venv/bin/activate"
echo "    3. Run tests: python -m pytest"
echo "    4. Run pipelines: cd pipelines/<pipeline_name> && python main.py"
echo ""
echo "  Database:"
echo "    1. Ensure Supabase CLI is installed: brew install supabase/tap/supabase"
echo "    2. Link to remote: cd .. && supabase link --project-ref <your-project-ref>"
echo "    3. Pull schema: supabase db pull"
echo ""

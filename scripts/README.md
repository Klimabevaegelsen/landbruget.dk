# scripts/

Utility scripts for repository setup and maintenance.

## Scripts

### `setup-worktree.sh`

Automates dependency installation and environment configuration for git worktrees. Designed to get a new worktree fully operational in one command.

**What it does (in order):**

1. Installs frontend Node.js dependencies (`npm ci` or `npm install`)
2. Sets up frontend `.env` -- symlinks to main repo's `.env` if in a worktree, otherwise copies from `.env.example`
3. Creates a Python virtual environment in `backend/venv` (if not present) and installs dependencies from `requirements.txt`
4. Sets up backend `.env` using the same symlink-or-copy strategy
5. Validates that required Supabase environment variables (`NEXT_PUBLIC_API_URL`, `NEXT_PUBLIC_API_KEY`) are configured
6. Verifies `.env` files are covered by `.gitignore`
7. Checks that `oxlint` is available in the frontend

**Usage:**

```bash
./scripts/setup-worktree.sh
```

The script exits on any error (`set -e`). It auto-detects whether it is running inside a worktree and adjusts its `.env` strategy accordingly: worktrees get symlinks to the main repo's credentials so changes propagate automatically; non-worktree checkouts copy from `.env.example` as a template.

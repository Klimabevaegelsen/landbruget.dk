# scripts/

Utility scripts for repository setup and maintenance.

## Scripts

### `setup-worktree.sh`

Automates dependency installation and environment configuration for git worktrees. Designed to get a new worktree fully operational in one command.

**What it does (in order):**

1. Installs frontend Node.js dependencies (`npm ci` or `npm install`)
2. Sets up the root `.env` -- symlinks to the main repo's `.env` if in a worktree, otherwise copies from `.env.example`
3. Runs `npm exec -- playwright install` so `npm test` works immediately
4. Installs backend dependencies with `uv sync --python 3.11 --all-packages --group dev`
5. Verifies the local `oxlint`, `playwright`, and `pytest` commands resolve successfully
6. Confirms `.env` files are covered by `.gitignore`

**Usage:**

```bash
./scripts/setup-worktree.sh
```

The script exits on any error (`set -e`). It auto-detects whether it is running inside a worktree and adjusts its `.env` strategy accordingly: worktrees get symlinks to the main repo's credentials so changes propagate automatically; non-worktree checkouts copy from `.env.example` as a template. Python setup is `uv`-based, pinned to Python 3.11 via `.python-version`; no `venv` activation or `requirements.txt` install step is required.

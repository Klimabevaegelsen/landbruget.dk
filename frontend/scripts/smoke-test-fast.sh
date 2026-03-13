#!/bin/bash

# Fast smoke test script - skips server startup for pre-commit hooks
# Only runs if a dev server is already running on port 3000

set -e

if lsof -ti:3000 >/dev/null 2>&1; then
    echo "🚀 Dev server found, running smoke tests..."
    npx playwright test tests/example.spec.ts --config=playwright.smoke.config.ts
    echo "✅ Smoke tests completed successfully!"
else
    echo "⚡ No dev server running - skipping smoke tests (run 'npm test' for full suite)"
    exit 0
fi

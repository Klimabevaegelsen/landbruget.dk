#!/bin/bash

# Fast smoke test script - skips server startup for pre-commit hooks
# Only runs if a dev server is already running on port 3000

set -e

project_server=""
for pid in $(lsof -ti:3000 2>/dev/null); do
    cwd=$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p')
    if [[ "$cwd" == "$PWD" || "$cwd" == "$PWD"/* ]]; then
        project_server="$pid"
        break
    fi
done

if [ -n "$project_server" ]; then
    echo "🚀 Frontend dev server found, running smoke tests..."
    node node_modules/@playwright/test/cli.js test tests/example.spec.ts --config=playwright.smoke.config.ts
    echo "✅ Smoke tests completed successfully!"
else
    echo "⚡ No frontend dev server running - skipping smoke tests (run 'npm test' for full suite)"
    exit 0
fi

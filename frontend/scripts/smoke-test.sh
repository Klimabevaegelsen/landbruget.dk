#!/bin/bash

# Smoke test script with robust process cleanup
# This ensures no hanging processes interfere with the test

set -e

echo "🧹 Cleaning up any existing processes..."

# Kill any existing Next.js dev servers
pkill -f "next dev" 2>/dev/null && echo "✓ Killed existing Next.js dev servers" || echo "✓ No existing Next.js dev servers found"

# Free up port 3000
if lsof -ti:3000 >/dev/null 2>&1; then
    echo "🔧 Port 3000 is in use, freeing it..."
    lsof -ti:3000 | xargs kill -9 2>/dev/null && echo "✓ Port 3000 freed" || echo "⚠️  Failed to free port 3000"
    sleep 2
else
    echo "✓ Port 3000 is already free"
fi

# Kill any hanging Playwright processes
pkill -f "playwright" 2>/dev/null && echo "✓ Killed hanging Playwright processes" || echo "✓ No hanging Playwright processes found"

echo "🚀 Starting Playwright smoke tests..."

# Run the actual smoke tests
npx playwright test tests/example.spec.ts --config=playwright.smoke.config.ts

echo "✅ Smoke tests completed successfully!"

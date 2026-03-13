#!/bin/bash

# Fast smoke test - skips process cleanup, assumes port is free
# Use this for quick pre-commit validation

set -e

echo "🚀 Running fast smoke tests..."

npx playwright test tests/example.spec.ts --config=playwright.smoke.config.ts

echo "✅ Fast smoke tests completed!"

#!/bin/bash

# Vercel build ignore script for pesticide frontend
# This script determines whether Vercel should build this project

echo "🔍 Checking if build should proceed for frontend-pesticide..."

# Always build on main/master branch deployments
if [[ "$VERCEL_GIT_COMMIT_REF" == "main" ]] || [[ "$VERCEL_GIT_COMMIT_REF" == "master" ]]; then
  echo "📦 Main branch deployment - checking for frontend-pesticide changes..."

  # Check if there are changes in the frontend-pesticide directory
  if git diff HEAD^ HEAD --quiet -- .; then
    echo "🛑 No changes detected in frontend-pesticide/ - Build cancelled"
    exit 1
  else
    echo "✅ Frontend-pesticide changes detected - Build proceeding"
    exit 0
  fi
fi

# For feature branches, always build (for preview deployments)
echo "🌿 Feature branch deployment - Build proceeding"
exit 0

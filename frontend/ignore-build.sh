#!/bin/bash

# Vercel build ignore script for main frontend
# This script determines whether Vercel should build this project

echo "🔍 Checking if build should proceed for frontend..."

# Always build on main/master branch deployments
if [[ "$VERCEL_GIT_COMMIT_REF" == "main" ]] || [[ "$VERCEL_GIT_COMMIT_REF" == "master" ]]; then
  echo "📦 Main branch deployment - checking for frontend changes..."

  # Check if there are changes in the frontend directory
  if git diff HEAD^ HEAD --quiet -- .; then
    echo "🛑 No changes detected in frontend/ - Build cancelled"
    exit 0
  else
    echo "✅ Frontend changes detected - Build proceeding"
    exit 1
  fi
fi

# For feature branches, always build (for preview deployments)
echo "🌿 Feature branch deployment - Build proceeding"
exit 1

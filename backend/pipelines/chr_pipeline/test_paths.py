#!/usr/bin/env python3
"""Test script to verify path configurations."""

import os
from pathlib import Path
import json

# Test paths
test_paths = [
    "/usr/data/bronze/chr",
    "/usr/data/silver/chr"
]

# Create test files
timestamp = "test_timestamp"
for base_path in test_paths:
    test_dir = Path(base_path) / timestamp
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # Create a test file
    test_file = test_dir / "_temp_test_file.json"
    with open(test_file, 'w') as f:
        json.dump({"test": "data"}, f)
    
    print(f"Created test file: {test_file}")

# Verify files exist
for base_path in test_paths:
    test_file = Path(base_path) / timestamp / "_temp_test_file.json"
    if test_file.exists():
        print(f"✅ Test file exists: {test_file}")
    else:
        print(f"❌ Test file does not exist: {test_file}")

print("Test completed.")

#!/usr/bin/env python3
"""Debug script to check paths and create test files."""

import json
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Test paths
test_paths = ["/usr/data/bronze/chr", "/usr/data/silver/chr", "/data/bronze/chr", "/data/silver/chr"]

# Create timestamp directory
timestamp = "debug_test"

# Check and create directories
for base_path in test_paths:
    path = Path(base_path)

    # Check if directory exists
    if path.exists():
        logger.info(f"✅ Directory exists: {path}")

        # Create timestamp directory
        test_dir = path / timestamp
        test_dir.mkdir(parents=True, exist_ok=True)

        # Create a test file
        test_file = test_dir / "_temp_test_file.json"
        with open(test_file, "w") as f:
            json.dump({"test": "data"}, f)

        logger.info(f"Created test file: {test_file}")
    else:
        logger.info(f"❌ Directory does not exist: {path}")
        try:
            # Try to create the directory
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {path}")

            # Create timestamp directory
            test_dir = path / timestamp
            test_dir.mkdir(parents=True, exist_ok=True)

            # Create a test file
            test_file = test_dir / "_temp_test_file.json"
            with open(test_file, "w") as f:
                json.dump({"test": "data"}, f)

            logger.info(f"Created test file: {test_file}")
        except Exception as e:
            logger.error(f"Failed to create directory {path}: {e}")

# List all directories in /
logger.info("Listing root directories:")
for item in Path("/").iterdir():
    if item.is_dir():
        logger.info(f"  {item}")

# List all directories in /usr
if Path("/usr").exists():
    logger.info("Listing /usr directories:")
    for item in Path("/usr").iterdir():
        if item.is_dir():
            logger.info(f"  {item}")

# List all directories in /data if it exists
if Path("/data").exists():
    logger.info("Listing /data directories:")
    for item in Path("/data").iterdir():
        if item.is_dir():
            logger.info(f"  {item}")

logger.info("Debug completed.")

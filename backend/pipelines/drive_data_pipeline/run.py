#!/usr/bin/env python
"""Script to run the Google Drive Data Pipeline."""

import os
import sys

# Add the parent directory to sys.path to enable imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Run the pipeline
from drive_data_pipeline.main import main

if __name__ == "__main__":
    sys.exit(main()) 
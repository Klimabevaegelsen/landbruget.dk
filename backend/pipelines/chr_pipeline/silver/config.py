import os
from pathlib import Path

# Base paths - handle both local and GCS environments
if os.getenv("GITHUB_ACTIONS"):
    # In GitHub Actions, use /tmp for local file operations
    BASE_DIR = Path("/tmp")
else:
    # In local environment, use a data directory in the workspace
    BASE_DIR = Path("/usr/data")

BRONZE_BASE_DIR = BASE_DIR / "bronze" / "chr"
SILVER_BASE_DIR = BASE_DIR / "silver" / "chr"
PIPELINE_DIR = Path(__file__).resolve().parent

# Configuration overrides
# Specify the date folder for bronze data, or use the latest
# Set to None to automatically find the latest dated folder
# Example: BRONZE_DATE_FOLDER_OVERRIDE = "20231027_100000"
BRONZE_DATE_FOLDER_OVERRIDE = None

# Coordinate Reference Systems (CRS)
SOURCE_CRS = "EPSG:25832"  # Assuming UTM zone 32N for Denmark CHR data
TARGET_CRS = "EPSG:4326"  # WGS 84

import os
from pathlib import Path

# Base paths - handle both local and GCS environments
if os.getenv("GITHUB_ACTIONS"):
    # In GitHub Actions, use LOCAL_DATA_PATH if set, otherwise /tmp
    local_data_path = os.getenv("LOCAL_DATA_PATH", "/tmp")
    BASE_DIR = Path(local_data_path)
else:
    # In local environment, use a data directory in the workspace
    BASE_DIR = Path("/usr/data")

SILVER_BASE_DIR = BASE_DIR / "silver" / "chr"
GOLD_BASE_DIR = BASE_DIR / "gold" / "chr"
PIPELINE_DIR = Path(__file__).resolve().parent

# Configuration overrides
# Specify the date folder for silver data, or use the latest
# Set to None to automatically find the latest dated folder
# Example: SILVER_DATE_FOLDER_OVERRIDE = "20231027_100000"
SILVER_DATE_FOLDER_OVERRIDE = None

# Coordinate Reference Systems (CRS)
SOURCE_CRS = "EPSG:25832"  # Assuming UTM zone 32N for Denmark CHR data
TARGET_CRS = "EPSG:4326"  # WGS 84

#!/usr/bin/env python3
"""
PMTiles Inspection Script for Buildings PMTiles Investigation.

This script downloads and inspects the generated buildings_proximity.pmtiles file
to verify its content, structure, and coordinate bounds.
"""

import asyncio
import json
import logging
import subprocess
from pathlib import Path
from typing import Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class PMTilesInspector:
    """Inspects PMTiles files for debugging purposes."""

    def __init__(self):
        self.base_url = "https://www.landbruget.dk/pmtiles"
        self.temp_dir = Path("data/generated")
        self.temp_dir.mkdir(exist_ok=True)

    async def inspect_buildings_pmtiles(self):
        """Main inspection process for buildings PMTiles."""
        logger.info("🔍 Starting PMTiles inspection...")

        try:
            # 1. Download PMTiles file
            pmtiles_path = await self._download_pmtiles("buildings_proximity.pmtiles")

            if not pmtiles_path:
                logger.error("❌ Failed to download buildings PMTiles")
                return

            # 2. Inspect file metadata
            await self._inspect_metadata(pmtiles_path)

            # 3. Analyze coordinate bounds
            await self._analyze_bounds(pmtiles_path)

            # 4. Count features
            await self._count_features(pmtiles_path)

            # 5. Compare with working PMTiles
            await self._compare_with_working_pmtiles(pmtiles_path)

            logger.info("✅ PMTiles inspection completed")

        except Exception as e:
            logger.error(f"❌ Inspection failed: {e}")
            raise

    async def _download_pmtiles(self, filename: str) -> Optional[Path]:
        """Download PMTiles file from the server."""
        logger.info(f"📥 Downloading {filename}...")

        url = f"{self.base_url}/{filename}"
        local_path = self.temp_dir / filename

        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(local_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            file_size = local_path.stat().st_size
            logger.info(f"✅ Downloaded {filename}: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")

            return local_path

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Failed to download {filename}: {e}")
            return None

    async def _inspect_metadata(self, pmtiles_path: Path):
        """Inspect PMTiles metadata using pmtiles CLI."""
        logger.info("📊 Inspecting PMTiles metadata...")

        try:
            # Check if pmtiles CLI is available
            result = subprocess.run(["pmtiles", "show", str(pmtiles_path)], capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                logger.info("📋 PMTiles Metadata:")
                for line in result.stdout.strip().split("\n"):
                    logger.info(f"   {line}")
            else:
                logger.warning(f"⚠️ pmtiles CLI not available or failed: {result.stderr}")
                # Fallback to basic file inspection
                await self._basic_file_inspection(pmtiles_path)

        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.warning(f"⚠️ pmtiles CLI not available: {e}")
            await self._basic_file_inspection(pmtiles_path)

    async def _basic_file_inspection(self, pmtiles_path: Path):
        """Basic file inspection without pmtiles CLI."""
        logger.info("📋 Basic file inspection:")

        file_size = pmtiles_path.stat().st_size
        logger.info(f"   File size: {file_size:,} bytes ({file_size/1024/1024:.1f} MB)")

        # Read first few bytes to check file signature
        with open(pmtiles_path, "rb") as f:
            header = f.read(16)
            logger.info(f"   File header: {header.hex()}")

            # PMTiles files should start with specific magic bytes
            if header[:8] == b"PMTiles\x03":
                logger.info("   ✅ Valid PMTiles file signature")
            else:
                logger.warning("   ⚠️ Unexpected file signature - may not be valid PMTiles")

    async def _analyze_bounds(self, pmtiles_path: Path):
        """Analyze coordinate bounds of the PMTiles."""
        logger.info("🗺️ Analyzing coordinate bounds...")

        try:
            # Try to extract bounds using pmtiles CLI
            result = subprocess.run(
                ["pmtiles", "show", str(pmtiles_path), "--json"], capture_output=True, text=True, timeout=30
            )

            if result.returncode == 0:
                metadata = json.loads(result.stdout)

                if "bounds" in metadata:
                    bounds = metadata["bounds"]
                    logger.info(f"📍 Coordinate bounds: {bounds}")

                    # Check if bounds are reasonable for Denmark
                    min_lon, min_lat, max_lon, max_lat = bounds

                    # Denmark approximate bounds: 8-15°E, 54.5-57.8°N
                    if 8 <= min_lon <= 15 and 8 <= max_lon <= 15 and 54 <= min_lat <= 58 and 54 <= max_lat <= 58:
                        logger.info("   ✅ Bounds appear to be within Denmark")
                    else:
                        logger.warning(
                            f"   ⚠️ Bounds may be outside Denmark: lon {min_lon}-{max_lon}, lat {min_lat}-{max_lat}"
                        )

                if "zoom" in metadata:
                    zoom_info = metadata["zoom"]
                    logger.info(f"🔍 Zoom levels: {zoom_info}")

                if "type" in metadata:
                    logger.info(f"📝 Layer type: {metadata['type']}")

        except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"⚠️ Could not analyze bounds: {e}")

    async def _count_features(self, pmtiles_path: Path):
        """Count features in the PMTiles file."""
        logger.info("🔢 Counting features...")

        try:
            # Try to extract feature count using pmtiles CLI
            result = subprocess.run(
                ["pmtiles", "extract", str(pmtiles_path), "--dry-run"], capture_output=True, text=True, timeout=60
            )

            if result.returncode == 0:
                # Parse output for feature count information
                output = result.stdout
                logger.info("📊 Feature extraction info:")
                for line in output.strip().split("\n"):
                    if line.strip():
                        logger.info(f"   {line}")
            else:
                logger.warning(f"⚠️ Could not extract feature count: {result.stderr}")

        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            logger.warning(f"⚠️ Could not count features: {e}")

    async def _compare_with_working_pmtiles(self, buildings_pmtiles_path: Path):
        """Compare with known working PMTiles files."""
        logger.info("🔄 Comparing with working PMTiles...")

        # Download and compare with field analysis PMTiles (known to work)
        working_files = ["field_analysis_2023.pmtiles", "bnbo_areas.pmtiles"]

        for working_file in working_files:
            logger.info(f"📊 Comparing with {working_file}...")

            working_path = await self._download_pmtiles(working_file)
            if not working_path:
                continue

            # Compare file sizes
            buildings_size = buildings_pmtiles_path.stat().st_size
            working_size = working_path.stat().st_size

            logger.info("   File sizes:")
            logger.info(f"     Buildings: {buildings_size:,} bytes ({buildings_size/1024/1024:.1f} MB)")
            logger.info(f"     {working_file}: {working_size:,} bytes ({working_size/1024/1024:.1f} MB)")

            # Compare metadata if possible
            await self._compare_metadata(buildings_pmtiles_path, working_path, working_file)

    async def _compare_metadata(self, buildings_path: Path, working_path: Path, working_name: str):
        """Compare metadata between buildings and working PMTiles."""
        try:
            # Get metadata for both files
            buildings_result = subprocess.run(
                ["pmtiles", "show", str(buildings_path), "--json"], capture_output=True, text=True, timeout=30
            )

            working_result = subprocess.run(
                ["pmtiles", "show", str(working_path), "--json"], capture_output=True, text=True, timeout=30
            )

            if buildings_result.returncode == 0 and working_result.returncode == 0:
                buildings_meta = json.loads(buildings_result.stdout)
                working_meta = json.loads(working_result.stdout)

                logger.info(f"   📋 Metadata comparison with {working_name}:")

                # Compare key fields
                for key in ["bounds", "zoom", "type"]:
                    if key in buildings_meta and key in working_meta:
                        logger.info(f"     {key}:")
                        logger.info(f"       Buildings: {buildings_meta[key]}")
                        logger.info(f"       {working_name}: {working_meta[key]}")
                    elif key in buildings_meta:
                        logger.info(f"     {key}: Buildings has it, {working_name} doesn't")
                    elif key in working_meta:
                        logger.info(f"     {key}: {working_name} has it, Buildings doesn't")

        except Exception as e:
            logger.warning(f"⚠️ Could not compare metadata: {e}")


async def main():
    """Main function."""
    inspector = PMTilesInspector()
    await inspector.inspect_buildings_pmtiles()


if __name__ == "__main__":
    asyncio.run(main())

"""Dynamic year detection for PMTiles data sources."""

import asyncio
import logging
import re
from pathlib import Path
from typing import Dict, List, Tuple

from unified_pipeline.util.gcs_access import GCSDataAccess

from .config import PMTilesGeneratorConfig

logger = logging.getLogger(__name__)


class DataSourceYearDetector:
    """Detects available years for different data sources in GCS."""

    def __init__(self, config: PMTilesGeneratorConfig, gcs_access: GCSDataAccess):
        """Initialize the year detector.

        Args:
            config: PMTiles generator configuration
            gcs_access: GCS data access instance
        """
        self.config = config
        self.gcs = gcs_access

    async def detect_all_available_years(self) -> Dict[str, List[int]]:
        """Detect available years for all data sources.

        Returns:
            Dictionary mapping data source names to lists of available years
        """
        logger.info("Detecting available years for all data sources...")

        # Define data source patterns
        data_sources = {
            "fvm_marker": "silver/fvm_marker_",
            "field_environmental": "gold/field_environmental_analysis_fields_",
            "field_production": "gold/field_production_",
            "pesticide_proximity": "gold/pesticide_proximity_",
            "nles5_estimation": "gold/nles5_nitrogen_estimation/",
        }

        # Run detection in parallel
        tasks = []
        for source_name, path_pattern in data_sources.items():
            if source_name == "pesticide_proximity":
                task = self._detect_pesticide_proximity_years()
            elif source_name == "nles5_estimation":
                task = self._detect_nles5_years()
            else:
                task = self._detect_years_for_pattern(source_name, path_pattern)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Combine results
        available_years = {}
        for i, (source_name, _) in enumerate(data_sources.items()):
            result = results[i]
            if isinstance(result, Exception):
                logger.warning(f"Failed to detect years for {source_name}: {result}")
                available_years[source_name] = []
            else:
                available_years[source_name] = result
                logger.info(f"Found {len(result)} years for {source_name}: {sorted(result)}")

        return available_years

    async def _detect_years_for_pattern(self, source_name: str, path_pattern: str) -> List[int]:
        """Detect years for a simple path pattern like 'silver/fvm_marker_YYYY/'.

        Args:
            source_name: Name of the data source
            path_pattern: Path pattern to search for

        Returns:
            List of available years
        """
        try:
            # Use direct glob pattern like pesticide_proximity does
            # For "silver/fvm_marker_" -> use "silver/fvm_marker_*"
            direct_pattern = f"gs://{self.config.gcs_bucket}/{path_pattern}*"
            all_paths = await asyncio.to_thread(self.gcs.list_files, direct_pattern)

            # Extract years using regex like pesticide_proximity does
            years = set()
            base_name = path_pattern.split("/")[-1]  # e.g., "fvm_marker_"
            
            # Create regex pattern: "fvm_marker_(\d{4})"
            year_pattern = re.compile(rf"{re.escape(base_name)}(\d{{4}})/?$")
            
            for path in all_paths:
                # Get just the directory name (last part of path)
                dir_name = path.split("/")[-1]
                
                # Match the pattern
                match = year_pattern.match(dir_name)
                if match:
                    year = int(match.group(1))
                    # Validate year is reasonable (2000-2030)
                    if 2000 <= year <= 2030:
                        years.add(year)

            logger.info(f"Found {len(years)} years for {source_name}: {sorted(years)}")
            return sorted(list(years))

        except Exception as e:
            logger.error(f"Error detecting years for {source_name}: {e}")
            return []

    async def _detect_pesticide_proximity_years(self) -> List[int]:
        """Detect years for pesticide proximity data (YYYY_YYYY+1 pattern).

        Returns:
            List of available years (base years, not the +1 years)
        """
        try:
            # List all pesticide proximity directories
            all_paths = await asyncio.to_thread(
                self.gcs.list_files, f"gs://{self.config.gcs_bucket}/gold/*"
            )

            years = set()
            # Pattern: pesticide_proximity_YYYY_YYYY+1
            pattern = re.compile(r"pesticide_proximity_(\d{4})_(\d{4})/?$")

            for path in all_paths:
                path_parts = path.replace(f"gs://{self.config.gcs_bucket}/", "").split("/")

                for part in path_parts:
                    match = pattern.match(part)
                    if match:
                        base_year = int(match.group(1))
                        next_year = int(match.group(2))

                        # Validate the year pattern (next_year should be base_year + 1)
                        if next_year == base_year + 1 and 2000 <= base_year <= 2030:
                            years.add(base_year)

            return sorted(list(years))

        except Exception as e:
            logger.error(f"Error detecting pesticide proximity years: {e}")
            return []

    async def _detect_nles5_years(self) -> List[int]:
        """Detect years for NLES5 nitrogen estimation data.

        Returns:
            List of available years from the NLES5 dataset
        """
        try:
            # NLES5 data is in a single 'latest' directory, but contains multiple years
            # We need to check the actual data to see what years are available
            nles5_path = f"gs://{self.config.gcs_bucket}/gold/nles5_nitrogen_estimation/latest/"

            # Check if the path exists
            paths = await asyncio.to_thread(self.gcs.list_files, f"{nles5_path}*")
            if not paths:
                logger.warning("NLES5 data path not found")
                return []

            # For now, return the known years from the documentation
            # TODO: In a full implementation, we could query the actual data to get available years
            # Based on the plan, NLES5 covers 2021-2022 and can be extended to 2025
            known_nles5_years = [2021, 2022]

            logger.info(f"NLES5 data found, assuming years: {known_nles5_years}")
            return known_nles5_years

        except Exception as e:
            logger.error(f"Error detecting NLES5 years: {e}")
            return []

    async def get_optimal_year_ranges(self) -> Dict[str, Tuple[int, int, List[str]]]:
        """Get optimal year ranges based on available data sources.

        Returns:
            Dictionary with year range information:
            - 'historical': (start_year, end_year, [available_sources])
            - 'pesticide_era': (start_year, end_year, [available_sources])
            - 'enhanced_recent': (start_year, end_year, [available_sources])
        """
        available_years = await self.detect_all_available_years()

        # Get year ranges for each data source
        fvm_years = set(available_years.get("fvm_marker", []))
        environmental_years = set(available_years.get("field_environmental", []))
        production_years = set(available_years.get("field_production", []))
        pesticide_years = set(available_years.get("pesticide_proximity", []))
        nles5_years = set(available_years.get("nles5_estimation", []))

        ranges = {}

        # Historical range: Only FVM + Production (2008-2014 based on plan)
        historical_years = fvm_years & production_years
        historical_early = [y for y in historical_years if y < 2015]
        if historical_early:
            ranges["historical"] = (
                min(historical_early),
                max(historical_early),
                ["fvm_marker", "field_production"],
            )

        # Pesticide era: FVM + Production + Pesticide (2015-2023 existing)
        pesticide_era_years = fvm_years & production_years & pesticide_years
        pesticide_era_range = [y for y in pesticide_era_years if 2015 <= y <= 2023]
        if pesticide_era_range:
            ranges["pesticide_era"] = (
                min(pesticide_era_range),
                max(pesticide_era_range),
                ["fvm_marker", "field_production", "pesticide_proximity"],
            )

        # Enhanced recent: All data sources (2021+)
        enhanced_years = fvm_years & environmental_years & production_years
        enhanced_recent = [y for y in enhanced_years if y >= 2021]

        # Add pesticide and NLES5 where available
        sources = ["fvm_marker", "field_environmental", "field_production"]
        if any(y in pesticide_years for y in enhanced_recent):
            sources.append("pesticide_proximity")
        if any(y in nles5_years for y in enhanced_recent):
            sources.append("nles5_estimation")

        if enhanced_recent:
            ranges["enhanced_recent"] = (min(enhanced_recent), max(enhanced_recent), sources)

        # Log the detected ranges
        for range_name, (start, end, sources) in ranges.items():
            logger.info(
                f"{range_name}: {start}-{end} ({len(sources)} data sources: {', '.join(sources)})"
            )

        return ranges

    def get_years_to_process(self, available_years: Dict[str, List[int]]) -> List[int]:
        """Get the final list of years to process based on configuration and available data.

        Args:
            available_years: Dictionary of available years per data source

        Returns:
            Sorted list of years to process
        """
        if self.config.target_years:
            # Use explicitly specified years
            target_years = set(self.config.target_years)
            logger.info(f"Using explicitly configured target years: {sorted(target_years)}")
        else:
            # Auto-detect based on available data
            # Use FVM marker as the base since it's required for all PMTiles
            fvm_years = set(available_years.get("fvm_marker", []))
            target_years = fvm_years
            logger.info(
                f"Auto-detected years based on FVM marker availability: {sorted(target_years)}"
            )

        # Apply exclusions
        if self.config.exclude_years:
            excluded = set(self.config.exclude_years)
            target_years = target_years - excluded
            logger.info(f"Excluded years: {sorted(excluded)}")

        # Filter to reasonable range
        target_years = {y for y in target_years if 2000 <= y <= 2030}

        final_years = sorted(list(target_years))
        logger.info(f"Final years to process: {final_years}")

        return final_years

"""
CVR Geometry Datasets Gold Layer

This module implements Phase 1 of the CVR geometry linking system:
1. Extract CVR address points from CVR Enrichment dataset
2. Extract CHR property points by joining CHR Properties with CHR Property Owners

This follows the analysis in docs/analysis/cvr_geometry_datasets_analysis.md
"""

import os
from typing import Any, Dict, Optional

from pydantic import Field

from unified_pipeline.common.base import BaseJobConfig, BaseSource, GoldJobInterface


class CVRGeometryDatasetsConfig(BaseJobConfig):
    """Configuration for CVR Geometry Datasets gold layer."""

    name: str = "CVR Geometry Datasets"
    dataset: str = "cvr_geometry_datasets"
    type: str = "gold"
    description: str = "Link geometries (points and polygons) to CVR numbers for spatial analysis"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET", "landbrugsdata-raw-data")

    # Phase 1: Point geometries
    process_address_points: bool = Field(
        default=True, description="Process CVR address points from enrichment data"
    )
    process_chr_property_points: bool = Field(
        default=True, description="Process CHR property points linked to CVR"
    )

    # Phase 2: Property polygons
    process_property_polygons: bool = Field(
        default=True, description="Process property polygons (Phase 2)"
    )
    process_building_polygons: bool = Field(
        default=True, description="Process building polygons (Phase 3)"
    )

    # Data quality filters
    min_coordinate_quality: str = Field(
        default="B", description="Minimum coordinate quality (A=best, B=good, C=poor)"
    )
    include_historical_addresses: bool = Field(
        default=False, description="Include historical addresses or current only"
    )

    # Testing configuration
    test_limit: Optional[int] = Field(
        default=1000, description="Limit records for testing (None = no limit)"
    )


class CVRGeometryDatasets(BaseSource[CVRGeometryDatasetsConfig], GoldJobInterface):
    """CVR Geometry Datasets gold layer implementation."""

    def __init__(self, config: CVRGeometryDatasetsConfig):
        super().__init__(config)
        # Note: Not using GCSDataAccess for now since we're working with local files
        # self.gcs_access = GCSDataAccess()

    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> bool:
        """
        Run the CVR geometry datasets processing.

        Args:
            silver_data: Optional silver data (not used in this pipeline)

        Returns:
            bool: Success status
        """
        try:
            self.log.info("🗺️ Starting CVR Geometry Datasets processing (Phases 1-2)")

            # Phase 1: Point geometries
            results = {}

            if self.config.process_address_points:
                self.log.info("📍 Processing CVR address points...")
                address_points = await self._process_cvr_address_points()
                results["address_points"] = address_points
                self.log.info(f"✅ Processed {len(address_points)} CVR address points")

            if self.config.process_chr_property_points:
                self.log.info("🏡 Processing CHR property points...")
                property_points = await self._process_chr_property_points()
                results["property_points"] = property_points
                self.log.info(f"✅ Processed {len(property_points)} CHR property points")

            # Phase 2: Property polygons
            if self.config.process_property_polygons:
                self.log.info("🏘️ Processing CVR property polygons...")
                property_polygons = await self._process_cvr_property_polygons()
                results["property_polygons"] = property_polygons
                self.log.info(f"✅ Processed {len(property_polygons)} CVR property polygons")

            # Phase 3: Building polygons
            if self.config.process_building_polygons:
                self.log.info("🏗️ Processing CVR building polygons...")
                building_polygons = await self._process_cvr_building_polygons()
                results["building_polygons"] = building_polygons
                self.log.info(f"✅ Processed {len(building_polygons)} CVR building polygons")

            # Save results
            await self._save_results(results)

            self.log.info("✅ CVR Geometry Datasets Phases 1-2 completed successfully")
            return True

        except Exception as e:
            self.log.error(f"❌ CVR Geometry Datasets processing failed: {e}")
            raise

    async def _process_cvr_address_points(self) -> Dict[str, Any]:
        """
        Process CVR address points from enrichment data.

        Returns:
            Dict with processed address points data
        """
        self.log.info("Loading CVR enrichment addresses data...")

        # Load CVR enrichment addresses from local data
        query = """
        SELECT
            cvr_number,
            latitude,
            longitude,
            geometry_wkt,
            geometry_geojson,
            is_current,
            company_uuid,
            coordinate_quality,
            coordinate_source,
            full_address,
            postal_code,
            city
        FROM read_parquet('data_cache/cvr_geometry_test/cvr_addresses/data.parquet')
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND coordinate_quality IN ('A', 'B', 'C')
        """

        if not self.config.include_historical_addresses:
            query += " AND is_current = true"

        if self.config.test_limit:
            query += f" LIMIT {self.config.test_limit}"

        # Execute query
        df = self.conn.execute(query).df()

        self.log.info(f"📊 Loaded {len(df)} CVR address records")

        # Basic validation
        if df.empty:
            self.log.warning("No CVR address data found!")
            return {"count": 0, "data": None}

        # Log quality distribution
        if "coordinate_quality" in df.columns:
            quality_dist = df["coordinate_quality"].value_counts().to_dict()
            self.log.info(f"📈 Coordinate quality distribution: {quality_dist}")

        # Log current vs historical
        if "is_current" in df.columns:
            current_dist = df["is_current"].value_counts().to_dict()
            self.log.info(f"📈 Current vs historical: {current_dist}")

        return {
            "count": len(df),
            "data": df,
            "unique_cvrs": df["cvr_number"].nunique() if "cvr_number" in df.columns else 0,
        }

    async def _process_chr_property_points(self) -> Dict[str, Any]:
        """
        Process CHR property points linked to CVR numbers.

        Returns:
            Dict with processed CHR property points data
        """
        self.log.info("Loading CHR properties and property owners data...")

        # Join CHR properties with property owners to link CVR numbers using local data
        query = """
        WITH chr_cvr_properties AS (
            SELECT
                p.property_id,
                p.chr_number,
                p.geo_coord_x_source,
                p.geo_coord_y_source,
                ST_Transform(p.geometry, 'EPSG:25832', 'EPSG:4326') as geometry_wgs84,
                p.address,
                p.postal_code,
                p.city,
                po.owner_cvr,
                po.owner_name
            FROM read_parquet('data_cache/cvr_geometry_test/properties.parquet') p
            JOIN read_parquet('data_cache/cvr_geometry_test/property_owners.parquet') po
                ON p.chr_number = po.chr_number
            WHERE po.owner_cvr IS NOT NULL
              AND p.geo_coord_x_source IS NOT NULL
              AND p.geo_coord_y_source IS NOT NULL
        )
        SELECT *
        FROM chr_cvr_properties
        """

        if self.config.test_limit:
            query += f" LIMIT {self.config.test_limit}"

        # Execute query
        df = self.conn.execute(query).df()

        self.log.info(f"📊 Loaded {len(df)} CHR property records with CVR links")

        # Basic validation
        if df.empty:
            self.log.warning("No CHR property data found!")
            return {"count": 0, "data": None}

        # Log statistics
        if "owner_cvr" in df.columns:
            unique_cvrs = df["owner_cvr"].nunique()
            self.log.info(f"📈 Unique CVR numbers: {unique_cvrs}")

        return {
            "count": len(df),
            "data": df,
            "unique_cvrs": df["owner_cvr"].nunique() if "owner_cvr" in df.columns else 0,
        }

    async def _process_cvr_property_polygons(self) -> Dict[str, Any]:
        """
        Process CVR property polygons from Property Cadastral Merged dataset.

        Returns:
            Dict with processed property polygons data
        """
        self.log.info("Loading Property Cadastral Merged data...")

        # Extract CVR property polygons from cadastral data using local data
        query = """
        SELECT
            bfe_number,
            company_data.attributes.CVRNummer as cvr_number,
            company_data.attributes.navn as company_name,
            company_data.attributes.beliggenhedsadresse."CVRAdresse.vejnavn" as street_name,
            company_data.attributes.beliggenhedsadresse."CVRAdresse.postnummer" as postal_code,
            company_data.attributes.beliggenhedsadresse."CVRAdresse.postdistrikt" as city,
            ownership_numerator,
            ownership_denominator,
            ST_AsText(geometry) as geometry_wkt,
            ST_Area(geometry) as area_sqm,
            cadastral_registration_from,
            cadastral_effect_from
        FROM read_parquet('data_cache/cvr_geometry_test/property_cadastral_merged.parquet')
        WHERE company_data.attributes.CVRNummer IS NOT NULL
          AND geometry IS NOT NULL
        """

        if self.config.test_limit:
            query += f" LIMIT {self.config.test_limit}"

        # Execute query
        df = self.conn.execute(query).df()

        self.log.info(f"📊 Loaded {len(df)} CVR property polygons")

        # Basic validation
        if df.empty:
            self.log.warning("No CVR property polygon data found!")
            return {"count": 0, "data": None}

        # Log statistics
        if "cvr_number" in df.columns:
            unique_cvrs = df["cvr_number"].nunique()
            self.log.info(f"📈 Unique CVR numbers: {unique_cvrs}")

        # Log ownership statistics
        if "ownership_numerator" in df.columns and "ownership_denominator" in df.columns:
            full_ownership = (df["ownership_numerator"] == df["ownership_denominator"]).sum()
            partial_ownership = len(df) - full_ownership
            self.log.info(f"📈 Ownership: {full_ownership} full, {partial_ownership} partial")

        # Log area statistics
        if "area_sqm" in df.columns:
            total_area = df["area_sqm"].sum()
            avg_area = df["area_sqm"].mean()
            self.log.info(f"📈 Total area: {total_area:,.0f} m², Average: {avg_area:,.0f} m²")

        return {
            "count": len(df),
            "data": df,
            "unique_cvrs": df["cvr_number"].nunique() if "cvr_number" in df.columns else 0,
        }

    async def _process_cvr_building_polygons(self) -> Dict[str, Any]:
        """
        Process CVR building polygons from BBR Buildings dataset.

        This is Phase 3 of the CVR geometry linking system.

        Returns:
            Dict with processed building polygons data
        """
        self.log.info("Loading BBR Buildings data...")

        # For now, return empty data as this is Phase 3 (not yet implemented)
        # TODO: Implement building polygons processing when BBR data is available
        self.log.info("⚠️ Building polygons processing not yet implemented (Phase 3)")

        return {
            "count": 0,
            "data": None,
            "unique_cvrs": 0,
        }

    async def _save_results(self, results: Dict[str, Any]) -> None:
        """Save processing results to GCS."""

        timestamp = self.date_pattern

        # Save address points
        if "address_points" in results and results["address_points"]["data"] is not None:
            address_path = (
                f"gold/{self.config.dataset}/phase1_address_points/{timestamp}/data.parquet"
            )
            await self._save_dataframe(
                results["address_points"]["data"],
                address_path,
                f"CVR address points ({results['address_points']['count']} records)",
            )

        # Save CHR property points
        if "property_points" in results and results["property_points"]["data"] is not None:
            property_path = (
                f"gold/{self.config.dataset}/phase1_chr_property_points/{timestamp}/data.parquet"
            )
            await self._save_dataframe(
                results["property_points"]["data"],
                property_path,
                f"CHR property points ({results['property_points']['count']} records)",
            )

        # Save CVR property polygons (Phase 2)
        if "property_polygons" in results and results["property_polygons"]["data"] is not None:
            polygons_path = (
                f"gold/{self.config.dataset}/phase2_property_polygons/{timestamp}/data.parquet"
            )
            await self._save_dataframe(
                results["property_polygons"]["data"],
                polygons_path,
                f"CVR property polygons ({results['property_polygons']['count']} records)",
            )

        # Save summary metadata
        summary = {
            "processing_timestamp": timestamp,
            "phase": "1-2",
            "config": self.config.model_dump(),
            "results_summary": {
                key: {k: v for k, v in value.items() if k != "data"}
                for key, value in results.items()
            },
        }

        # Save summary locally for testing
        import json
        from pathlib import Path

        local_summary_path = (
            f"data_cache/cvr_geometry_test/output/metadata/{timestamp}/phase1_summary.json"
        )
        local_summary_file = Path(local_summary_path)
        local_summary_file.parent.mkdir(parents=True, exist_ok=True)

        with open(local_summary_file, "w") as f:
            json.dump(summary, f, indent=2, default=str)

        self.log.info(f"📋 Saved processing summary to: {local_summary_file}")

    async def _save_dataframe(self, df, path: str, description: str) -> None:
        """Save a dataframe locally for testing."""
        from pathlib import Path

        # Create local path in data_cache
        local_path = f"data_cache/cvr_geometry_test/output/{path}"
        local_file = Path(local_path)
        local_file.parent.mkdir(parents=True, exist_ok=True)

        # Convert UUIDs to strings for Parquet compatibility
        df_copy = df.copy()
        for col in df_copy.columns:
            if df_copy[col].dtype == "object":
                # Check if column contains UUIDs and convert to string
                try:
                    df_copy[col] = df_copy[col].astype(str)
                except (ValueError, TypeError):
                    pass

        # Save as parquet
        df_copy.to_parquet(local_file)

        self.log.info(f"💾 Saved {description} to: {local_file}")

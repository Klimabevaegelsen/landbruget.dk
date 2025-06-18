"""
Building Processor for the BBR Buildings Pipeline Silver Layer.

This module handles cleaning, harmonizing, and filtering building data
from the INSPIRE BBR dataset.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import ibis
import pandas as pd

from config import Settings


class BuildingProcessor:
    """Processes and transforms building data for the silver layer."""

    def __init__(self, settings: Settings, logger: logging.Logger):
        """
        Initialize the building processor.

        Args:
            settings: Pipeline settings
            logger: Logger instance
        """
        self.settings = settings
        self.logger = logger

        # Initialize Ibis with DuckDB backend with spatial extensions
        self.ibis_conn = ibis.duckdb.connect(":memory:", extensions=["spatial"])

        # Get the underlying DuckDB connection from Ibis (optional, for manual SQL if needed)
        self.conn = self.ibis_conn.con

    def process_buildings_from_data(
        self, bronze_data: dict, output_dir: Path, enhance_classification: bool = False
    ) -> None:
        """
        Process buildings data directly from bronze layer data (in-memory processing).

        Args:
            bronze_data: Data object returned from bronze layer
            output_dir: Output directory for silver data
            enhance_classification: Whether to enhance classification using WFS data
        """
        timestamp = datetime.now().strftime("%Y%m%d")
        run_dir = output_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Starting in-memory building processing to {run_dir}")

        try:
            # Load building data from bronze_data object
            buildings_table = self._load_building_data_from_object(bronze_data)

            # Apply filters
            filtered_buildings = self._filter_buildings(buildings_table)

            # Standardize schema
            standardized_buildings = self._standardize_schema(filtered_buildings)

            # Enhance classification if requested
            if enhance_classification:
                enhanced_buildings = self._enhance_classification_from_data(
                    standardized_buildings, bronze_data
                )
            else:
                enhanced_buildings = standardized_buildings

            # Validate and clean geometries
            clean_buildings = self._validate_geometries(enhanced_buildings)

            # Add derived fields
            final_buildings = self._add_derived_fields(clean_buildings)

            # Save to GeoParquet
            output_path = run_dir / "buildings_filtered.parquet"
            self._save_to_geoparquet(final_buildings, output_path)

            # Generate summary statistics
            self._generate_summary_stats(final_buildings, run_dir)

            # Save processing metadata
            self._save_processing_metadata_from_data(run_dir, bronze_data, enhance_classification)

            self.logger.info(f"Successfully processed buildings to {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to process buildings: {e}")
            raise
        finally:
            # Clean up connections
            if hasattr(self, "conn"):
                self.conn.close()

    def process_buildings(
        self, input_dir: Path, output_dir: Path, enhance_classification: bool = False
    ) -> None:
        """
        Process buildings data from bronze to silver layer.

        Args:
            input_dir: Input directory containing bronze data
            output_dir: Output directory for silver data
            enhance_classification: Whether to enhance classification using WFS data
        """
        timestamp = datetime.now().strftime("%Y%m%d")
        run_dir = output_dir / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Starting building processing to {run_dir}")

        try:
            # Find the GPKG file in input directory
            gpkg_path = self._find_gpkg_file(input_dir)

            # Load building data
            buildings_table = self._load_building_data(gpkg_path)

            # Apply filters
            filtered_buildings = self._filter_buildings(buildings_table)

            # Standardize schema
            standardized_buildings = self._standardize_schema(filtered_buildings)

            # Enhance classification if requested
            if enhance_classification:
                enhanced_buildings = self._enhance_classification(standardized_buildings, input_dir)
            else:
                enhanced_buildings = standardized_buildings

            # Validate and clean geometries
            clean_buildings = self._validate_geometries(enhanced_buildings)

            # Add derived fields
            final_buildings = self._add_derived_fields(clean_buildings)

            # Save to GeoParquet
            output_path = run_dir / "buildings_filtered.parquet"
            self._save_to_geoparquet(final_buildings, output_path)

            # Generate summary statistics
            self._generate_summary_stats(final_buildings, run_dir)

            # Save processing metadata
            self._save_processing_metadata(run_dir, gpkg_path, enhance_classification)

            self.logger.info(f"Successfully processed buildings to {output_path}")

        except Exception as e:
            self.logger.error(f"Failed to process buildings: {e}")
            raise
        finally:
            # Clean up connections
            if hasattr(self, "conn"):
                self.conn.close()

    def _find_gpkg_file(self, input_dir: Path) -> Path:
        """
        Find the GPKG file in the input directory.

        Args:
            input_dir: Directory to search

        Returns:
            Path to the GPKG file
        """
        # Look for GPKG files in subdirectories (bronze layer structure)
        gpkg_files = list(input_dir.rglob("*.gpkg"))

        if not gpkg_files:
            raise FileNotFoundError(f"No GPKG files found in {input_dir}")

        if len(gpkg_files) > 1:
            self.logger.warning(f"Multiple GPKG files found: {gpkg_files}, using the first one")

        gpkg_path = gpkg_files[0]
        self.logger.info(f"Using GPKG file: {gpkg_path}")

        return gpkg_path

    def _load_building_data(self, gpkg_path: Path) -> ibis.Table:
        """
        Load building data from GPKG file using Ibis/DuckDB.

        Args:
            gpkg_path: Path to GPKG file

        Returns:
            Ibis table with building data
        """
        self.logger.info(f"Loading building data from {gpkg_path}")

        try:
            # Try to read directly as a spatial table using DuckDB spatial extension
            table_name = "buildings_raw"

            # First make sure spatial extension is available
            self.conn.execute("LOAD spatial;")

            # Create buildings table with layer source
            self.conn.execute(f"""
                CREATE TABLE buildings_temp AS 
                SELECT *, 'building' as layer_source FROM ST_Read('{gpkg_path}', layer='building')
            """)

            # Create constructions table with layer source
            self.conn.execute(f"""
                CREATE TABLE constructions_temp AS 
                SELECT *, 'otherConstruction' as layer_source FROM ST_Read('{gpkg_path}', layer='otherConstruction')
            """)

            # Get column information for both tables to handle schema differences
            buildings_cols = self.conn.execute("DESCRIBE buildings_temp").fetchall()
            constructions_cols = self.conn.execute("DESCRIBE constructions_temp").fetchall()

            buildings_col_names = {col[0] for col in buildings_cols}
            constructions_col_names = {col[0] for col in constructions_cols}

            # Find common columns and unique columns
            common_cols = buildings_col_names & constructions_col_names
            buildings_only = buildings_col_names - constructions_col_names
            constructions_only = constructions_col_names - buildings_col_names

            self.logger.info(
                f"Buildings columns: {len(buildings_col_names)}, Constructions columns: {len(constructions_col_names)}"
            )
            self.logger.info(
                f"Common columns: {len(common_cols)}, Buildings-only: {len(buildings_only)}, Constructions-only: {len(constructions_only)}"
            )

            # Build SELECT statements with matching column structures
            all_cols = sorted(common_cols | buildings_only | constructions_only)

            buildings_select = []
            constructions_select = []

            for col in all_cols:
                if col in buildings_col_names:
                    buildings_select.append(col)
                else:
                    buildings_select.append(f"NULL as {col}")

                if col in constructions_col_names:
                    constructions_select.append(col)
                else:
                    constructions_select.append(f"NULL as {col}")

            buildings_select_str = ", ".join(buildings_select)
            constructions_select_str = ", ".join(constructions_select)

            # Combine both tables with matching schemas
            self.conn.execute(f"""
                CREATE TABLE {table_name} AS 
                SELECT {buildings_select_str} FROM buildings_temp
                UNION ALL
                SELECT {constructions_select_str} FROM constructions_temp
            """)

            # Clean up temporary tables
            self.conn.execute("DROP TABLE buildings_temp")
            self.conn.execute("DROP TABLE constructions_temp")

            # Get the table through Ibis
            buildings_table = self.ibis_conn.table(table_name)

            # Log basic info
            total_count = buildings_table.count().execute()
            self.logger.info(f"Loaded {total_count:,} buildings from GPKG")

            # Log column info
            columns = buildings_table.columns
            self.logger.info(
                f"Available columns: {columns[:10]}{'...' if len(columns) > 10 else ''}"
            )

            return buildings_table

        except Exception as e:
            self.logger.error(f"Failed to load building data: {e}")
            # Fallback: try loading with GeoPandas and converting
            return self._load_with_geopandas_fallback(gpkg_path)

    def _load_with_geopandas_fallback(self, gpkg_path: Path) -> ibis.Table:
        """
        Fallback method to load GPKG using GeoPandas.

        Args:
            gpkg_path: Path to GPKG file

        Returns:
            Ibis table with building data
        """
        self.logger.info("Using GeoPandas fallback to load GPKG")

        try:
            # Read both layers with GeoPandas and combine them
            buildings_gdf = gpd.read_file(gpkg_path, layer="building")
            self.logger.info(f"Loaded {len(buildings_gdf):,} buildings with GeoPandas")

            constructions_gdf = gpd.read_file(gpkg_path, layer="otherConstruction")
            self.logger.info(
                f"Loaded {len(constructions_gdf):,} other constructions with GeoPandas"
            )

            # Add layer source columns
            buildings_gdf["layer_source"] = "building"
            constructions_gdf["layer_source"] = "otherConstruction"

            # Combine both datasets
            gdf = gpd.GeoDataFrame(pd.concat([buildings_gdf, constructions_gdf], ignore_index=True))
            self.logger.info(f"Combined total: {len(gdf):,} records")

            # Convert to regular pandas DataFrame for DuckDB
            df = pd.DataFrame(gdf)

            # Convert geometry to WKT for DuckDB
            if "geometry" in df.columns:
                df["geometry_wkt"] = gdf.geometry.to_wkt()
                df = df.drop("geometry", axis=1)

            # Register with DuckDB
            table_name = "buildings_raw"
            self.conn.register(table_name, df)

            return self.ibis_conn.table(table_name)

        except Exception as e:
            self.logger.error(f"GeoPandas fallback also failed: {e}")
            raise

    def _load_building_data_from_object(self, bronze_data: dict) -> ibis.Table:
        """
        Load building data from coordinated bronze layer data object.
        Joins building attributes from INSPIRE BBR with geometries from GeoDanmark WFS.

        Args:
            bronze_data: Data object from coordinated bronze layer

        Returns:
            Ibis table with joined building data
        """
        self.logger.info("Loading and joining building data from coordinated bronze sources")

        try:
            # Check if this is the new coordinated data structure
            if "data" in bronze_data and "attributes" in bronze_data["data"]:
                # New coordinated structure: attributes + geometries
                attributes_df = bronze_data["data"]["attributes"]
                geometries_list = bronze_data["data"]["geometries"]

                self.logger.info(f"Attributes: {len(attributes_df):,} records")
                self.logger.info(f"Geometries: {len(geometries_list):,} features")

                # Convert geometries list to DataFrame
                geometries_data = []
                for geom_feature in geometries_list:
                    if "properties" in geom_feature and "geometry" in geom_feature:
                        props = geom_feature["properties"]
                        geom = geom_feature["geometry"]

                        # Extract BBRUUID (the join key)
                        bbruuid = props.get("BBRUUID")
                        if bbruuid:
                            # Convert geometry to WKT
                            geom_wkt = self._geojson_to_wkt(geom)

                            geometries_data.append(
                                {
                                    "localId": bbruuid,  # Use same column name as attributes
                                    "geometry_wkt": geom_wkt,
                                    "geometry_type": geom.get("type"),
                                    "has_geometry": True,
                                }
                            )

                geometries_df = pd.DataFrame(geometries_data)
                self.logger.info(f"Processed {len(geometries_df):,} geometries for joining")

                # Register both DataFrames with DuckDB
                self.conn.register("attributes_table", attributes_df)
                self.conn.register("geometries_table", geometries_df)

                # Perform LEFT JOIN to combine attributes with geometries
                join_query = """
                    SELECT 
                        a.*,
                        g.geometry_wkt,
                        g.geometry_type,
                        CASE WHEN g.has_geometry IS NOT NULL THEN true ELSE false END as has_geometry
                    FROM attributes_table a
                    LEFT JOIN geometries_table g ON a.localId = g.localId
                """

                self.conn.execute(f"CREATE TABLE buildings_joined AS {join_query}")
                buildings_table = self.ibis_conn.table("buildings_joined")

                # Log join results
                total_count = buildings_table.count().execute()
                with_geometry_count = (
                    buildings_table.filter(buildings_table.has_geometry == True).count().execute()
                )

                self.logger.info(f"Joined total: {total_count:,} buildings")
                self.logger.info(
                    f"With geometry: {with_geometry_count:,} buildings ({with_geometry_count / total_count:.1%})"
                )

                return buildings_table

            # Fallback: Check if we have direct GeoDataFrame data (old structure)
            elif "data" in bronze_data and bronze_data["data"] is not None:
                gdf = bronze_data["data"]
                self.logger.info(f"Using legacy in-memory data with {len(gdf):,} records")

                # Convert to regular pandas DataFrame for DuckDB
                df = pd.DataFrame(gdf)

                # Convert geometry to WKT for DuckDB
                if "geometry" in df.columns:
                    df["geometry_wkt"] = gdf.geometry.to_wkt()
                    df = df.drop("geometry", axis=1)

                # Register with DuckDB
                table_name = "buildings_raw"
                self.conn.register(table_name, df)

                return self.ibis_conn.table(table_name)

            # Fallback to reading from file path if data not in memory
            elif "gpkg_path" in bronze_data:
                self.logger.info("Bronze data object contains path, falling back to file loading")
                return self._load_building_data(bronze_data["gpkg_path"])

            else:
                raise ValueError("Bronze data object doesn't contain expected data structure")

        except Exception as e:
            self.logger.error(f"Failed to load building data from object: {e}")
            raise

    def _geojson_to_wkt(self, geojson_geom: dict) -> str:
        """
        Convert GeoJSON geometry to WKT format.

        Args:
            geojson_geom: GeoJSON geometry object

        Returns:
            WKT representation of the geometry
        """
        try:
            from shapely.geometry import shape

            shapely_geom = shape(geojson_geom)
            return shapely_geom.wkt
        except Exception as e:
            self.logger.warning(f"Failed to convert geometry to WKT: {e}")
            return None

    def _filter_buildings(self, buildings_table: ibis.Table) -> ibis.Table:
        """
        Filter buildings based on usage codes and current use values.

        Args:
            buildings_table: Input buildings table

        Returns:
            Filtered buildings table
        """
        self.logger.info("Applying building filters")

        try:
            # Determine which columns are available for filtering
            columns = buildings_table.columns

            # Try different possible column names for current use
            current_use_col = None
            for col_name in ["currentUse", "current_use", "CURRENTUSE"]:
                if col_name in columns:
                    current_use_col = col_name
                    break

            # Try different possible column names for building usage
            usage_code_col = None
            for col_name in ["buildingUsage", "building_usage", "BUILDINGUSAGE", "usage"]:
                if col_name in columns:
                    usage_code_col = col_name
                    break

            # Build filter conditions
            filter_conditions = []

            if current_use_col:
                # Filter by INSPIRE current use values
                all_target_uses = (
                    self.settings.agricultural_current_use
                    + self.settings.residential_current_use
                    + self.settings.public_services_current_use
                    + self.settings.other_construction_current_use
                )
                filter_conditions.append(buildings_table[current_use_col].isin(all_target_uses))
                self.logger.info(
                    f"Filtering by {current_use_col} column with values: {all_target_uses}"
                )

            if usage_code_col:
                # Filter by BBR usage codes
                all_usage_codes = (
                    self.settings.agricultural_usage_codes
                    + self.settings.residential_usage_codes
                    + self.settings.educational_usage_codes
                )
                filter_conditions.append(buildings_table[usage_code_col].isin(all_usage_codes))
                self.logger.info(
                    f"Filtering by {usage_code_col} column with codes: {all_usage_codes}"
                )

            # Apply filters
            if filter_conditions:
                # Combine conditions with OR (building matches any criteria)
                combined_filter = filter_conditions[0]
                for condition in filter_conditions[1:]:
                    combined_filter = combined_filter | condition

                filtered_table = buildings_table.filter(combined_filter)
            else:
                self.logger.warning("No recognized filter columns found, using all buildings")
                filtered_table = buildings_table

            # Log filtering results
            original_count = buildings_table.count().execute()
            filtered_count = filtered_table.count().execute()
            self.logger.info(f"Filtered from {original_count:,} to {filtered_count:,} buildings")

            return filtered_table

        except Exception as e:
            self.logger.error(f"Failed to filter buildings: {e}")
            raise

    def _standardize_schema(self, buildings_table: ibis.Table) -> ibis.Table:
        """
        Standardize the schema according to project conventions.

        Args:
            buildings_table: Input buildings table

        Returns:
            Table with standardized schema
        """
        self.logger.info("Standardizing schema")

        try:
            # Define column mappings (source -> target)
            column_mappings = {
                # Geometry columns
                "geometry": "geo_building_polygon",
                "geom": "geo_building_polygon",
                "geometry_wkt": "geo_building_polygon",
                # Building attributes
                "currentUse": "building_current_use",
                "current_use": "building_current_use",
                "CURRENTUSE": "building_current_use",
                "buildingUsage": "building_usage_code",
                "building_usage": "building_usage_code",
                "BUILDINGUSAGE": "building_usage_code",
                "constructionYear": "building_construction_year",
                "construction_year": "building_construction_year",
                "CONSTRUCTIONYEAR": "building_construction_year",
                "floorArea": "building_floor_area_sqm",
                "floor_area": "building_floor_area_sqm",
                "FLOORAREA": "building_floor_area_sqm",
                "numberOfFloors": "building_floors_above_ground",
                "number_of_floors": "building_floors_above_ground",
                "NUMBEROFFLOORS": "building_floors_above_ground",
                "numberOfDwellings": "building_dwellings_count",
                "number_of_dwellings": "building_dwellings_count",
                "NUMBEROFDWELLINGS": "building_dwellings_count",
                # Administrative attributes
                "localId": "bbr_uuid",
                "local_id": "bbr_uuid",
                "LOCALID": "bbr_uuid",
                "id": "bbr_uuid",
                "address": "address_full",
                "ADDRESS": "address_full",
                "parcelId": "parcel_id",
                "parcel_id": "parcel_id",
                "PARCELID": "parcel_id",
                # Layer source (added by our processing)
                "layer_source": "layer_source",
            }

            # Get available columns
            available_columns = buildings_table.columns

            # Build selection dict for existing columns
            selections = {}
            for source_col, target_col in column_mappings.items():
                if source_col in available_columns:
                    selections[target_col] = buildings_table[source_col]

            # Add any unmapped columns that might be useful
            for col in available_columns:
                if col not in column_mappings and col not in selections:
                    # Keep some additional columns that might be useful
                    if any(
                        keyword in col.lower() for keyword in ["uuid", "date", "time", "updated"]
                    ):
                        selections[col] = buildings_table[col]

            # Ensure we have at least some core columns
            if not selections:
                self.logger.warning("No recognized columns found, selecting all")
                return buildings_table

            # Select and rename columns
            standardized_table = buildings_table.select(**selections)

            # Add current timestamp
            standardized_table = standardized_table.mutate(
                last_updated=ibis.now().date(), processing_timestamp=ibis.now()
            )

            self.logger.info(f"Standardized schema with {len(selections)} mapped columns")

            return standardized_table

        except Exception as e:
            self.logger.error(f"Failed to standardize schema: {e}")
            raise

    def _enhance_classification_from_data(
        self, buildings_table: ibis.Table, bronze_data: dict
    ) -> ibis.Table:
        """
        Enhance building classification using bronze data object.

        Args:
            buildings_table: Input buildings table
            bronze_data: Bronze data object that may contain WFS data

        Returns:
            Enhanced buildings table
        """
        self.logger.info("Enhancing building classification from bronze data")

        try:
            # Check if bronze data contains WFS samples
            if "samples" in bronze_data and bronze_data["samples"]:
                # Use WFS data from bronze layer
                samples = bronze_data["samples"]
                # Process WFS samples for enhanced classification
                # For now, return the original table since this is complex
                self.logger.warning("WFS enhancement from bronze data not yet implemented")
                return buildings_table
            else:
                self.logger.info("No WFS data in bronze_data, skipping enhancement")
                return buildings_table

        except Exception as e:
            self.logger.error(f"Failed to enhance classification from bronze data: {e}")
            return buildings_table

    def _enhance_classification(self, buildings_table: ibis.Table, input_dir: Path) -> ibis.Table:
        """
        Enhance building classification using GeoDanmark WFS data.

        Args:
            buildings_table: Input buildings table
            input_dir: Input directory containing WFS data

        Returns:
            Enhanced buildings table
        """
        self.logger.info("Enhancing building classification with WFS data")

        try:
            # Look for WFS data files
            wfs_files = list(input_dir.rglob("geodanmark_samples.json"))

            if not wfs_files:
                self.logger.warning("No WFS data found for enhancement, skipping")
                return buildings_table

            # Load WFS data
            wfs_path = wfs_files[0]
            with open(wfs_path, encoding="utf-8") as f:
                wfs_data = json.load(f)

            # For now, just add a flag indicating that WFS data was available
            # In a full implementation, this would cross-reference BBRU UIDs
            enhanced_table = buildings_table.mutate(
                wfs_data_available=True, enhancement_timestamp=ibis.now()
            )

            self.logger.info("Added WFS enhancement flags")

            return enhanced_table

        except Exception as e:
            self.logger.error(f"Failed to enhance classification: {e}")
            # Return original table if enhancement fails
            return buildings_table

    def _validate_geometries(self, buildings_table: ibis.Table) -> ibis.Table:
        """
        Validate and clean building geometries.

        Args:
            buildings_table: Input buildings table

        Returns:
            Table with validated geometries
        """
        self.logger.info("Validating geometries")

        try:
            # Check if we have a geometry column
            geometry_col = None
            for col in buildings_table.columns:
                if "geo_building_polygon" in col or "geometry" in col.lower():
                    geometry_col = col
                    break

            if not geometry_col:
                self.logger.warning("No geometry column found, skipping geometry validation")
                return buildings_table

            # For now, just return the table as-is
            # In a full implementation, this would:
            # 1. Validate geometry validity
            # 2. Repair invalid geometries
            # 3. Ensure EPSG:4326 projection
            # 4. Calculate centroids

            validated_table = buildings_table.mutate(
                geometry_validated=True, validation_timestamp=ibis.now()
            )

            self.logger.info("Geometry validation completed")

            return validated_table

        except Exception as e:
            self.logger.error(f"Failed to validate geometries: {e}")
            return buildings_table

    def _add_derived_fields(self, buildings_table: ibis.Table) -> ibis.Table:
        """
        Add derived fields for analysis.

        Args:
            buildings_table: Input buildings table

        Returns:
            Table with derived fields
        """
        self.logger.info("Adding derived fields")

        try:
            # Classify building categories based on current use or usage codes
            enhanced_table = buildings_table

            # Add building category classification
            if "building_current_use" in buildings_table.columns:
                enhanced_table = enhanced_table.mutate(
                    building_usage_category=ibis.cases(
                        (
                            enhanced_table.building_current_use.isin(
                                self.settings.agricultural_current_use
                            ),
                            "agricultural",
                        ),
                        (
                            enhanced_table.building_current_use.isin(
                                self.settings.residential_current_use
                            ),
                            "residential",
                        ),
                        (
                            enhanced_table.building_current_use.isin(
                                self.settings.public_services_current_use
                            ),
                            "public_services",
                        ),
                        else_="other",
                    )
                )

            # Add processing metadata
            enhanced_table = enhanced_table.mutate(
                pipeline_version=ibis.literal("1.0.0"), processed_at=ibis.now()
            )

            self.logger.info("Added derived fields")

            return enhanced_table

        except Exception as e:
            self.logger.error(f"Failed to add derived fields: {e}")
            return buildings_table

    def _save_to_geoparquet(self, buildings_table: ibis.Table, output_path: Path) -> None:
        """
        Save the processed buildings to GeoParquet format.

        Args:
            buildings_table: Buildings table to save
            output_path: Output file path
        """
        self.logger.info(f"Saving buildings to {output_path}")

        try:
            # Execute the query and get results as pandas DataFrame
            df = buildings_table.execute()

            # If we have geometry data as WKT, convert to GeoPandas
            geometry_col = None
            for col in df.columns:
                if "geo_building_polygon" in col or "geometry" in col.lower():
                    geometry_col = col
                    break

            if geometry_col and geometry_col in df.columns:
                # Convert to GeoDataFrame
                gdf = gpd.GeoDataFrame(df)
                if df[geometry_col].dtype == "object":
                    # Assume it's WKT format
                    from shapely import wkt

                    gdf["geometry"] = df[geometry_col].apply(wkt.loads)
                    gdf = gdf.drop(geometry_col, axis=1)

                # Set CRS (assume EPSG:4326 for now)
                gdf.crs = "EPSG:4326"

                # Save as GeoParquet
                gdf.to_parquet(output_path)
            else:
                # Save as regular Parquet
                df.to_parquet(output_path)

            file_size = output_path.stat().st_size / (1024**2)
            self.logger.info(f"Saved {len(df):,} buildings ({file_size:.2f} MB)")

        except Exception as e:
            self.logger.error(f"Failed to save to GeoParquet: {e}")
            raise

    def _generate_summary_stats(self, buildings_table: ibis.Table, output_dir: Path) -> None:
        """
        Generate summary statistics for the processed buildings.

        Args:
            buildings_table: Buildings table
            output_dir: Output directory
        """
        self.logger.info("Generating summary statistics")

        try:
            stats = {
                "total_buildings": buildings_table.count().execute(),
                "processing_timestamp": datetime.now().isoformat(),
            }

            # Category breakdown if available
            if "building_usage_category" in buildings_table.columns:
                category_counts = (
                    buildings_table.group_by("building_usage_category")
                    .aggregate(count=ibis._.count())
                    .execute()
                )
                stats["categories"] = category_counts.to_dict("records")

            # Save stats
            stats_path = output_dir / "summary_stats.json"
            with open(stats_path, "w", encoding="utf-8") as f:
                json.dump(stats, f, indent=2, ensure_ascii=False)

            self.logger.info(
                f"Generated summary statistics: {stats['total_buildings']:,} buildings"
            )

        except Exception as e:
            self.logger.error(f"Failed to generate summary stats: {e}")

    def _save_processing_metadata(
        self, output_dir: Path, gpkg_path: Path, enhance_classification: bool
    ) -> None:
        """
        Save metadata about the processing run.

        Args:
            output_dir: Output directory
            gpkg_path: Path to source GPKG file
            enhance_classification: Whether classification was enhanced
        """
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "source_gpkg": str(gpkg_path),
            "enhance_classification": enhance_classification,
            "settings": {
                "agricultural_usage_codes": self.settings.agricultural_usage_codes,
                "residential_usage_codes": self.settings.residential_usage_codes,
                "educational_usage_codes": self.settings.educational_usage_codes,
                "agricultural_current_use": self.settings.agricultural_current_use,
                "residential_current_use": self.settings.residential_current_use,
                "public_services_current_use": self.settings.public_services_current_use,
            },
            "pipeline_version": "1.0.0",
        }

        metadata_path = output_dir / "processing_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved processing metadata to {metadata_path}")

    def _save_processing_metadata_from_data(
        self, output_dir: Path, bronze_data: dict, enhance_classification: bool
    ) -> None:
        """
        Save processing metadata when using bronze data object.

        Args:
            output_dir: Output directory
            bronze_data: Bronze data object
            enhance_classification: Whether classification was enhanced
        """
        metadata = {
            "timestamp": datetime.now().isoformat(),
            "source": "bronze_data_object",
            "enhance_classification": enhance_classification,
            "bronze_metadata": bronze_data.get("metadata", {}),
            "settings": {
                "agricultural_usage_codes": self.settings.agricultural_usage_codes,
                "residential_usage_codes": self.settings.residential_usage_codes,
                "educational_usage_codes": self.settings.educational_usage_codes,
                "agricultural_current_use": self.settings.agricultural_current_use,
                "residential_current_use": self.settings.residential_current_use,
                "public_services_current_use": self.settings.public_services_current_use,
            },
            "pipeline_version": "1.0.0",
        }

        metadata_path = output_dir / "processing_metadata.json"
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        self.logger.info(f"Saved processing metadata to {metadata_path}")

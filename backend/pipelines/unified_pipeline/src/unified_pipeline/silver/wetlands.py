"""
Silver layer processing for Wetlands data.

This module handles the transformation of raw wetlands data from the bronze layer.
It parses XML data, processes geometries, and creates both original and dissolved
versions of the wetlands dataset for analysis and visualization.

The module contains:
- WetlandsSilverConfig: Configuration class for the processing
- WetlandsSilver: Implementation class for transforming and analyzing data

The processing includes XML parsing, geometry validation, spatial operations for
merging adjacent polygons, and comprehensive logging of geometry statistics.
"""

import xml.etree.ElementTree as ET
from collections import Counter
from typing import Any, Optional

# ✅ MIGRATION: Removed geopandas import - using DuckDB-spatial for all operations
# # ✅ MIGRATION: Removed shapely import - using DuckDB-spatial for geometry operations
# from shapely import Polygon
from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed


class WetlandsSilverConfig(BaseJobConfig):
    """
    Configuration for the Wetlands Silver processing.

    This class defines all configuration parameters needed for transforming wetlands
    data from the bronze layer to the silver layer, including dataset names,
    storage settings, and XML namespaces.

    Attributes:
        dataset (str): Name of the dataset in storage
        bucket (str): GCS bucket name for data storage
        storage_batch_size (int): Batch size for storage operations
        namespaces (dict[str, str]): XML namespaces used in the wetlands data
        gml_ns (str): GML namespace prefix for XML parsing
    """

    dataset: str = "wetlands"
    bucket: str = "landbrugsdata-raw-data"
    storage_batch_size: int = 8000  # Increased for better performance with 16GB RAM
    namespaces: dict[str, str] = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "natur": "http://wfs2-miljoegis.mim.dk/natur",
        "gml": "http://www.opengis.net/gml/3.2",
    }
    gml_ns: str = "{http://www.opengis.net/gml/3.2}"  # This is not a f-string.


class WetlandsSilver(BaseSource[WetlandsSilverConfig], SilverJobInterface):
    """
    Silver layer processor for wetlands data.

    This class handles the processing of wetlands data from the bronze layer
    to the silver layer. It reads, transforms, and saves the data according to
    the data pipeline architecture. The class handles XML processing, geometry
    operations, and data storage in GCS.

    Key functionalities include:
    1. Parsing XML data to extract features and geometries
    2. Creating standard and dissolved (merged) versions of the dataset
    3. Analyzing and logging geometry statistics for data quality assessment
    """

    def __init__(self, config: WetlandsSilverConfig):
        """
        Initialize the WetlandsSilver processor.

        Args:
            config (WetlandsSilverConfig): Configuration object containing settings
                                            for the processor."""
        super().__init__(config)

        # ✅ MIGRATION: BaseSource already created GCSDataAccess and configured DuckDB
        # No need to create another instance or setup DuckDB again
        self.log.info("✅ WetlandsSilver: Using unified GCS access and DuckDB connection")
        self.log.info("✅ Optimized for DuckDB-spatial v1.2.2+ with SPATIAL_JOIN operator")

    def analyze_geometry(self, geometry_wkt: str) -> dict[str, Any]:
        """
        Analyze a geometry and extract key metrics using DuckDB-spatial.

        This method calculates width, height, area, grid alignment, and vertex count
        for a geometry. It is used to generate statistics about the wetlands dataset.

        Args:
            geometry_wkt: A WKT geometry string

        Returns:
            dict: Dictionary containing geometry metrics:
                - width: Width of the geometry's bounding box
                - height: Height of the geometry's bounding box
                - area: Area of the bounding box
                - grid_aligned: Whether the geometry is aligned to a 10-unit grid
                - vertices: Number of vertices in the geometry
        """
        # Use DuckDB-spatial for geometry analysis
        conn = self.conn

        result = conn.execute(f"""
            WITH geom_analysis AS (
                SELECT 
                    ST_XMax(geom) - ST_XMin(geom) as width,
                    ST_YMax(geom) - ST_YMin(geom) as height,
                    (ST_XMax(geom) - ST_XMin(geom)) * (ST_YMax(geom) - ST_YMin(geom)) as area,
                    ST_NPoints(geom) as vertices
                FROM (SELECT ST_GeomFromText('{geometry_wkt}') as geom)
            )
            SELECT width, height, area, vertices FROM geom_analysis
        """).fetchone()

        width, height, area, vertices = result

        # For grid alignment, we'll use a simplified heuristic
        # Check if bounding box dimensions are close to multiples of 10
        grid_aligned_width = abs(round(width / 10) * 10 - width) < 0.01
        grid_aligned_height = abs(round(height / 10) * 10 - height) < 0.01
        is_grid_aligned = grid_aligned_width and grid_aligned_height

        return {
            "width": width,
            "height": height,
            "area": area,
            "grid_aligned": is_grid_aligned,
            "vertices": vertices,
        }

    def log_geometry_statistics(self, table_name: str) -> None:
        """
        Log detailed geometry statistics for a DuckDB table.

        This method analyzes geometry properties including dimensions, area,
        vertex count, and grid alignment for data quality assessment.

        Args:
            table_name (str): Name of the DuckDB table containing geometries to analyze

        Returns:
            None: Results are logged directly
        """
        conn = self.conn

        # Check which geometry column exists in the table
        columns = conn.execute(f"DESCRIBE {table_name}").fetchall()
        column_names = [col[0] for col in columns]

        if "geometry_original" in column_names:
            geometry_col = "geometry_original"
        elif "geometry" in column_names:
            geometry_col = "geometry"
        else:
            self.log.error(f"No geometry column found in table {table_name}")
            return

        # Get geometry statistics using DuckDB-spatial
        stats_data = conn.execute(f"""
            SELECT 
                (ST_XMax({geometry_col}) - ST_XMin({geometry_col})) * 
                (ST_YMax({geometry_col}) - ST_YMin({geometry_col})) as area,
                ST_NPoints({geometry_col}) as vertices,
                ST_XMax({geometry_col}) - ST_XMin({geometry_col}) as width,
                ST_YMax({geometry_col}) - ST_YMin({geometry_col}) as height,
                CASE 
                    WHEN ABS(ROUND((ST_XMax({geometry_col}) - ST_XMin({geometry_col})) / 10) * 10 - 
                             (ST_XMax({geometry_col}) - ST_XMin({geometry_col}))) < 0.01
                         AND ABS(ROUND((ST_YMax({geometry_col}) - ST_YMin({geometry_col})) / 10) * 10 - 
                                 (ST_YMax({geometry_col}) - ST_YMin({geometry_col}))) < 0.01
                    THEN true 
                    ELSE false 
                END as grid_aligned
            FROM {table_name}
            WHERE {geometry_col} IS NOT NULL
        """).fetchall()

        # Unique dimensions analysis
        dimensions = Counter([(row[2], row[3]) for row in stats_data])  # width, height

        self.log.info("Geometry Statistics:")
        self.log.info(f"Total features: {len(stats_data)}")
        self.log.info(f"Unique dimension combinations: {len(dimensions)}")
        self.log.info("\nTop 10 most common dimensions (width x height, count):")
        for (width, height), count in dimensions.most_common(10):
            self.log.info(f"{width:.1f}m x {height:.1f}m: {count} features")

        if len(dimensions) > 10:
            remaining_count = len(dimensions) - 10
            remaining_features = sum(
                count for (width, height), count in dimensions.most_common()[10:]
            )
            self.log.info(
                f"... and {remaining_count} other dimension combinations ({remaining_features} features)"
            )

        non_grid_aligned = sum(1 for row in stats_data if not row[4])  # grid_aligned is index 4
        avg_vertices = (
            sum(row[1] for row in stats_data) / len(stats_data) if stats_data else 0
        )  # vertices is index 1
        total_area = sum(row[0] for row in stats_data) if stats_data else 0  # area is index 0

        self.log.info(f"\nNon-grid-aligned features: {non_grid_aligned}")
        self.log.info(f"Average vertices per feature: {avg_vertices:.1f}")
        self.log.info(f"Total area covered: {total_area / 1_000_000:.2f} km²")

    def _parse_geometry(self, geom_elem: ET.Element) -> Optional[str]:
        """
        Parse a GML geometry element into a WKT string using DuckDB-spatial.

        This method extracts coordinates from a GML posList element and
        creates a WKT polygon string. It also ensures the polygon is valid.

        Args:
            geom_elem (ET.Element): The XML element containing the geometry

        Returns:
            Optional[str]: A WKT polygon string if successful, None otherwise

        Raises:
            None: Exceptions are caught and logged
        """
        try:
            pos_list = geom_elem.find(".//gml:posList", self.config.namespaces)
            if pos_list is None or pos_list.text is None:
                self.log.debug(
                    "Missing posList in geometry element"
                )  # Changed to debug to reduce noise
                return None
            coords_str = pos_list.text.split()
            coords = [
                (float(coords_str[i]), float(coords_str[i + 1]))
                for i in range(0, len(coords_str), 2)
            ]

            # Create WKT polygon string
            coord_pairs = [f"{x} {y}" for x, y in coords]
            wkt_coords = ", ".join(coord_pairs)
            wkt_polygon = f"POLYGON(({wkt_coords}))"

            # Validate and fix geometry using DuckDB-spatial
            conn = self.conn
            result = conn.execute(f"""
                SELECT 
                    CASE 
                        WHEN ST_IsValid(ST_GeomFromText('{wkt_polygon}')) 
                        THEN '{wkt_polygon}'
                        ELSE ST_AsText(ST_Buffer(ST_GeomFromText('{wkt_polygon}'), 0))
                    END as geometry_wkt
            """).fetchone()

            return result[0] if result else None

        except Exception as e:
            self.log.error(f"Error parsing geometry: {str(e)}")
            return None

    def _get_attribute(self, element: ET.Element, tag: str) -> Optional[str]:
        """
        Get an attribute value from an XML element.

        This method finds a child element with the specified tag and
        returns its text value if found.

        Args:
            element (ET.Element): The parent XML element
            tag (str): The tag name of the child element to find

        Returns:
            Optional[str]: The text value of the child element if found, None otherwise
        """
        attr = element.find(tag, self.config.namespaces)
        return attr.text if attr is not None else None

    def _parse_feature(self, feature: ET.Element) -> Optional[dict[str, Any]]:
        """
        Parse an XML feature element into a feature dictionary with WKT geometry.

        This method extracts geometry and attributes from an XML feature element,
        and constructs a dictionary with properties and WKT geometry.

        Args:
            feature (ET.Element): The XML element containing the feature

        Returns:
            Optional[dict[str, Any]]: A feature dictionary if successful,
                                     None if required attributes are missing

        Raises:
            None: Exceptions are caught and logged
        """
        try:
            polygon = feature.find(".//gml:Polygon", self.config.namespaces)
            if polygon is None:
                self.log.debug("Missing Polygon in feature")  # Changed to debug to reduce noise
                return None
            geom_wkt = self._parse_geometry(polygon)
            if not geom_wkt:
                return None

            gridcode_str = self._get_attribute(feature, "natur:gridcode")
            if gridcode_str is None:
                self.log.debug("Missing gridcode in feature")  # Changed to debug to reduce noise
                return None
            gridcode = int(gridcode_str)

            toerv_pct = self._get_attribute(feature, "natur:toerv_pct")
            if toerv_pct is None:
                self.log.debug("Missing toerv_pct in feature")  # Changed to debug to reduce noise
                return None

            return {
                "id": feature.get(f"{self.config.gml_ns}id"),
                "gridcode": gridcode,
                "toerv_pct": toerv_pct,
                "geometry_wkt": geom_wkt,
            }
        except Exception as e:
            self.log.error(f"Error parsing feature: {str(e)}")
            return None

    @timed(name="Processing XML data")  # type: ignore
    def _process_xml_data(self, raw_data_input) -> Optional[str]:
        """
        Process raw XML data into a DuckDB table.

        This method parses XML data containing wetland features and converts it to
        a DuckDB table with attributes and geometries. It can handle both
        and table name inputs.

        Args:
            raw_data_input:  or table name containing raw XML data

        Returns:
            Optional[str]: Name of the DuckDB table containing wetland features,
                          or None if no data is available

        Raises:
            Exception: If there are errors during processing that cannot be handled
        """
        # ✅ MIGRATION: Handle only table names now
        if isinstance(raw_data_input, str):
            # Input is a table name
            table_name = raw_data_input
            # Get XML data from table
            xml_data_rows = self.conn.execute(f"SELECT payload FROM {table_name}").fetchall()
            if not xml_data_rows:
                self.log.warning("No raw data to process")
                return None
            self.log.info("Processing XML data from DuckDB table")
        else:
            self.log.error(f"Expected table name (string), got {type(raw_data_input)}")
            return None

        features = []
        for i, row in enumerate(xml_data_rows):
            try:
                # Parse the XML data
                xml_data = row[0]  # row is a tuple from fetchall()
                root = ET.fromstring(xml_data)

                for member in root.findall(
                    ".//natur:kulstof2022", namespaces=self.config.namespaces
                ):
                    parsed = self._parse_feature(member)
                    if parsed and parsed.get("geometry_wkt"):
                        features.append(parsed)

                    if len(features) % 100000 == 0:
                        self.log.info(f"Processed {len(features):,} features")

            except Exception as e:
                self.log.error(f"Error processing row {i}: {str(e)}", exc_info=True)
                raise e

        self.log.info(f"Parsed {len(features):,} features from XML data")

        # ✅ MIGRATION: Create DuckDB table directly with spatial data
        # Use DuckDB's native support for Python objects without pandas
        conn = self.conn

        if not features:
            self.log.warning("No features to process")
            return None

        # Create table directly from the list of dictionaries using DuckDB's native capabilities
        # First, get the column names from the first feature
        columns = list(features[0].keys())

        # Create the table schema
        conn.execute(f"""
            CREATE TABLE temp_features (
                {", ".join([f"{col} VARCHAR" for col in columns])}
            )
        """)

        # Insert data in batches to avoid SQL statement size limits
        batch_size = 1000
        for i in range(0, len(features), batch_size):
            batch = features[i : i + batch_size]

            # Create VALUES clause for this batch
            # Use parameterized queries instead of string concatenation
            for feature in batch:
                values = [feature.get(col) for col in columns]
                placeholders = ", ".join(["?" for _ in columns])

                conn.execute(
                    f"""
                    INSERT INTO temp_features ({", ".join(columns)})
                    VALUES ({placeholders})
                """,
                    values,
                )

            if (i + batch_size) % 10000 == 0:
                self.log.info(f"Inserted {i + batch_size:,} features into temp table")

        # Create spatial table with proper geometry column
        # ✅ COORDINATE FIX: Keep original geometry for adjacency detection
        table_name = "wetlands_processed"
        conn.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT 
                id,
                gridcode,
                toerv_pct,
                ST_GeomFromText(geometry_wkt) as geometry,
                geometry_wkt as geometry_wkt_original
            FROM temp_features
            WHERE geometry_wkt IS NOT NULL
        """)

        self.log.info(f"Created DuckDB table '{table_name}' with {len(features):,} features")

        # ✅ CRITICAL: Perform adjacency detection BEFORE coordinate transformation
        # Grid cells are perfectly adjacent in original coordinate system (EPSG:25832)
        # but may have tiny gaps after transformation to EPSG:4326
        dissolved_table_name = self._create_dissolved_df_before_transform(
            table_name, self.config.dataset
        )

        # ✅ COORDINATE FIX: Apply geometry validation and transformation AFTER adjacency detection
        from unified_pipeline.common.geometry_validator import (
            validate_and_transform_geometries_duckdb,
        )

        # Transform the original non-dissolved table
        validate_and_transform_geometries_duckdb(
            conn, table_name, f"silver.{self.config.dataset}", geometry_column="geometry"
        )

        # Transform the dissolved table
        validate_and_transform_geometries_duckdb(
            conn,
            dissolved_table_name,
            f"silver.{self.config.dataset}_dissolved",
            geometry_column="geometry",
        )

        return table_name

    @timed(name="Creating dissolved DuckDB table in original coordinates")  # type: ignore
    def _create_dissolved_df_before_transform(self, input_table_name: str, dataset: str) -> str:
        """
        Create a dissolved (merged) version of the wetlands dataset using DuckDB-spatial.

        CRITICAL: This method works on the original coordinate system (EPSG:25832)
        BEFORE transformation to EPSG:4326. This preserves perfect grid adjacency
        relationships that would be lost after coordinate transformation.

        Args:
            input_table_name (str): Name of the input DuckDB table with wetland features
            dataset (str): Name of the dataset, used for logging and transformation

        Returns:
            str: Name of the DuckDB table with merged wetland polygons and wetland IDs

        Raises:
            Exception: If there are errors during the dissolve operation
        """
        try:
            # Clean up any existing tables from previous runs
            conn = self.conn
            tables_to_cleanup = [
                "wetlands_spatial",
                "wetland_adjacency_raw",
                "wetland_adjacency_dedup",
                "wetland_adjacency",
                "wetland_groups",
                "wetland_groups_final",
                "wetlands_dissolved_temp",
            ]
            for table in tables_to_cleanup:
                conn.execute(f"DROP TABLE IF EXISTS {table}")

            # Log statistics of input table
            self.log_geometry_statistics(input_table_name)

            feature_count = conn.execute(f"SELECT COUNT(*) FROM {input_table_name}").fetchone()[0]
            self.log.info(
                f"Starting DuckDB-spatial merge of {feature_count:,} features in original coordinates..."
            )

            # Create spatial table with wetland IDs using ORIGINAL geometry (before transformation)
            # Handle overlapping peat percentages by prioritizing higher percentages
            self.log.info(
                "Handling overlapping peat percentage areas - prioritizing higher percentages..."
            )

            # First, create a staging table with all wetlands
            conn.execute(f"""
                CREATE TABLE wetlands_staging AS
                SELECT 
                    ROW_NUMBER() OVER () as temp_id,
                    id,
                    gridcode,
                    toerv_pct,
                    geometry
                FROM {input_table_name}
            """)

            # Create index to improve spatial query performance
            conn.execute(
                "CREATE INDEX idx_wetlands_staging_geometry ON wetlands_staging USING RTREE (geometry)"
            )

            # Handle peat percentage overlaps using SPATIAL_JOIN operator (PR #545)
            # Strategy: Exclude overlapping 6-12% areas entirely, keep >12% priority

            # Step 1: Keep all >12% areas unchanged
            conn.execute("""
                CREATE TABLE wetlands_high_peat AS
                SELECT temp_id, id, gridcode, toerv_pct, geometry
                FROM wetlands_staging
                WHERE toerv_pct = '>12'
            """)

            # Step 2: Keep only 6-12% areas that DON'T overlap with >12% areas
            # Uses ST_Intersects in JOIN to trigger optimized SPATIAL_JOIN operator
            conn.execute("""
                CREATE TABLE wetlands_medium_peat AS
                SELECT ws.temp_id, ws.id, ws.gridcode, ws.toerv_pct, ws.geometry
                FROM wetlands_staging ws
                WHERE ws.toerv_pct = '6-12'
                  AND NOT EXISTS (
                      SELECT 1 FROM wetlands_high_peat hp 
                      WHERE ST_Intersects(ws.geometry, hp.geometry)
                  )
            """)

            # Step 3: Keep all other peat percentage areas unchanged
            conn.execute("""
                CREATE TABLE wetlands_other_peat AS
                SELECT temp_id, id, gridcode, toerv_pct, geometry
                FROM wetlands_staging
                WHERE toerv_pct NOT IN ('>12', '6-12')
            """)

            # Step 4: Combine all non-overlapping areas (no complex geometry filtering needed)
            conn.execute("""
                CREATE TABLE wetlands_spatial AS
                SELECT 
                    ROW_NUMBER() OVER () as wetland_id,
                    id,
                    gridcode,
                    toerv_pct,
                    geometry
                FROM (
                    SELECT id, gridcode, toerv_pct, geometry FROM wetlands_high_peat
                    UNION ALL
                    SELECT id, gridcode, toerv_pct, geometry FROM wetlands_medium_peat
                    UNION ALL
                    SELECT id, gridcode, toerv_pct, geometry FROM wetlands_other_peat
                ) combined_wetlands
            """)

            # Clean up temporary tables
            conn.execute("DROP TABLE wetlands_staging")
            conn.execute("DROP TABLE wetlands_high_peat")
            conn.execute("DROP TABLE wetlands_medium_peat")
            conn.execute("DROP TABLE wetlands_other_peat")

            # Log the overlap resolution results
            original_count = conn.execute(f"SELECT COUNT(*) FROM {input_table_name}").fetchone()[0]
            processed_count = conn.execute("SELECT COUNT(*) FROM wetlands_spatial").fetchone()[0]
            high_peat_count = conn.execute(
                "SELECT COUNT(*) FROM wetlands_spatial WHERE toerv_pct = '>12'"
            ).fetchone()[0]
            medium_peat_count = conn.execute(
                "SELECT COUNT(*) FROM wetlands_spatial WHERE toerv_pct = '6-12'"
            ).fetchone()[0]

            self.log.info(
                "✅ Peat overlap resolution completed using SPATIAL_JOIN optimization (PR #545):"
            )
            self.log.info(f"   Original features: {original_count:,}")
            self.log.info(f"   Processed features: {processed_count:,}")
            self.log.info(f"   >12% peat areas: {high_peat_count:,}")
            self.log.info(f"   6-12% peat areas (non-overlapping): {medium_peat_count:,}")

            # ✅ DuckDB-spatial optimization: No explicit indexing needed
            # DuckDB spatial extension automatically creates temporary spatial indexes
            # for SPATIAL_JOIN operations when query structure is optimized
            self.log.info("✅ DuckDB spatial table created - automatic indexing will be used")

            # ✅ OPTIMIZED: Absolute simplest SPATIAL_JOIN query for DuckDB-spatial v1.2.2+
            # Based on PR #545: SPATIAL_JOIN operator requires:
            # 1. Direct table joins (no CTEs)
            # 2. ONLY ONE spatial predicate in JOIN condition
            # 3. Simple SELECT without ANY calculations or complex WHERE conditions
            # 4. Deduplication handled separately AFTER spatial join
            adjacency_query = """
                SELECT 
                    w1.wetland_id as id1,
                    w2.wetland_id as id2
                FROM wetlands_spatial w1
                INNER JOIN wetlands_spatial w2 ON ST_Touches(w1.geometry, w2.geometry)
            """

            # Verify SPATIAL_JOIN is being used
            from unified_pipeline.common.geometry_validator import verify_spatial_join_usage

            spatial_join_detected = verify_spatial_join_usage(conn, adjacency_query)
            if spatial_join_detected:
                self.log.info("✅ Using optimized SPATIAL_JOIN operator for adjacency detection")
            else:
                self.log.warning(
                    "⚠️ SPATIAL_JOIN not detected - query may use fallback nested loop join"
                )

            # Create adjacency table using optimized SPATIAL_JOIN (single condition only)
            self.log.info("Finding adjacent wetlands using SPATIAL_JOIN...")
            conn.execute(f"""
                CREATE TABLE wetland_adjacency_raw AS
                {adjacency_query}
            """)

            raw_adjacency_count = conn.execute(
                "SELECT COUNT(*) FROM wetland_adjacency_raw"
            ).fetchone()[0]
            self.log.info(
                f"Found {raw_adjacency_count:,} touching wetland pairs (including self-joins)"
            )

            # Deduplicate and remove self-joins AFTER spatial join to maintain SPATIAL_JOIN compliance
            self.log.info("Removing self-joins and deduplicating pairs...")
            conn.execute("""
                CREATE TABLE wetland_adjacency_dedup AS
                SELECT DISTINCT
                    LEAST(id1, id2) as id1,
                    GREATEST(id1, id2) as id2
                FROM wetland_adjacency_raw
                WHERE id1 != id2
            """)

            conn.execute("DROP TABLE wetland_adjacency_raw")
            conn.execute("ALTER TABLE wetland_adjacency_dedup RENAME TO wetland_adjacency_raw")

            dedup_adjacency_count = conn.execute(
                "SELECT COUNT(*) FROM wetland_adjacency_raw"
            ).fetchone()[0]
            self.log.info(f"After deduplication: {dedup_adjacency_count:,} unique adjacent pairs")

            # Apply edge length filter AFTER spatial join to maintain SPATIAL_JOIN operator usage
            self.log.info("Filtering for meaningful shared edges (length >= 10m)...")
            conn.execute("""
                CREATE TABLE wetland_adjacency AS
                SELECT DISTINCT
                    LEAST(wa.id1, wa.id2) as id1,
                    GREATEST(wa.id1, wa.id2) as id2
                FROM wetland_adjacency_raw wa
                JOIN wetlands_spatial w1 ON wa.id1 = w1.wetland_id
                JOIN wetlands_spatial w2 ON wa.id2 = w2.wetland_id
                WHERE ST_Length(ST_Intersection(w1.geometry, w2.geometry)) >= 10
            """)

            conn.execute("DROP TABLE wetland_adjacency_raw")

            adjacency_count = conn.execute("SELECT COUNT(*) FROM wetland_adjacency").fetchone()[0]
            self.log.info(
                f"Filtered to {adjacency_count:,} adjacent wetland pairs with meaningful edges"
            )

            # Continue with connected components if we found adjacent wetlands
            if adjacency_count > 0:
                # Create connected components for merging
                self.log.info("Creating connected components for merging...")

                # Simplified connected components using DuckDB
                # Start with each wetland in its own group
                conn.execute("""
                    CREATE TABLE wetland_groups AS
                    SELECT wetland_id, wetland_id as group_id
                    FROM wetlands_spatial
                """)

                # Iteratively merge groups based on adjacency
                # This is more similar to the GeoPandas approach
                iteration = 0
                changes_made = True
                while changes_made and iteration < 100:  # Safety limit
                    iteration += 1

                    # Update group IDs based on adjacency
                    rows_updated = conn.execute("""
                        UPDATE wetland_groups 
                        SET group_id = (
                            SELECT MIN(LEAST(wg1.group_id, wg2.group_id))
                            FROM wetland_adjacency adj
                            JOIN wetland_groups wg1 ON adj.id1 = wg1.wetland_id
                            JOIN wetland_groups wg2 ON adj.id2 = wg2.wetland_id
                            WHERE wetland_groups.wetland_id IN (adj.id1, adj.id2)
                        )
                        WHERE wetland_id IN (
                            SELECT DISTINCT UNNEST([id1, id2]) 
                            FROM wetland_adjacency
                        )
                    """).rowcount

                    changes_made = rows_updated > 0
                    if changes_made:
                        self.log.info(
                            f"Iteration {iteration}: Updated {rows_updated} group assignments"
                        )

                # Normalize group IDs to be sequential
                conn.execute("""
                    CREATE TABLE wetland_groups_final AS
                    SELECT 
                        wetland_id,
                        DENSE_RANK() OVER (ORDER BY group_id) as final_group_id
                    FROM wetland_groups
                """)

                conn.execute("DROP TABLE wetland_groups")
                conn.execute("ALTER TABLE wetland_groups_final RENAME TO wetland_groups")

                # Check number of groups for progress tracking
                group_count = conn.execute("""
                    SELECT COUNT(DISTINCT final_group_id) FROM wetland_groups
                """).fetchone()[0]

                self.log.info(f"Found {group_count:,} connected components to dissolve")

                # Create dissolved geometries using ST_Union_Agg with progress tracking
                self.log.info("Creating dissolved geometries using ST_Union_Agg...")

                # Process in batches if we have many groups (>1000) to avoid memory issues
                if group_count > 1000:
                    self.log.info(
                        f"Processing {group_count:,} groups in batches for memory efficiency"
                    )

                    # Create empty result table
                    conn.execute("DROP TABLE IF EXISTS wetlands_dissolved_temp")
                    conn.execute("""
                        CREATE TABLE wetlands_dissolved_temp (
                            wetland_id INTEGER,
                            final_group_id INTEGER,
                            geometry GEOMETRY,
                            dominant_toerv_pct VARCHAR
                        )
                    """)

                    # Process in batches of 100 groups
                    batch_size = 100
                    for offset in range(0, group_count, batch_size):
                        batch_end = min(offset + batch_size, group_count)
                        self.log.info(
                            f"Processing groups {offset + 1}-{batch_end} of {group_count:,}"
                        )

                        conn.execute(f"""
                            INSERT INTO wetlands_dissolved_temp
                            SELECT 
                                ROW_NUMBER() OVER () + {offset} as wetland_id,
                                final_group_id,
                                ST_Union_Agg(ws.geometry) as geometry,
                                -- Select dominant peat percentage with >12% priority
                                CASE 
                                    WHEN bool_or(ws.toerv_pct = '>12') THEN '>12'
                                    WHEN bool_or(ws.toerv_pct = '6-12') THEN '6-12'
                                    ELSE mode(ws.toerv_pct)  -- Most frequent for other values
                                END as dominant_toerv_pct
                            FROM wetland_groups wg
                            JOIN wetlands_spatial ws ON wg.wetland_id = ws.wetland_id
                            WHERE final_group_id >= {offset + 1} AND final_group_id <= {batch_end}
                            GROUP BY final_group_id
                        """)
                else:
                    # Process all at once for smaller datasets
                    conn.execute("DROP TABLE IF EXISTS wetlands_dissolved_temp")
                    conn.execute("""
                        CREATE TABLE wetlands_dissolved_temp AS
                        SELECT 
                            ROW_NUMBER() OVER () as wetland_id,
                            final_group_id,
                            ST_Union_Agg(ws.geometry) as geometry,
                            -- Select dominant peat percentage with >12% priority
                            CASE 
                                WHEN bool_or(ws.toerv_pct = '>12') THEN '>12'
                                WHEN bool_or(ws.toerv_pct = '6-12') THEN '6-12'
                                ELSE mode(ws.toerv_pct)  -- Most frequent for other values
                            END as dominant_toerv_pct
                        FROM wetland_groups wg
                        JOIN wetlands_spatial ws ON wg.wetland_id = ws.wetland_id
                        GROUP BY final_group_id
                    """)
            else:
                # No adjacent wetlands found, keep original geometries
                self.log.info("No adjacent wetlands found, keeping original geometries...")
                conn.execute("DROP TABLE IF EXISTS wetlands_dissolved_temp")
                conn.execute("""
                    CREATE TABLE wetlands_dissolved_temp AS
                    SELECT 
                        wetland_id,
                        wetland_id as final_group_id,
                        geometry,
                        toerv_pct as dominant_toerv_pct
                    FROM wetlands_spatial
                """)

            # Create final dissolved table WITH PEAT PERCENTAGE PRESERVATION
            dissolved_table_name = f"{dataset}_dissolved"
            conn.execute(f"DROP TABLE IF EXISTS {dissolved_table_name}")

            # Create final dissolved table - just wetland_id, geometry, and toerv_pct
            conn.execute(f"""
                CREATE TABLE {dissolved_table_name} AS
                SELECT 
                    wetland_id,
                    geometry,
                    dominant_toerv_pct as toerv_pct
                FROM wetlands_dissolved_temp
                ORDER BY wetland_id
            """)

            dissolved_count = conn.execute(
                f"SELECT COUNT(*) FROM {dissolved_table_name}"
            ).fetchone()[0]
            self.log.info(f"Created {dissolved_count:,} dissolved wetland polygons")

            self.log.info(f"Reduced from {feature_count:,} to {dissolved_count:,} features")
            self.log.info("Analyzing dissolved geometries:")
            self.log_geometry_statistics(dissolved_table_name)

            self.log.info(
                f"Dissolved {feature_count:,} features into "
                f"{dissolved_count:,} geometries using DuckDB-spatial in original coordinates"
            )
            return dissolved_table_name

        except Exception as e:
            self.log.error(f"Error during DuckDB-spatial dissolve operation: {str(e)}")
            raise e

    async def run(self, bronze_data: Optional[Any] = None) -> Optional[dict[str, Any]]:
        """
        Run the wetlands silver layer processing pipeline.

        This method orchestrates the entire data processing workflow:
        1. Reads raw data from the bronze layer
        2. Processes XML data into a DuckDB spatial table
        3. Creates a dissolved version with merged adjacent polygons
        4. Saves both the original and dissolved datasets to GCS

        Args:
            bronze_data: Optional in-memory data from bronze stage. If provided,
                        this data will be used instead of reading from storage.

        The method handles error conditions at each step and logs progress.

        Returns:
            dict[str, Any]: Success information including dataset name, timestamps, and status
            None: If processing failed

        Note:
            This is the main entry point for the silver layer processing of wetlands data.
        """
        self.log.info("Running Wetlands silver job")
        async with AsyncTimer("Wetlands silver job"):
            # Read data with support for in-memory passing
            if bronze_data is not None:
                self.log.info("Using bronze data from memory (in-memory data passing)")
                # Bronze data is expected to be a list of raw XML strings
                if isinstance(bronze_data, list):
                    # ✅ MIGRATION: Create table using DuckDB instead of wasteful  conversion
                    conn = self.conn
                    current_timestamp = conn.execute("SELECT current_timestamp").fetchone()[0]

                    # Create table directly without  conversion
                    conn.execute(
                        """CREATE OR REPLACE TABLE temp_raw_data (
                            payload VARCHAR, 
                            source VARCHAR, 
                            created_at TIMESTAMP, 
                            updated_at TIMESTAMP
                        )"""
                    )

                    # Insert data directly into table
                    for xml_data in bronze_data:
                        conn.execute(
                            "INSERT INTO temp_raw_data VALUES (?, ?, ?, ?)",
                            [xml_data, self.config.dataset, current_timestamp, current_timestamp],
                        )

                    # Use table name instead of
                    raw_data_table = "temp_raw_data"
                else:
                    self.log.error(
                        f"Expected list of XML strings from bronze stage, got {type(bronze_data)}"
                    )
                    return None
            else:
                # Fallback to reading from storage
                self.log.info("Reading bronze data from storage (fallback)")
                raw_data = self._read_bronze_data(self.config.dataset, self.config.bucket)
                if raw_data is None:
                    self.log.error("Failed to read raw data")
                    return None

            self.log.info("Read raw data successfully")

            # Determine what to pass to processing method
            if bronze_data is not None and isinstance(bronze_data, list):
                # Use the table we created from bronze data
                processing_input = raw_data_table
            else:
                # Use the  from storage (backward compatibility)
                processing_input = raw_data

            table_name = self._process_xml_data(processing_input)
            if table_name is None:
                self.log.error("Failed to process raw data")
                return None
            self.log.info("Processed raw data successfully")

            # Note: Dissolved table is created within _process_xml_data before coordinate transformation
            dissolved_table_name = f"{self.config.dataset}_dissolved"

            # ✅ MIGRATION: Save both tables to GCS using optimized save methods
            self.save_data_direct(table_name, self.config.dataset, self.config.bucket, "silver")
            self.save_data_direct(
                dissolved_table_name,
                f"{self.config.dataset}_dissolved",
                self.config.bucket,
                "silver",
            )
            self.log.info("Saved processed data successfully")

            # Return success information for the main app
            return {
                "dataset": self.config.dataset,
                "processed_at": self.conn.execute("SELECT current_timestamp")
                .fetchone()[0]
                .isoformat(),
                "status": "completed",
                "tables_created": [self.config.dataset, f"{self.config.dataset}_dissolved"],
            }

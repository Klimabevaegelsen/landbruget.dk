"""
Silver layer processing for GEUS Borehole Pesticides data.

This module handles the transformation of raw GML data from the bronze layer.
It parses GML/XML data, extracts borehole locations and pesticide analyses,
filters for tracked pesticides, and joins the datasets.

The module contains:
- GEUSBoreholePesticidesSilverConfig: Configuration class for the processing
- GEUSBoreholePesticidesSilver: Implementation class for transforming and analyzing data

The processing includes GML parsing, coordinate validation, pesticide filtering,
and joining boreholes with their associated analyses.
"""

import xml.etree.ElementTree as ET
from typing import Any, ClassVar

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed


class GEUSBoreholePesticidesSilverConfig(BaseJobConfig):
    """
    Configuration for the GEUS Borehole Pesticides Silver processing.

    This class defines all configuration parameters needed for transforming
    borehole pesticides data from the bronze layer to the silver layer.

    Attributes:
        dataset (str): Name of the dataset in storage
        bucket (str): GCS bucket name for data storage
        source_crs (str): Source coordinate reference system
        storage_batch_size (int): Batch size for storage operations
        namespaces (dict[str, str]): XML namespaces used in the GEUS data
        tracked_pesticides (dict[int, str]): Pesticide stofnr codes to track
    """

    dataset: str = "geus_borehole_pesticides"
    bucket: str = "landbrugsdata-raw-data"
    source_crs: str = "EPSG:25832"
    storage_batch_size: int = 5000

    # XML namespaces for GEUS GML data
    # Note: GEUS uses MapServer, so features are in 'ms' namespace, not 'geus'
    namespaces: ClassVar[dict[str, str]] = {
        "wfs": "http://www.opengis.net/wfs/2.0",
        "ms": "http://mapserver.gis.umn.edu/mapserver",  # Actual feature namespace
        "gml": "http://www.opengis.net/gml/3.2",  # GML 3.2 (verified from GEUS API response)
    }

    # Tracked pesticides and groundwater contaminants (stofnr -> substance name)
    # Based on substances available in GEUS jupiter_anlaegsanalyser WFS layer
    # Note: The WFS layer contains a subset of all monitored substances.
    # For comprehensive pesticide data, the full Jupiter database should be queried.
    #
    # Currently tracking ALL pesticide-related substances available in the WFS:
    tracked_pesticides: ClassVar[dict[int, str]] = {
        # === Pesticides and metabolites (verified in WFS data) ===
        438: "2,6-Dichlorbenzamid (BAM)",  # From dichlobenil - most detected in DK
        846: "Atrazin",  # Triazine herbicide, banned 1994
        613: "Chloridazon",  # Herbicide (beet crops)
        1448: "Desphenyl chloridazon (DPC)",  # Chloridazon metabolite - high detection
        1534: "Methyl-desphenyl-chloridazon (MDPC)",  # Chloridazon metabolite
        2251: "Trifluoreddikesyre (TFA)",  # PFAS-related degradation product
        # === Chlorinated solvents (industrial contamination, often co-located) ===
        374: "Trichlormethan (chloroform)",  # Chlorinated solvent
        379: "Tetrachlorethylen (PCE)",  # Dry cleaning solvent
        380: "Trichlorethylen (TCE)",  # Industrial degreaser
        1171: "Vinylchlorid",  # TCE/PCE degradation product, carcinogenic
        # === Fuel-related contaminants ===
        166: "Methyl-tert-butylether (MTBE)",  # Gasoline additive
        215: "Benzen",  # BTEX component, carcinogenic
        218: "Toluen",  # BTEX component
    }

    # Note: Additional pesticides commonly found in Danish groundwater that are NOT
    # in the jupiter_anlaegsanalyser WFS layer but are in the full Jupiter database:
    # - Simazin, DEA, DIA, DEIA (triazine metabolites)
    # - Bentazon (mandatory monitoring since 1998)
    # - MCPA, Mecoprop, Dichlorprop (phenoxy acids)
    # - Glyphosat, AMPA
    # - 1,2,4-Triazol (fungicide metabolite)
    # - DMS (N,N-dimethylsulfamid)
    # - Isoproturon, Diuron (urea herbicides)
    #
    # To track these, the bronze layer would need to query additional Jupiter APIs
    # or use a different data source (e.g., direct Jupiter database access).


class GEUSBoreholePesticidesSilver(
    BaseSource[GEUSBoreholePesticidesSilverConfig], SilverJobInterface
):
    """
    Silver layer processor for GEUS borehole pesticides data.

    This class handles the processing of borehole and pesticide analysis data
    from the bronze layer to the silver layer. It parses GML data, filters for
    tracked pesticides, and joins boreholes with their analyses.

    Key functionalities include:
    1. Parsing GML data to extract boreholes and analyses
    2. Filtering analyses for tracked pesticide substances
    3. Joining analyses to borehole locations
    4. Validating geometries within Denmark bounds
    """

    def __init__(self, config: GEUSBoreholePesticidesSilverConfig):
        """
        Initialize the GEUSBoreholePesticidesSilver processor.

        Args:
            config (GEUSBoreholePesticidesSilverConfig): Configuration object
        """
        super().__init__(config)
        self.log.info(
            "GEUSBoreholePesticidesSilver: Using unified GCS access and DuckDB connection"
        )

    def _get_element_text(self, element: ET.Element, path: str) -> str | None:
        """
        Get text content from an XML element by path.

        Args:
            element: Parent XML element
            path: XPath to child element

        Returns:
            Text content or None if not found
        """
        child = element.find(path, self.config.namespaces)
        return child.text if child is not None and child.text else None

    def _parse_borehole_feature(self, feature: ET.Element) -> dict[str, Any] | None:
        """
        Parse a borehole feature from GML.

        Extracts dgunr (unique ID), anlaegid, coordinates, and metadata.

        Args:
            feature: XML element containing borehole data

        Returns:
            Dictionary with borehole data or None if parsing fails
        """
        try:
            # Extract basic identifiers (fields are in MapServer 'ms:' namespace)
            dgunr = self._get_element_text(feature, "ms:dgunr")
            anlaegid = self._get_element_text(feature, "ms:anlaegid")

            if not dgunr:
                # Debug: log first failure reason
                if not hasattr(self, "_debug_no_dgunr_logged"):
                    self._debug_no_dgunr_logged = True
                    self.log.warning(f"First feature missing dgunr. Feature tag: {feature.tag}")
                    # Log first few child tags to understand structure
                    children = list(feature)[:5]
                    self.log.warning(f"  Feature children: {[c.tag for c in children]}")
                return None

            # Extract coordinates from GML point (geometry is in ms:msGeometry wrapper)
            point = feature.find(".//gml:Point", self.config.namespaces)
            if point is None:
                if not hasattr(self, "_debug_no_point_logged"):
                    self._debug_no_point_logged = True
                    self.log.warning(f"First feature missing gml:Point. dgunr={dgunr}")
                    # Debug: show children and try different XPath
                    children = list(feature)[:10]
                    self.log.warning(f"  Feature children tags: {[c.tag for c in children]}")
                    # Try without namespace
                    point_no_ns = feature.find(".//{http://www.opengis.net/gml}Point")
                    self.log.warning(f"  Point with explicit NS: {point_no_ns}")
                    # Try via ms:msGeometry
                    geom = feature.find("ms:msGeometry", self.config.namespaces)
                    self.log.warning(f"  ms:msGeometry element: {geom}")
                    if geom is not None:
                        geom_children = list(geom)
                        self.log.warning(f"  msGeometry children: {[c.tag for c in geom_children]}")
                return None

            pos = point.find("gml:pos", self.config.namespaces)
            if pos is None or not pos.text:
                if not hasattr(self, "_debug_no_pos_logged"):
                    self._debug_no_pos_logged = True
                    self.log.warning(f"First feature missing gml:pos. dgunr={dgunr}")
                return None

            # Parse coordinates (format: "x y" in EPSG:25832)
            coords = pos.text.strip().split()
            if len(coords) != 2:
                return None

            x, y = float(coords[0]), float(coords[1])

            # Extract additional metadata (all fields use ms: prefix)
            return {
                "dgunr": dgunr,
                "anlaegid": anlaegid,
                "x": x,
                "y": y,
                "kommunenr": self._get_element_text(
                    feature, "ms:komnr"
                ),  # Note: 'komnr' not 'kommunenr'
                "kommunenavn": self._get_element_text(feature, "ms:kommunenavn"),
                "region_tekst": self._get_element_text(feature, "ms:region_tekst"),
                "dybde": self._get_element_text(feature, "ms:dybde"),
                "formaal": self._get_element_text(
                    feature, "ms:formaal_tekst"
                ),  # Use full text version
                "boringsstatus": self._get_element_text(
                    feature, "ms:kode_tekst"
                ),  # Status is in kode_tekst
                "anlaegtype": self._get_element_text(feature, "ms:hovedtype"),  # Facility type
            }
        except Exception as e:
            self.log.debug(f"Error parsing borehole feature: {e}")
            return None

    def _parse_analysis_feature(self, feature: ET.Element) -> dict[str, Any] | None:
        """
        Parse an analysis feature from GML.

        Extracts substance information, measurements, sample dates, and coordinates.
        Each analysis record includes its own location data.

        Args:
            feature: XML element containing analysis data

        Returns:
            Dictionary with analysis data or None if parsing fails
        """
        try:
            # Extract substance identifiers (fields are in MapServer 'ms:' namespace)
            stofnr_str = self._get_element_text(feature, "ms:stofnr_num")
            if not stofnr_str:
                return None

            stofnr = int(stofnr_str)

            # Only keep tracked pesticides
            if stofnr not in self.config.tracked_pesticides:
                return None

            anlaegid_str = self._get_element_text(feature, "ms:anlaegid_num")
            if not anlaegid_str:
                return None

            # Extract coordinates - each analysis has its own location
            x_str = self._get_element_text(feature, "ms:xutm32euref89")
            y_str = self._get_element_text(feature, "ms:yutm32euref89")
            x = float(x_str) if x_str else None
            y = float(y_str) if y_str else None

            # Extract measurement data
            maengde_str = self._get_element_text(feature, "ms:maengde_num")
            maengde = float(maengde_str) if maengde_str else None

            return {
                "anlaegid": anlaegid_str,
                "stofnr": stofnr,
                "stof": self._get_element_text(feature, "ms:stof"),
                "stof_status": self._get_element_text(feature, "ms:stof_status"),
                "maengde": maengde,
                "enhed": self._get_element_text(feature, "ms:enhed"),
                "proevedato": self._get_element_text(feature, "ms:proevedato"),
                "x": x,  # UTM32 EUREF89 easting
                "y": y,  # UTM32 EUREF89 northing
                "anlaeg": self._get_element_text(feature, "ms:anlaeg"),  # Facility name
                "kommune": self._get_element_text(feature, "ms:kommune"),
                "virksomhedstype": self._get_element_text(feature, "ms:virksomhedstype"),
            }
        except Exception as e:
            self.log.debug(f"Error parsing analysis feature: {e}")
            return None

    @timed(name="Parsing boreholes GML data")
    def _parse_boreholes_gml(self, gml_data: list[str]) -> list[dict[str, Any]]:
        """
        Parse all borehole features from GML data chunks.

        Args:
            gml_data: List of GML XML strings

        Returns:
            List of parsed borehole dictionaries
        """
        boreholes = []
        seen_dgunr = set()  # Track seen DGU numbers to avoid duplicates

        for i, gml_chunk in enumerate(gml_data):
            try:
                root = ET.fromstring(gml_chunk)

                # Debug: Log first chunk structure to understand format
                if i == 0:
                    self.log.info(f"First chunk root tag: {root.tag}")
                    members = root.findall(".//wfs:member", self.config.namespaces)
                    self.log.info(f"Found {len(members)} wfs:member elements in first chunk")
                    if members:
                        for child in members[0]:
                            self.log.info(f"  First member child tag: {child.tag}")

                # Find all borehole features via wfs:member path (most reliable)
                for member in root.findall(".//wfs:member", self.config.namespaces):
                    for child in member:
                        if "jupiter_boringer" in child.tag.lower():
                            parsed = self._parse_borehole_feature(child)
                            if parsed and parsed["dgunr"] not in seen_dgunr:
                                boreholes.append(parsed)
                                seen_dgunr.add(parsed["dgunr"])

                # Also try direct path for formats without wfs:member wrapper
                for feature in root.findall(".//ms:jupiter_boringer_ws", self.config.namespaces):
                    parsed = self._parse_borehole_feature(feature)
                    if parsed and parsed["dgunr"] not in seen_dgunr:
                        boreholes.append(parsed)
                        seen_dgunr.add(parsed["dgunr"])

                if (i + 1) % 10 == 0:
                    self.log.info(
                        f"Processed {i + 1}/{len(gml_data)} borehole chunks, {len(boreholes):,} boreholes so far"
                    )

            except ET.ParseError as e:
                self.log.warning(f"Failed to parse borehole GML chunk {i}: {e}")
                continue

        self.log.info(f"Parsed {len(boreholes):,} boreholes from {len(gml_data)} GML chunks")
        return boreholes

    @timed(name="Parsing analyses GML data")
    def _parse_analyses_gml(self, gml_data: list[str]) -> list[dict[str, Any]]:
        """
        Parse all analysis features from GML data chunks, filtering for tracked pesticides.

        Args:
            gml_data: List of GML XML strings

        Returns:
            List of parsed analysis dictionaries (only tracked pesticides)
        """
        analyses = []
        total_analyses_seen = 0
        # Track seen analyses by (anlaegid, stofnr, proevedato) to avoid duplicates
        seen_analyses = set()

        for i, gml_chunk in enumerate(gml_data):
            try:
                root = ET.fromstring(gml_chunk)

                # Find all analysis features via wfs:member path (most reliable)
                for member in root.findall(".//wfs:member", self.config.namespaces):
                    for child in member:
                        if "anlaegsanalyser" in child.tag.lower():
                            total_analyses_seen += 1
                            parsed = self._parse_analysis_feature(child)
                            if parsed:
                                key = (
                                    parsed["anlaegid"],
                                    parsed["stofnr"],
                                    parsed.get("proevedato"),
                                )
                                if key not in seen_analyses:
                                    analyses.append(parsed)
                                    seen_analyses.add(key)

                # Also try direct path for formats without wfs:member wrapper
                for feature in root.findall(
                    ".//ms:jupiter_anlaegsanalyser", self.config.namespaces
                ):
                    total_analyses_seen += 1
                    parsed = self._parse_analysis_feature(feature)
                    if parsed:
                        key = (parsed["anlaegid"], parsed["stofnr"], parsed.get("proevedato"))
                        if key not in seen_analyses:
                            analyses.append(parsed)
                            seen_analyses.add(key)

                if (i + 1) % 10 == 0:
                    self.log.info(
                        f"Processed {i + 1}/{len(gml_data)} analysis chunks, "
                        f"{len(analyses):,} pesticide analyses found"
                    )

            except ET.ParseError as e:
                self.log.warning(f"Failed to parse analysis GML chunk {i}: {e}")
                continue

        self.log.info(
            f"Filtered {len(analyses):,} pesticide analyses from {total_analyses_seen:,} "
            f"total analyses across {len(gml_data)} GML chunks"
        )

        # Log breakdown by pesticide type
        from collections import Counter

        pesticide_counts = Counter(a["stofnr"] for a in analyses)
        for stofnr, count in pesticide_counts.most_common():
            pesticide_name = self.config.tracked_pesticides.get(stofnr, "Unknown")
            self.log.info(f"  - {pesticide_name} (stofnr={stofnr}): {count:,} analyses")

        return analyses

    @timed(name="Creating DuckDB tables")
    def _create_duckdb_tables(
        self, boreholes: list[dict], analyses: list[dict]
    ) -> tuple[str, str, str]:
        """
        Create DuckDB tables from parsed data and perform join.

        Creates three tables:
        1. geus_boreholes - All borehole locations
        2. geus_pesticide_analyses - Filtered pesticide analyses
        3. geus_borehole_pesticides_joined - Boreholes with pesticide detections

        Args:
            boreholes: List of parsed borehole dictionaries
            analyses: List of parsed analysis dictionaries

        Returns:
            Tuple of table names (boreholes, analyses, joined)
        """
        conn = self.conn

        # Create boreholes table
        self.log.info("Creating boreholes table...")
        conn.execute("DROP TABLE IF EXISTS geus_boreholes")
        conn.execute("""
            CREATE TABLE geus_boreholes (
                dgunr VARCHAR,
                anlaegid VARCHAR,
                x DOUBLE,
                y DOUBLE,
                kommunenr VARCHAR,
                kommunenavn VARCHAR,
                region_tekst VARCHAR,
                dybde VARCHAR,
                formaal VARCHAR,
                boringsstatus VARCHAR,
                anlaegtype VARCHAR,
                geometry GEOMETRY
            )
        """)

        # Insert boreholes in batches
        batch_size = 1000
        for i in range(0, len(boreholes), batch_size):
            batch = boreholes[i : i + batch_size]
            for bh in batch:
                conn.execute(
                    """
                    INSERT INTO geus_boreholes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ST_Point(?, ?))
                    """,
                    [
                        bh["dgunr"],
                        bh["anlaegid"],
                        bh["x"],
                        bh["y"],
                        bh.get("kommunenr"),
                        bh.get("kommunenavn"),
                        bh.get("region_tekst"),
                        bh.get("dybde"),
                        bh.get("formaal"),
                        bh.get("boringsstatus"),
                        bh.get("anlaegtype"),
                        bh["x"],
                        bh["y"],
                    ],
                )

            if (i + batch_size) % 50000 == 0:
                self.log.info(
                    f"Inserted {min(i + batch_size, len(boreholes)):,}/{len(boreholes):,} boreholes"
                )

        self.log.info(f"Created geus_boreholes table with {len(boreholes):,} records")

        # Create analyses table with coordinates (each analysis has its own location)
        self.log.info("Creating pesticide analyses table...")
        conn.execute("DROP TABLE IF EXISTS geus_pesticide_analyses")
        conn.execute("""
            CREATE TABLE geus_pesticide_analyses (
                anlaegid VARCHAR,
                stofnr INTEGER,
                stof VARCHAR,
                stof_status VARCHAR,
                maengde DOUBLE,
                enhed VARCHAR,
                proevedato VARCHAR,
                x DOUBLE,
                y DOUBLE,
                anlaeg VARCHAR,
                kommune VARCHAR,
                virksomhedstype VARCHAR,
                geometry GEOMETRY
            )
        """)

        # Insert analyses in batches
        for i in range(0, len(analyses), batch_size):
            batch = analyses[i : i + batch_size]
            for an in batch:
                x = an.get("x")
                y = an.get("y")
                conn.execute(
                    """
                    INSERT INTO geus_pesticide_analyses VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        CASE WHEN ? IS NOT NULL AND ? IS NOT NULL
                             THEN ST_Point(?, ?)
                             ELSE NULL END
                    )
                    """,
                    [
                        an["anlaegid"],
                        an["stofnr"],
                        an.get("stof"),
                        an.get("stof_status"),
                        an.get("maengde"),
                        an.get("enhed"),
                        an.get("proevedato"),
                        x,
                        y,
                        an.get("anlaeg"),
                        an.get("kommune"),
                        an.get("virksomhedstype"),
                        x,
                        y,
                        x,
                        y,  # For CASE WHEN and ST_Point
                    ],
                )

        self.log.info(f"Created geus_pesticide_analyses table with {len(analyses):,} records")

        # Create joined table - boreholes with pesticide detections
        self.log.info("Creating joined table...")
        conn.execute("DROP TABLE IF EXISTS geus_borehole_pesticides_joined")
        conn.execute("""
            CREATE TABLE geus_borehole_pesticides_joined AS
            SELECT
                b.dgunr,
                b.anlaegid,
                a.stofnr,
                a.stof,
                a.stof_status,
                a.maengde,
                a.enhed,
                a.proevedato,
                b.kommunenavn,
                b.region_tekst,
                b.dybde,
                b.geometry,
                CASE
                    WHEN a.maengde > 0 THEN true
                    ELSE false
                END as is_detection
            FROM geus_boreholes b
            INNER JOIN geus_pesticide_analyses a ON b.anlaegid = a.anlaegid
        """)

        joined_count = conn.execute(
            "SELECT COUNT(*) FROM geus_borehole_pesticides_joined"
        ).fetchone()[0]
        self.log.info(
            f"Created geus_borehole_pesticides_joined table with {joined_count:,} records"
        )

        # Log statistics
        detection_count = conn.execute(
            "SELECT COUNT(*) FROM geus_borehole_pesticides_joined WHERE is_detection = true"
        ).fetchone()[0]
        self.log.info(f"  - Positive detections: {detection_count:,}")
        self.log.info(f"  - Non-detections: {joined_count - detection_count:,}")

        # Log unique boreholes with pesticide data
        unique_boreholes = conn.execute(
            "SELECT COUNT(DISTINCT dgunr) FROM geus_borehole_pesticides_joined"
        ).fetchone()[0]
        self.log.info(f"  - Unique boreholes with pesticide data: {unique_boreholes:,}")

        return "geus_boreholes", "geus_pesticide_analyses", "geus_borehole_pesticides_joined"

    @timed(name="Validating geometries")
    def _validate_geometries(self, table_name: str) -> None:
        """
        Validate geometries are within Denmark bounds (EPSG:25832).

        Args:
            table_name: Name of table to validate
        """
        conn = self.conn

        # Denmark bounds in EPSG:25832 (UTM zone 32N)
        # Approximate: X: 400000-900000, Y: 6000000-6500000
        invalid_count = conn.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE geometry IS NOT NULL
            AND NOT ST_Within(
                geometry,
                ST_MakeEnvelope(400000, 6000000, 900000, 6500000)
            )
        """).fetchone()[0]

        total_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        if invalid_count > 0:
            self.log.warning(
                f"{invalid_count:,} of {total_count:,} geometries outside Denmark bounds"
            )
        else:
            self.log.info(f"All {total_count:,} geometries validated within Denmark bounds")

    async def run(self, bronze_data: Any | None = None) -> dict[str, Any] | None:
        """
        Run the GEUS borehole pesticides silver layer processing pipeline.

        This method orchestrates the entire data processing workflow:
        1. Reads raw GML data from bronze layer
        2. Parses boreholes and analyses from GML
        3. Filters analyses for tracked pesticides
        4. Creates joined dataset
        5. Saves to GCS

        Args:
            bronze_data: Optional in-memory data from bronze stage
                        Expected format: {"boreholes": [gml_strings], "analyses": [gml_strings]}

        Returns:
            dict[str, Any]: Success information including dataset name and status
            None: If processing failed
        """
        self.log.info("Running GEUS Borehole Pesticides silver job")

        async with AsyncTimer("GEUS Borehole Pesticides silver job"):
            # Get data either from memory or storage
            if bronze_data is not None:
                self.log.info("Using bronze data from memory (in-memory data passing)")
                if not isinstance(bronze_data, dict):
                    self.log.error(f"Expected dict from bronze stage, got {type(bronze_data)}")
                    return None

                boreholes_gml = bronze_data.get("boreholes", [])
                analyses_gml = bronze_data.get("analyses", [])
            else:
                # Read from storage
                self.log.info("Reading bronze data from storage (fallback)")
                raw_data = self._read_bronze_data(self.config.dataset, self.config.bucket)
                if raw_data is None:
                    self.log.error("Failed to read bronze data")
                    return None

                # Parse the stored data - it should have layer_type column
                conn = self.conn
                boreholes_rows = conn.execute(
                    "SELECT payload FROM raw_data WHERE layer_type = 'boreholes'"
                ).fetchall()
                analyses_rows = conn.execute(
                    "SELECT payload FROM raw_data WHERE layer_type = 'analyses'"
                ).fetchall()

                boreholes_gml = [row[0] for row in boreholes_rows]
                analyses_gml = [row[0] for row in analyses_rows]

            self.log.info(
                f"Processing {len(boreholes_gml)} borehole chunks and "
                f"{len(analyses_gml)} analysis chunks"
            )

            # Parse GML data
            boreholes = self._parse_boreholes_gml(boreholes_gml)
            if not boreholes:
                self.log.error("No boreholes parsed from GML data")
                return None

            analyses = self._parse_analyses_gml(analyses_gml)
            if not analyses:
                self.log.warning(
                    "No pesticide analyses found - this may be expected if no pesticides detected"
                )

            # Create DuckDB tables
            boreholes_table, analyses_table, joined_table = self._create_duckdb_tables(
                boreholes, analyses
            )

            # Validate geometries
            self._validate_geometries(boreholes_table)
            if analyses:
                self._validate_geometries(joined_table)

            # Save to GCS
            self.log.info("Saving data to GCS...")

            # Save boreholes (all boreholes, not just those with pesticides)
            self.save_data_direct(boreholes_table, "geus_boreholes", self.config.bucket, "silver")

            # Save pesticide analyses if we have any
            if analyses:
                self.save_data_direct(
                    analyses_table, "geus_pesticide_analyses", self.config.bucket, "silver"
                )
                self.save_data_direct(
                    joined_table, "geus_borehole_pesticides", self.config.bucket, "silver"
                )

            self.log.info("GEUS Borehole Pesticides silver job completed successfully")

            return {
                "dataset": self.config.dataset,
                "processed_at": self.conn.execute("SELECT current_timestamp")
                .fetchone()[0]
                .isoformat(),
                "status": "completed",
                "tables_created": [
                    "geus_boreholes",
                    "geus_pesticide_analyses" if analyses else None,
                    "geus_borehole_pesticides" if analyses else None,
                ],
                "statistics": {
                    "total_boreholes": len(boreholes),
                    "total_pesticide_analyses": len(analyses),
                },
            }

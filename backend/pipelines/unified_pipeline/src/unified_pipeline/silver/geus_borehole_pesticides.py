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
        tracked_pesticides (dict[int, str]): Pesticide stofnr codes to track (legacy)
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

    # Tracked pesticides for jupiter_anlaegsanalyser (legacy - limited to substances in WFS)
    # These are kept for backward compatibility with the facility analyses layer.
    # The mc_analyse layer provides ALL pesticides via stofgruppe=50 filter.
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

    # Note: The mc_analyse layer with stofgruppe=50 filter provides comprehensive
    # pesticide data including substances NOT in jupiter_anlaegsanalyser:
    # - Simazin, DEA, DIA, DEIA (triazine metabolites)
    # - Bentazon (mandatory monitoring since 1998)
    # - MCPA, Mecoprop, Dichlorprop (phenoxy acids)
    # - Glyphosat, AMPA
    # - 1,2,4-Triazol (fungicide metabolite)
    # - DMS (N,N-dimethylsulfamid)
    # - Isoproturon, Diuron (urea herbicides)
    # - And many more pesticides and metabolites


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
                "data_source": "jupiter_anlaegsanalyser",
            }
        except Exception as e:
            self.log.debug(f"Error parsing analysis feature: {e}")
            return None

    def _parse_mc_analyse_feature(self, feature: ET.Element) -> dict[str, Any] | None:
        """
        Parse a feature from the mc_analyse WFS layer.

        The mc_analyse layer contains comprehensive groundwater chemical analyses
        filtered by stofgruppe=50 (pesticides). This layer has different field names
        than jupiter_anlaegsanalyser.

        Field mapping (mc_analyse WFS fields):
        - dgu_nr: DGU borehole number (e.g., "212. 2283")
        - anlaeg_id: Facility ID (may be null)
        - stofnr: Substance number
        - stof_tekst: Substance name
        - seneste_analysevaerdi: Latest analysis value
        - dato_seneste_analyse: Date of latest analysis
        - xutm32euref89, yutm32euref89: Coordinates in UTM32 EUREF89
        - kommune_navn: Municipality name

        Args:
            feature: XML element containing mc_analyse data

        Returns:
            Dictionary with analysis data or None if parsing fails
        """
        try:
            # Extract substance identifiers
            stofnr_str = self._get_element_text(feature, "ms:stofnr")
            if not stofnr_str:
                return None

            stofnr = int(stofnr_str)

            # Get DGU number (primary identifier for boreholes in mc_analyse)
            # Note: Field is dgu_nr (with underscore), not dgunr
            dgunr = self._get_element_text(feature, "ms:dgu_nr")

            # Get facility ID as fallback identifier
            anlaeg_id = self._get_element_text(feature, "ms:anlaeg_id")

            # Use dgunr if available, fall back to anlaeg_id
            # Note: dgunr may contain spaces (e.g., "212. 2283")
            identifier = dgunr or anlaeg_id
            if not identifier:
                return None

            # Extract coordinates - mc_analyse uses xutm32euref89/yutm32euref89
            x_str = self._get_element_text(feature, "ms:xutm32euref89")
            y_str = self._get_element_text(feature, "ms:yutm32euref89")
            x = float(x_str) if x_str else None
            y = float(y_str) if y_str else None

            # Extract measurement data - mc_analyse uses seneste_analysevaerdi
            analysevaerdi_str = self._get_element_text(feature, "ms:seneste_analysevaerdi")
            maengde = float(analysevaerdi_str) if analysevaerdi_str else None

            # Get substance name - mc_analyse uses stof_tekst
            stof = self._get_element_text(feature, "ms:stof_tekst")

            # Get sample date - field is dato_seneste_analyse
            proevedato = self._get_element_text(feature, "ms:dato_seneste_analyse")

            # Get location info - field is kommune_navn
            kommune = self._get_element_text(feature, "ms:kommune_navn")

            # Get facility name
            anlaeg_navn = self._get_element_text(feature, "ms:anlaegs_navn")

            return {
                "anlaegid": anlaeg_id,  # Facility ID (may be null)
                "dgunr": dgunr,  # DGU borehole number (primary identifier)
                "stofnr": stofnr,
                "stof": stof,
                "stof_status": None,  # Not available in mc_analyse
                "maengde": maengde,
                "enhed": self._get_element_text(feature, "ms:enhed"),
                "proevedato": proevedato,
                "x": x,  # UTM32 EUREF89 easting
                "y": y,  # UTM32 EUREF89 northing
                "anlaeg": anlaeg_navn,  # Facility name
                "kommune": kommune,
                "virksomhedstype": self._get_element_text(feature, "ms:virksomhedstype_tekst"),
                "data_source": "mc_analyse",
            }
        except Exception as e:
            self.log.debug(f"Error parsing mc_analyse feature: {e}")
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
            f"total analyses across {len(gml_data)} GML chunks (jupiter_anlaegsanalyser)"
        )

        # Log breakdown by pesticide type
        from collections import Counter

        pesticide_counts = Counter(a["stofnr"] for a in analyses)
        for stofnr, count in pesticide_counts.most_common():
            pesticide_name = self.config.tracked_pesticides.get(stofnr, "Unknown")
            self.log.info(f"  - {pesticide_name} (stofnr={stofnr}): {count:,} analyses")

        return analyses

    @timed(name="Parsing mc_analyse pesticide GML data")
    def _parse_pesticide_analyses_gml(self, gml_data: list[str]) -> list[dict[str, Any]]:
        """
        Parse all pesticide features from mc_analyse GML data chunks.

        The mc_analyse layer is pre-filtered by stofgruppe=50 (pesticides) at the
        bronze layer, so all features here are pesticide analyses.

        Args:
            gml_data: List of GML XML strings from mc_analyse

        Returns:
            List of parsed pesticide analysis dictionaries
        """
        analyses = []
        total_features_seen = 0
        # Track seen analyses by (identifier, stofnr, proevedato) to avoid duplicates
        seen_analyses: set[tuple[str, int, str | None]] = set()

        for i, gml_chunk in enumerate(gml_data):
            try:
                root = ET.fromstring(gml_chunk)

                # Debug: Log first chunk structure
                if i == 0:
                    self.log.info(f"[mc_analyse] First chunk root tag: {root.tag}")
                    members = root.findall(".//wfs:member", self.config.namespaces)
                    self.log.info(f"[mc_analyse] Found {len(members)} wfs:member elements")
                    # Debug: Log first member's children to understand structure
                    if members:
                        first_member = members[0]
                        children = list(first_member)
                        self.log.info(
                            f"[mc_analyse] First member has {len(children)} children. "
                            f"Tags: {[c.tag for c in children[:3]]}"
                        )
                        for child in children[:1]:
                            self.log.info(f"[mc_analyse] First child tag: '{child.tag}'")
                            self.log.info(
                                f"[mc_analyse] 'mc_analyse' in tag.lower(): "
                                f"{'mc_analyse' in child.tag.lower()}"
                            )

                # Find all mc_analyse features via wfs:member path
                chunk_features_found = 0
                for member in root.findall(".//wfs:member", self.config.namespaces):
                    for child in member:
                        if "mc_analyse" in child.tag.lower():
                            total_features_seen += 1
                            chunk_features_found += 1
                            parsed = self._parse_mc_analyse_feature(child)
                            if parsed:
                                key = (
                                    parsed["anlaegid"],
                                    parsed["stofnr"],
                                    parsed.get("proevedato"),
                                )
                                if key not in seen_analyses:
                                    analyses.append(parsed)
                                    seen_analyses.add(key)

                # Debug: Log if no features found in this chunk
                if i == 0 and chunk_features_found == 0:
                    members = root.findall(".//wfs:member", self.config.namespaces)
                    self.log.warning(
                        f"[mc_analyse] First chunk: 0 features matched 'mc_analyse' in tag. "
                        f"Members found: {len(members)}"
                    )

                # Also try direct path for formats without wfs:member wrapper
                for feature in root.findall(".//ms:mc_analyse", self.config.namespaces):
                    total_features_seen += 1
                    parsed = self._parse_mc_analyse_feature(feature)
                    if parsed:
                        key = (parsed["anlaegid"], parsed["stofnr"], parsed.get("proevedato"))
                        if key not in seen_analyses:
                            analyses.append(parsed)
                            seen_analyses.add(key)

                if (i + 1) % 10 == 0:
                    self.log.info(
                        f"[mc_analyse] Processed {i + 1}/{len(gml_data)} chunks, "
                        f"{len(analyses):,} pesticide analyses found"
                    )

            except ET.ParseError as e:
                self.log.warning(f"[mc_analyse] Failed to parse GML chunk {i}: {e}")
                continue

        self.log.info(
            f"[mc_analyse] Parsed {len(analyses):,} pesticide analyses from "
            f"{total_features_seen:,} features across {len(gml_data)} GML chunks"
        )

        # Log breakdown by substance
        from collections import Counter

        substance_counts = Counter(a.get("stof", "Unknown") for a in analyses)
        self.log.info(f"[mc_analyse] Found {len(substance_counts)} unique pesticide substances")
        for substance, count in substance_counts.most_common(20):  # Top 20
            self.log.info(f"  - {substance}: {count:,} analyses")
        if len(substance_counts) > 20:
            self.log.info(f"  ... and {len(substance_counts) - 20} more substances")

        return analyses

    @timed(name="Creating DuckDB tables")
    def _create_duckdb_tables(
        self, boreholes: list[dict], analyses: list[dict]
    ) -> tuple[str, str, str]:
        """
        Create DuckDB tables from parsed data and perform join.

        Creates three tables:
        1. geus_boreholes - All borehole locations
        2. geus_pesticide_analyses - Combined pesticide analyses from both sources
        3. geus_borehole_pesticides_joined - Boreholes with pesticide detections

        Args:
            boreholes: List of parsed borehole dictionaries
            analyses: List of parsed analysis dictionaries (combined from both sources)

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
                dgunr VARCHAR,
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
                data_source VARCHAR,
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
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        CASE WHEN ? IS NOT NULL AND ? IS NOT NULL
                             THEN ST_Point(?, ?)
                             ELSE NULL END
                    )
                    """,
                    [
                        an["anlaegid"],
                        an.get("dgunr"),  # New: dgunr from mc_analyse
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
                        an.get("data_source", "unknown"),  # New: data source
                        x,
                        y,
                        x,
                        y,  # For CASE WHEN and ST_Point
                    ],
                )

        self.log.info(f"Created geus_pesticide_analyses table with {len(analyses):,} records")

        # Create joined table - boreholes with pesticide detections
        # Note: We join on both anlaegid AND dgunr to handle both data sources:
        # - jupiter_anlaegsanalyser uses anlaegid
        # - mc_analyse uses dgunr (more reliable for borehole matching)
        self.log.info("Creating joined table...")
        conn.execute("DROP TABLE IF EXISTS geus_borehole_pesticides_joined")
        conn.execute("""
            CREATE TABLE geus_borehole_pesticides_joined AS
            SELECT
                COALESCE(b.dgunr, a.dgunr) as dgunr,
                COALESCE(b.anlaegid, a.anlaegid) as anlaegid,
                a.stofnr,
                a.stof,
                a.stof_status,
                a.maengde,
                a.enhed,
                a.proevedato,
                COALESCE(b.kommunenavn, a.kommune) as kommunenavn,
                b.region_tekst,
                b.dybde,
                a.data_source,
                COALESCE(b.geometry, a.geometry) as geometry,
                CASE
                    WHEN a.maengde > 0 THEN true
                    ELSE false
                END as is_detection
            FROM geus_pesticide_analyses a
            LEFT JOIN geus_boreholes b ON (
                -- Join on dgunr if available (more reliable)
                (a.dgunr IS NOT NULL AND a.dgunr = b.dgunr)
                OR
                -- Fall back to anlaegid for facility analyses
                (a.dgunr IS NULL AND a.anlaegid = b.anlaegid)
            )
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
            "SELECT COUNT(DISTINCT dgunr) FROM geus_borehole_pesticides_joined WHERE dgunr IS NOT NULL"
        ).fetchone()[0]
        self.log.info(f"  - Unique boreholes with pesticide data: {unique_boreholes:,}")

        # Log breakdown by data source
        source_counts = conn.execute("""
            SELECT data_source, COUNT(*) as count
            FROM geus_borehole_pesticides_joined
            GROUP BY data_source
            ORDER BY count DESC
        """).fetchall()
        self.log.info("  - By data source:")
        for source, count in source_counts:
            self.log.info(f"      {source}: {count:,} analyses")

        # Log unique substances
        unique_substances = conn.execute(
            "SELECT COUNT(DISTINCT stofnr) FROM geus_borehole_pesticides_joined"
        ).fetchone()[0]
        self.log.info(f"  - Unique substances tracked: {unique_substances:,}")

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

    def _read_gml_chunks_from_gcs(self, chunk_paths: list[str]) -> list[str]:
        """
        Read GML data from GCS chunk files.

        Args:
            chunk_paths: List of relative paths to chunk parquet files.
                New format: bronze/{dataset}/{timestamp}/chunks/{layer}/chunk_0000.parquet

        Returns:
            List of GML strings from the chunks
        """
        gml_data = []
        for path in chunk_paths:
            try:
                # New path structure already includes full relative path:
                # bronze/{dataset}/{timestamp}/chunks/{layer}/chunk_0000.parquet
                gcs_path = f"gs://{self.config.bucket}/{path}"
                self.log.debug(f"Reading chunk from {gcs_path}")

                # Read the parquet file
                result = self.conn.execute(
                    f"SELECT payload FROM read_parquet('{gcs_path}')"
                ).fetchall()

                gml_data.extend(row[0] for row in result)

            except Exception as e:
                self.log.warning(f"Failed to read chunk {path}: {e}")
                continue

        return gml_data

    async def run(self, bronze_data: Any | None = None) -> dict[str, Any] | None:
        """
        Run the GEUS borehole pesticides silver layer processing pipeline.

        This method orchestrates the entire data processing workflow:
        1. Reads raw GML data from bronze layer (from GCS chunks or in-memory)
        2. Parses boreholes and analyses from GML
        3. Combines analyses from both sources (jupiter_anlaegsanalyser and mc_analyse)
        4. Creates joined dataset
        5. Saves to GCS

        Args:
            bronze_data: Optional data from bronze stage. Can be:
                        - Manifest dict with 'layers' containing GCS paths to chunks
                        - Legacy format with 'boreholes', 'analyses' lists of GML strings

        Returns:
            dict[str, Any]: Success information including dataset name and status
            None: If processing failed
        """
        self.log.info("Running GEUS Borehole Pesticides silver job")

        async with AsyncTimer("GEUS Borehole Pesticides silver job"):
            # Get data either from manifest (GCS chunks) or legacy in-memory format
            if bronze_data is not None:
                if not isinstance(bronze_data, dict):
                    self.log.error(f"Expected dict from bronze stage, got {type(bronze_data)}")
                    return None

                # Check if this is the new manifest format (has 'layers' key with paths)
                if "layers" in bronze_data:
                    self.log.info("Using bronze manifest format (reading from GCS chunks)")
                    layers = bronze_data["layers"]

                    # Read boreholes from GCS chunks
                    boreholes_paths = layers.get("boreholes", {}).get("saved_paths", [])
                    self.log.info(f"Reading {len(boreholes_paths)} borehole chunks from GCS...")
                    boreholes_gml = self._read_gml_chunks_from_gcs(boreholes_paths)

                    # Read analyses from GCS chunks
                    analyses_paths = layers.get("analyses", {}).get("saved_paths", [])
                    self.log.info(
                        f"Reading {len(analyses_paths)} facility analyses chunks from GCS..."
                    )
                    analyses_gml = self._read_gml_chunks_from_gcs(analyses_paths)

                    # Read pesticide analyses from GCS chunks
                    pesticide_paths = layers.get("pesticide_analyses", {}).get("saved_paths", [])
                    self.log.info(
                        f"Reading {len(pesticide_paths)} pesticide analyses chunks from GCS..."
                    )
                    pesticide_analyses_gml = self._read_gml_chunks_from_gcs(pesticide_paths)
                else:
                    # Legacy in-memory format (lists of GML strings)
                    self.log.info("Using legacy bronze data format (in-memory GML strings)")
                    boreholes_gml = bronze_data.get("boreholes", [])
                    analyses_gml = bronze_data.get("analyses", [])
                    pesticide_analyses_gml = bronze_data.get("pesticide_analyses", [])
            else:
                # Read from storage (fallback - read manifest from GCS)
                self.log.info("No bronze data passed - reading from GCS (independent silver run)")

                # Find the latest manifest in GCS
                # New path structure: bronze/{dataset}/{run_timestamp}/manifest.json
                manifest_pattern = (
                    f"gs://{self.config.bucket}/bronze/{self.config.dataset}/*/manifest.json"
                )
                self.log.info(f"Looking for manifest files matching: {manifest_pattern}")

                manifest_files = self.gcs_access.list_files(manifest_pattern)
                if not manifest_files:
                    self.log.error(
                        f"No manifest files found in GCS. Run bronze stage first. "
                        f"Pattern: {manifest_pattern}"
                    )
                    return None

                # Get the most recent manifest (sorted by path which includes timestamp)
                latest_manifest = sorted(manifest_files, reverse=True)[0]
                self.log.info(f"Reading manifest from {latest_manifest}")

                # Read and parse manifest (JSON format)
                try:
                    manifest = self.gcs_access.download_json(latest_manifest)
                    self.log.info(
                        f"Loaded manifest for run {manifest.get('run_timestamp', 'unknown')}"
                    )

                    # Read chunks from paths stored in manifest
                    layers = manifest.get("layers", {})

                    # Read boreholes chunks
                    boreholes_paths = layers.get("boreholes", {}).get("saved_paths", [])
                    self.log.info(
                        f"Reading {len(boreholes_paths)} borehole chunks from manifest..."
                    )
                    boreholes_gml = []
                    for path in boreholes_paths:
                        gcs_path = f"gs://{self.config.bucket}/{path}"
                        result = self.conn.execute(
                            f"SELECT payload FROM read_parquet('{gcs_path}')"
                        ).fetchall()
                        boreholes_gml.extend(row[0] for row in result)

                    # Read analyses chunks
                    analyses_paths = layers.get("analyses", {}).get("saved_paths", [])
                    self.log.info(
                        f"Reading {len(analyses_paths)} facility analyses chunks from manifest..."
                    )
                    analyses_gml = []
                    for path in analyses_paths:
                        gcs_path = f"gs://{self.config.bucket}/{path}"
                        result = self.conn.execute(
                            f"SELECT payload FROM read_parquet('{gcs_path}')"
                        ).fetchall()
                        analyses_gml.extend(row[0] for row in result)

                    # Read pesticide analyses chunks
                    pesticide_paths = layers.get("pesticide_analyses", {}).get("saved_paths", [])
                    self.log.info(
                        f"Reading {len(pesticide_paths)} pesticide analyses chunks from manifest..."
                    )
                    pesticide_analyses_gml = []
                    for path in pesticide_paths:
                        gcs_path = f"gs://{self.config.bucket}/{path}"
                        result = self.conn.execute(
                            f"SELECT payload FROM read_parquet('{gcs_path}')"
                        ).fetchall()
                        pesticide_analyses_gml.extend(row[0] for row in result)

                except Exception as e:
                    self.log.error(f"Failed to read from GCS: {e}")
                    import traceback

                    self.log.error(traceback.format_exc())
                    return None

            self.log.info(
                f"Processing {len(boreholes_gml)} borehole GML responses, "
                f"{len(analyses_gml)} facility analysis GML responses (jupiter_anlaegsanalyser), "
                f"and {len(pesticide_analyses_gml)} pesticide analysis GML responses (mc_analyse)"
            )

            # Parse GML data
            boreholes = self._parse_boreholes_gml(boreholes_gml)
            if not boreholes:
                self.log.error("No boreholes parsed from GML data")
                return None

            # Parse facility analyses (jupiter_anlaegsanalyser - limited to tracked pesticides)
            facility_analyses = self._parse_analyses_gml(analyses_gml)
            self.log.info(
                f"Parsed {len(facility_analyses):,} analyses from jupiter_anlaegsanalyser"
            )

            # Parse pesticide analyses (mc_analyse - comprehensive pesticide data)
            mc_analyse_data = self._parse_pesticide_analyses_gml(pesticide_analyses_gml)
            self.log.info(
                f"Parsed {len(mc_analyse_data):,} analyses from mc_analyse (stofgruppe=50)"
            )

            # Combine analyses from both sources
            # mc_analyse data is prioritized since it has comprehensive pesticide coverage
            all_analyses = facility_analyses + mc_analyse_data
            self.log.info(
                f"Combined total: {len(all_analyses):,} pesticide analyses from both sources"
            )

            if not all_analyses:
                self.log.warning(
                    "No pesticide analyses found from either source - this may indicate a data issue"
                )

            # Create DuckDB tables
            boreholes_table, analyses_table, joined_table = self._create_duckdb_tables(
                boreholes, all_analyses
            )

            # Validate geometries
            self._validate_geometries(boreholes_table)
            if all_analyses:
                self._validate_geometries(joined_table)

            # Save to GCS with pipeline metadata for data tracing
            self.log.info("Saving data to GCS...")

            # Save boreholes (all boreholes, not just those with pesticides)
            self._save_data_with_metadata(
                data=boreholes_table,
                dataset="geus_boreholes",
                source_key="geus_borehole_pesticides",
                bucket=self.config.bucket,
                stage="silver",
            )

            # Save pesticide analyses if we have any
            if all_analyses:
                self._save_data_with_metadata(
                    data=analyses_table,
                    dataset="geus_pesticide_analyses",
                    source_key="geus_borehole_pesticides",
                    bucket=self.config.bucket,
                    stage="silver",
                )
                self._save_data_with_metadata(
                    data=joined_table,
                    dataset="geus_borehole_pesticides",
                    source_key="geus_borehole_pesticides",
                    bucket=self.config.bucket,
                    stage="silver",
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
                    "geus_pesticide_analyses" if all_analyses else None,
                    "geus_borehole_pesticides" if all_analyses else None,
                ],
                "statistics": {
                    "total_boreholes": len(boreholes),
                    "total_pesticide_analyses": len(all_analyses),
                    "facility_analyses_count": len(facility_analyses),
                    "mc_analyse_count": len(mc_analyse_data),
                },
            }

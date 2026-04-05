"""
Silver layer processing for GEUS Dataverse Pesticides and PFAS data.

This module handles the transformation of raw .rds data from the bronze layer
into clean, structured parquet format. It reads the R Data Serialization files,
converts them to pandas DataFrames, standardizes column names, creates geometries,
and saves as parquet to cloud storage.

Data source:
- Input: AM_pest.rds (Annual Mean pesticide concentrations) - 633 substances, 4.2M+ analyses
- Input: AM_pfas.rds (Annual Mean PFAS concentrations) - 26 substances, 397k analyses
- Input: clean_dataset.rds (Sample-level clean dataset, Deliverable 2) - ~9.4M analyses
- Output: Parquet files with standardized schema and Point geometries

Key columns in pesticide output (AM):
- year: Sample year (PROEVEAAR)
- stofnr: Substance code
- stof_tekst: Substance name in Danish
- maengde: Annual mean concentration (µg/L)
- dgu_nr: Borehole identifier
- groundwater_body: Grundvandsforekomst ID
- x, y: Coordinates in EPSG:25832
- geometry: Point geometry

Key columns in clean dataset output (sample-level):
- year, sample_date, sample_id: Temporal and sample identification
- stofnr, stof_tekst: Substance identification
- concentration: Sample-level concentration (µg/L, with ½ LOQ substitution)
- maengde: Alias for concentration (backward compatibility)
- chemical_monitoring, investigative_monitoring, surveillance_monitoring: Monitoring flags
- dgu_nr, groundwater_body, data_type: Borehole and hydrogeological context
- x, y, geometry: Coordinates in EPSG:25832

Key columns in PFAS output:
- year: Sample year (PROEVEAAR)
- substance_name: PFAS substance name (KORT_NAVN)
- maengde: Annual mean concentration (µg/L)
- dgu_nr: Borehole identifier
- groundwater_body: Grundvandsforekomst ID
- x, y: Coordinates in EPSG:25832
- geometry: Point geometry
"""

import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

from unified_pipeline.common.base import BaseJobConfig, BaseSource, SilverJobInterface
from unified_pipeline.util.timing import AsyncTimer, timed


class GEUSDataversePesticidesSilverConfig(BaseJobConfig):
    """
    Configuration for the GEUS Dataverse Pesticides Silver processing.

    Attributes:
        dataset: Name of the dataset in storage
        bucket: storage bucket name for data storage
        source_crs: Source coordinate reference system (already in EPSG:25832)
    """

    dataset: str = "geus_dataverse_pesticides"
    bucket: str = "landbruget-data"
    source_crs: str = "EPSG:25832"  # Data uses UTM32 EUREF89 coordinates


class GEUSDataversePesticidesSilver(
    BaseSource[GEUSDataversePesticidesSilverConfig], SilverJobInterface
):
    """
    Silver layer processor for GEUS Dataverse pesticides data.

    This class handles the conversion of raw .rds files from the bronze layer
    into clean parquet format with standardized schema.

    Processing flow:
    1. Read .rds file from cloud storage (via manifest path or direct)
    2. Convert to DataFrame using pyreadr
    3. Standardize column names
    4. Create Point geometries from coordinates
    5. Save as parquet to cloud storage silver layer
    """

    def __init__(self, config: GEUSDataversePesticidesSilverConfig):
        """
        Initialize the GEUSDataversePesticidesSilver processor.

        Args:
            config: Configuration object
        """
        super().__init__(config)
        self._setup_duckdb_spatial()

    def _setup_duckdb_spatial(self):
        """Setup DuckDB connection with spatial extensions."""
        try:
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
            self.log.info("✅ DuckDB-spatial initialized for GEUS Dataverse processing")
        except Exception as e:
            self.log.error(f"Failed to setup DuckDB spatial extension: {e}")
            raise

    def _download_rds_from_storage(self, rds_path: str) -> Path:
        """
        Download .rds file from storage to a temporary local file.

        Args:
            rds_path: Relative path to .rds file in storage

        Returns:
            Path to the downloaded temporary file
        """
        storage_path = f"{self.config.bucket}/{rds_path}"
        self.log.info(f"Downloading .rds file from {storage_path}")

        # Create temporary file
        tmp_fd, tmp_name = tempfile.mkstemp(suffix=".rds")
        os.close(tmp_fd)
        tmp_path = Path(tmp_name)

        # Download using s3fs (R2) directly (binary read)
        with (
            self.storage.fs.open(storage_path, "rb") as cloud_file,
            tmp_path.open("wb") as local_file,
        ):
            local_file.write(cloud_file.read())

        self.log.info(f"Downloaded .rds to {tmp_path}")
        return tmp_path

    @timed(name="Converting RDS to DataFrame")
    def _convert_rds_to_dataframe(self, rds_path: Path) -> Any:
        """
        Convert .rds file to pandas DataFrame using pyreadr.

        Args:
            rds_path: Path to the local .rds file

        Returns:
            pandas DataFrame with the pesticide data
        """
        try:
            import pyreadr
        except ImportError as e:
            self.log.error("pyreadr not installed. Run: pip install pyreadr")
            raise ImportError("pyreadr required for reading .rds files") from e

        self.log.info(f"Reading .rds file: {rds_path}")

        # Read the .rds file
        result = pyreadr.read_r(str(rds_path))

        # pyreadr returns a dict with None key for single objects
        df = next(iter(result.values()))

        self.log.info(f"Loaded DataFrame with {len(df):,} rows and {len(df.columns)} columns")
        self.log.info(f"Columns: {list(df.columns)}")

        return df

    @timed(name="Transforming pesticide data")
    def _transform_data(self, df: Any) -> str:
        """
        Transform the DataFrame into DuckDB table with standardized schema.

        Args:
            df: pandas DataFrame from .rds file

        Returns:
            Name of the DuckDB table with transformed data
        """
        # Register the DataFrame with DuckDB
        self.conn.register("raw_pesticides", df)

        # Get column names for logging
        columns = list(df.columns)

        self.log.info(f"Available columns: {columns}")

        # Build the column mapping based on what's available
        # Full schema from AM_pest.rds (34 columns):
        # BORID_INDTAGSID, PROEVEAAR, GROUP, KORT_NAVN, STOFKODE, STOFNAVN, AM,
        # BORID, INDTAGSID, DGUNR, INDTAGSNR, BORINGSDYBDE, XUTM32EUREF89, YUTM32EUREF89,
        # TERRAENKOTE, INDTAG_TOP, INDTAG_BUND, DATATYPE, REDOXVANDTYPE, LOOP,
        # euProgrammeCode, euMonitoringSiteCode, chemicalMonitoring, investigativeMonitoring,
        # surveillanceMonitoring, wellSpring, Depth, GVMAG, GVF, Grundvandsforekomst,
        # quantitativeMonitoring, CASNUMBER, EUNAME, ENHED

        # Create the transformed table with standardized names
        self.conn.execute("""
            CREATE OR REPLACE TABLE pesticides_transformed AS
            SELECT
                -- Temporal
                CAST(PROEVEAAR AS INTEGER) as year,

                -- Substance identification
                CAST(STOFKODE AS INTEGER) as stofnr,
                STOFNAVN as stof_tekst,
                CASNUMBER as cas_number,

                -- Measurement
                CAST(AM AS DOUBLE) as maengde,
                ENHED as enhed,

                -- Location coordinates
                CAST(XUTM32EUREF89 AS DOUBLE) as x,
                CAST(YUTM32EUREF89 AS DOUBLE) as y,

                -- Borehole identifiers
                DGUNR as dgu_nr,
                CAST(BORID AS INTEGER) as borid,
                INDTAGSID as indtagsid,

                -- Borehole physical properties
                CAST(BORINGSDYBDE AS DOUBLE) as borehole_depth_m,
                CAST(TERRAENKOTE AS DOUBLE) as terrain_elevation_m,
                CAST(INDTAG_TOP AS DOUBLE) as intake_top_m,
                CAST(INDTAG_BUND AS DOUBLE) as intake_bottom_m,

                -- Hydrogeological context
                Grundvandsforekomst as groundwater_body,
                REDOXVANDTYPE as redox_water_type,
                Depth as depth_category,

                -- Monitoring metadata
                DATATYPE as data_type,
                euProgrammeCode as eu_programme_code,
                euMonitoringSiteCode as eu_monitoring_site_code,

                -- Create geometry from coordinates
                ST_Point(
                    CAST(XUTM32EUREF89 AS DOUBLE),
                    CAST(YUTM32EUREF89 AS DOUBLE)
                ) as geometry

            FROM raw_pesticides
            WHERE XUTM32EUREF89 IS NOT NULL
              AND YUTM32EUREF89 IS NOT NULL
              AND PROEVEAAR >= 1980  -- Filter out clearly erroneous years (1899 etc)
              AND PROEVEAAR <= 2030
        """)

        # Get statistics
        stats = self.conn.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT stofnr) as unique_substances,
                COUNT(DISTINCT dgu_nr) as unique_boreholes,
                MIN(year) as min_year,
                MAX(year) as max_year,
                COUNT(DISTINCT year) as years_covered
            FROM pesticides_transformed
        """).fetchone()

        self.log.info(
            f"Transformed data: {stats[0]:,} records, "
            f"{stats[1]} substances, {stats[2]:,} boreholes, "
            f"years {stats[3]}-{stats[4]} ({stats[5]} years)"
        )

        # Log top substances by count
        top_substances = self.conn.execute("""
            SELECT stof_tekst, COUNT(*) as count
            FROM pesticides_transformed
            GROUP BY stof_tekst
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        self.log.info("Top 10 substances by analysis count:")
        for substance, count in top_substances:
            self.log.info(f"  - {substance}: {count:,}")

        # Unregister the pandas DataFrame
        self.conn.unregister("raw_pesticides")

        return "pesticides_transformed"

    @timed(name="Transforming PFAS data")
    def _transform_pfas_data(self, df: Any) -> str:
        """
        Transform the PFAS DataFrame into DuckDB table with standardized schema.

        PFAS data has a different schema than pesticide data:
        - Uses KORT_NAVN for substance name instead of STOFNAVN
        - No STOFKODE or CASNUMBER columns
        - GROUP column contains 'pfas'

        Args:
            df: pandas DataFrame from AM_pfas.rds file

        Returns:
            Name of the DuckDB table with transformed data
        """
        # Register the DataFrame with DuckDB
        self.conn.register("raw_pfas", df)

        # Get column names
        columns = list(df.columns)
        self.log.info(f"PFAS available columns: {columns}")

        # Create the transformed table with standardized names
        # PFAS schema: 30 columns including KORT_NAVN for substance name
        self.conn.execute("""
            CREATE OR REPLACE TABLE pfas_transformed AS
            SELECT
                -- Temporal
                CAST(PROEVEAAR AS INTEGER) as year,

                -- Substance identification (PFAS uses KORT_NAVN, no STOFKODE)
                KORT_NAVN as substance_name,
                "GROUP" as substance_group,

                -- Measurement
                CAST(AM AS DOUBLE) as maengde,
                CAST(ENHED AS VARCHAR) as enhed,

                -- Location coordinates
                CAST(XUTM32EUREF89 AS DOUBLE) as x,
                CAST(YUTM32EUREF89 AS DOUBLE) as y,

                -- Borehole identifiers
                DGUNR as dgu_nr,
                CAST(BORID AS INTEGER) as borid,
                INDTAGSID as indtagsid,

                -- Borehole physical properties
                CAST(BORINGSDYBDE AS DOUBLE) as borehole_depth_m,
                CAST(TERRAENKOTE AS DOUBLE) as terrain_elevation_m,
                CAST(INDTAG_TOP AS DOUBLE) as intake_top_m,
                CAST(INDTAG_BUND AS DOUBLE) as intake_bottom_m,

                -- Hydrogeological context
                Grundvandsforekomst as groundwater_body,
                REDOXVANDTYPE as redox_water_type,
                Depth as depth_category,

                -- Monitoring metadata
                DATATYPE as data_type,
                euProgrammeCode as eu_programme_code,
                euMonitoringSiteCode as eu_monitoring_site_code,

                -- Create geometry from coordinates
                ST_Point(
                    CAST(XUTM32EUREF89 AS DOUBLE),
                    CAST(YUTM32EUREF89 AS DOUBLE)
                ) as geometry

            FROM raw_pfas
            WHERE XUTM32EUREF89 IS NOT NULL
              AND YUTM32EUREF89 IS NOT NULL
              AND PROEVEAAR >= 2000  -- PFAS monitoring started around 2010
              AND PROEVEAAR <= 2030
        """)

        # Get statistics
        stats = self.conn.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT substance_name) as unique_substances,
                COUNT(DISTINCT dgu_nr) as unique_boreholes,
                MIN(year) as min_year,
                MAX(year) as max_year,
                COUNT(DISTINCT year) as years_covered
            FROM pfas_transformed
        """).fetchone()

        self.log.info(
            f"Transformed PFAS data: {stats[0]:,} records, "
            f"{stats[1]} substances, {stats[2]:,} boreholes, "
            f"years {stats[3]}-{stats[4]} ({stats[5]} years)"
        )

        # Log top substances by count
        top_substances = self.conn.execute("""
            SELECT substance_name, COUNT(*) as count
            FROM pfas_transformed
            GROUP BY substance_name
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        self.log.info("Top 10 PFAS substances by analysis count:")
        for substance, count in top_substances:
            self.log.info(f"  - {substance}: {count:,}")

        # Unregister the pandas DataFrame
        self.conn.unregister("raw_pfas")

        return "pfas_transformed"

    def _build_clean_select_sql(self, group_filter: str | None = None) -> str:
        """
        Build the SELECT SQL for clean dataset transform.

        Args:
            group_filter: Optional GROUP value to filter on (e.g. 'pest').
                         If None, includes all groups with a parameter_group column.

        Returns:
            SQL string for CREATE TABLE
        """
        where_clauses = [
            "XUTM32EUREF89 IS NOT NULL",
            "YUTM32EUREF89 IS NOT NULL",
            "PROEVEAAR >= 1980",
            "PROEVEAAR <= 2030",
        ]
        extra_col = ""
        if group_filter:
            where_clauses.append(f"\"GROUP\" = '{group_filter}'")
        else:
            extra_col = '"GROUP" as parameter_group,'

        where_sql = " AND ".join(where_clauses)

        return f"""
            SELECT
                -- Temporal
                CAST(PROEVEAAR AS INTEGER) as year,
                CAST(PROEVEDATO AS VARCHAR) as sample_date,
                CAST(PROEVEID AS BIGINT) as sample_id,

                -- Substance identification
                CAST(STOFKODE AS INTEGER) as stofnr,
                STOFNAVN as stof_tekst,
                KORT_NAVN as kort_navn,
                CASNUMBER as cas_number,

                -- Measurement (sample-level, with LOQ substitution)
                CAST(value AS DOUBLE) as concentration,
                CAST(value AS DOUBLE) as maengde,  -- backward compat alias
                ENHED as enhed,
                {extra_col}

                -- Location coordinates
                CAST(XUTM32EUREF89 AS DOUBLE) as x,
                CAST(YUTM32EUREF89 AS DOUBLE) as y,

                -- Borehole identifiers
                DGUNR as dgu_nr,
                CAST(BORID AS INTEGER) as borid,
                INDTAGSID as indtagsid,
                BORID_INDTAGSID as borid_indtagsid,

                -- Borehole physical properties
                CAST(BORINGSDYBDE AS DOUBLE) as borehole_depth_m,
                CAST(TERRAENKOTE AS DOUBLE) as terrain_elevation_m,
                CAST(INDTAG_TOP AS DOUBLE) as intake_top_m,
                CAST(INDTAG_BUND AS DOUBLE) as intake_bottom_m,

                -- Hydrogeological context
                Grundvandsforekomst as groundwater_body,
                REDOXVANDTYPE as redox_water_type,
                Depth as depth_category,

                -- Monitoring metadata (NOT dropped like in AM transform)
                DATATYPE as data_type,
                euProgrammeCode as eu_programme_code,
                euMonitoringSiteCode as eu_monitoring_site_code,
                chemicalMonitoring as chemical_monitoring,
                investigativeMonitoring as investigative_monitoring,
                surveillanceMonitoring as surveillance_monitoring,

                -- Create geometry from coordinates
                ST_Point(
                    CAST(XUTM32EUREF89 AS DOUBLE),
                    CAST(YUTM32EUREF89 AS DOUBLE)
                ) as geometry

            FROM raw_clean
            WHERE {where_sql}
        """

    @timed(name="Transforming clean dataset (sample-level)")
    def _transform_clean_data(self, df: Any) -> tuple[str, str]:
        """
        Transform the clean dataset (Deliverable 2) into DuckDB tables.

        Produces two tables:
        1. Pesticide-only (GROUP = 'pest') for correlation analysis
        2. All parameter groups (pest, pfas, nitrate, mfs, etc.) for comprehensive output

        Args:
            df: pandas DataFrame from clean_dataset.rds file

        Returns:
            Tuple of (pest_table_name, all_groups_table_name)
        """
        self.conn.register("raw_clean", df)

        columns = list(df.columns)
        self.log.info(f"Clean dataset columns: {columns}")
        self.log.info(f"Clean dataset total rows: {len(df):,}")

        # Log parameter group distribution before filtering
        self.conn.execute("""
            CREATE OR REPLACE TABLE _clean_groups AS
            SELECT "GROUP" as grp, COUNT(*) as cnt
            FROM raw_clean
            GROUP BY "GROUP"
            ORDER BY cnt DESC
        """)
        groups = self.conn.execute("SELECT * FROM _clean_groups").fetchall()
        self.log.info("Parameter groups in clean dataset:")
        for grp, cnt in groups:
            self.log.info(f"  - {grp}: {cnt:,}")

        # 1. Pesticide-only table
        pest_sql = self._build_clean_select_sql(group_filter="pest")
        self.conn.execute(f"CREATE OR REPLACE TABLE clean_pest_transformed AS {pest_sql}")

        # 2. All parameter groups table
        all_sql = self._build_clean_select_sql(group_filter=None)
        self.conn.execute(f"CREATE OR REPLACE TABLE clean_all_transformed AS {all_sql}")

        # Get pest statistics
        stats = self.conn.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT stofnr) as unique_substances,
                COUNT(DISTINCT dgu_nr) as unique_boreholes,
                COUNT(DISTINCT sample_id) as unique_samples,
                MIN(year) as min_year,
                MAX(year) as max_year,
                COUNT(DISTINCT year) as years_covered
            FROM clean_pest_transformed
        """).fetchone()

        self.log.info(
            f"Clean pesticide data: {stats[0]:,} records, "
            f"{stats[1]} substances, {stats[2]:,} boreholes, "
            f"{stats[3]:,} samples, years {stats[4]}-{stats[5]} ({stats[6]} years)"
        )

        # Get all-groups statistics
        all_stats = self.conn.execute("""
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT parameter_group) as groups,
                COUNT(DISTINCT dgu_nr) as unique_boreholes,
                MIN(year) as min_year,
                MAX(year) as max_year
            FROM clean_all_transformed
        """).fetchone()

        self.log.info(
            f"Clean all-groups data: {all_stats[0]:,} records, "
            f"{all_stats[1]} parameter groups, {all_stats[2]:,} boreholes, "
            f"years {all_stats[3]}-{all_stats[4]}"
        )

        # Log concentration distribution to help determine detection threshold
        dist = self.conn.execute("""
            SELECT
                COUNT(*) as total,
                COUNT(CASE WHEN concentration = 0 THEN 1 END) as zero,
                COUNT(CASE WHEN concentration > 0 AND concentration <= 0.005 THEN 1 END) as lte_005,
                COUNT(CASE WHEN concentration > 0.005 AND concentration <= 0.01 THEN 1 END) as lte_01,
                COUNT(CASE WHEN concentration > 0.01 AND concentration <= 0.015 THEN 1 END) as lte_015,
                COUNT(CASE WHEN concentration > 0.015 AND concentration <= 0.1 THEN 1 END) as lte_01ug,
                COUNT(CASE WHEN concentration > 0.1 THEN 1 END) as above_01ug
            FROM clean_pest_transformed
        """).fetchone()

        self.log.info(
            f"Concentration distribution: total={dist[0]:,}, "
            f"zero={dist[1]:,}, (0,0.005]={dist[2]:,}, (0.005,0.01]={dist[3]:,}, "
            f"(0.01,0.015]={dist[4]:,}, (0.015,0.1]={dist[5]:,}, >0.1={dist[6]:,}"
        )

        # Log top substances
        top_substances = self.conn.execute("""
            SELECT stof_tekst, COUNT(*) as count
            FROM clean_pest_transformed
            GROUP BY stof_tekst
            ORDER BY count DESC
            LIMIT 10
        """).fetchall()

        self.log.info("Top 10 substances by analysis count (clean dataset):")
        for substance, count in top_substances:
            self.log.info(f"  - {substance}: {count:,}")

        # Clean up
        self.conn.execute("DROP TABLE IF EXISTS _clean_groups")
        self.conn.unregister("raw_clean")

        return "clean_pest_transformed", "clean_all_transformed"

    @timed(name="Validating geometries")
    def _validate_geometries(self, table_name: str) -> None:
        """
        Validate geometries are within Denmark bounds (EPSG:25832).

        Args:
            table_name: Name of table to validate
        """
        # Denmark bounds in EPSG:25832 (UTM zone 32N)
        invalid_count = self.conn.execute(f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE geometry IS NOT NULL
            AND NOT ST_Within(
                geometry,
                ST_MakeEnvelope(400000, 6000000, 900000, 6500000)
            )
        """).fetchone()[0]

        total_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

        if invalid_count > 0:
            self.log.warning(
                f"{invalid_count:,} of {total_count:,} geometries outside Denmark bounds"
            )
        else:
            self.log.info(f"All {total_count:,} geometries validated within Denmark bounds")

    async def run(self, bronze_data: Any | None = None) -> dict[str, Any] | None:
        """
        Run the silver layer processing pipeline.

        Processes both pesticide and PFAS data from the bronze layer.

        Args:
            bronze_data: Optional manifest dict from bronze stage with 'rds_path' and
                        'pfas_rds_path' keys. If None, will look for latest manifest in cloud storage.

        Returns:
            Success information including dataset name and statistics,
            or None if processing fails
        """
        async with AsyncTimer("Running GEUS Dataverse Pesticides and PFAS silver job"):
            self.log.info("Running GEUS Dataverse Pesticides and PFAS silver job")

            pest_rds_path = None
            pfas_rds_path = None
            clean_rds_path = None
            tmp_pest_path = None
            tmp_pfas_path = None
            tmp_clean_path = None

            try:
                # Get the .rds file paths from bronze manifest
                if bronze_data is not None:
                    if not isinstance(bronze_data, dict):
                        self.log.error(f"Expected dict from bronze stage, got {type(bronze_data)}")
                        return None

                    # Support both old (rds_path) and new (pest_rds_path) manifest formats
                    pest_rds_path = bronze_data.get("pest_rds_path") or bronze_data.get("rds_path")
                    pfas_rds_path = bronze_data.get("pfas_rds_path")
                    clean_rds_path = bronze_data.get("clean_rds_path")

                    if not pest_rds_path:
                        self.log.error("Bronze manifest missing 'rds_path' or 'pest_rds_path'")
                        return None

                    self.log.info(
                        f"Using bronze manifest: pest={pest_rds_path}, pfas={pfas_rds_path}, "
                        f"clean={clean_rds_path}"
                    )
                else:
                    # Find latest manifest in cloud storage
                    self.log.info("No bronze data passed - reading from cloud storage")
                    manifest_pattern = (
                        f"{self.config.bucket}/bronze/{self.config.dataset}/*/manifest.json"
                    )
                    manifest_files = self.storage.list_files(manifest_pattern)

                    if not manifest_files:
                        self.log.error(
                            f"No manifest files found. Run bronze stage first. "
                            f"Pattern: {manifest_pattern}"
                        )
                        return None

                    # Get most recent manifest
                    latest_manifest = sorted(manifest_files, reverse=True)[0]
                    self.log.info(f"Reading manifest from {latest_manifest}")

                    manifest = self.storage.download_json(latest_manifest)
                    pest_rds_path = manifest.get("pest_rds_path") or manifest.get("rds_path")
                    pfas_rds_path = manifest.get("pfas_rds_path")
                    clean_rds_path = manifest.get("clean_rds_path")

                    if not pest_rds_path:
                        self.log.error("Manifest missing 'rds_path' or 'pest_rds_path'")
                        return None

                # === Process Pesticide Data ===
                self.log.info("=== Processing Pesticide Data ===")
                tmp_pest_path = self._download_rds_from_storage(pest_rds_path)
                pest_df = self._convert_rds_to_dataframe(tmp_pest_path)
                pest_table = self._transform_data(pest_df)
                self._validate_geometries(pest_table)

                # Get pesticide statistics
                pest_stats = self.conn.execute(f"""
                    SELECT
                        COUNT(*) as total_records,
                        COUNT(DISTINCT stofnr) as unique_substances,
                        COUNT(DISTINCT dgu_nr) as unique_boreholes,
                        MIN(year) as min_year,
                        MAX(year) as max_year
                    FROM {pest_table}
                """).fetchone()

                # Save pesticide data to cloud storage silver layer
                self._save_data_with_metadata(
                    data=pest_table,
                    dataset=self.config.dataset,
                    source_key="geus_dataverse_pesticides",
                    bucket=self.config.bucket,
                    stage="silver",
                )

                # === Process PFAS Data (if available) ===
                pfas_stats = None
                if pfas_rds_path:
                    self.log.info("=== Processing PFAS Data ===")
                    tmp_pfas_path = self._download_rds_from_storage(pfas_rds_path)
                    pfas_df = self._convert_rds_to_dataframe(tmp_pfas_path)
                    pfas_table = self._transform_pfas_data(pfas_df)
                    self._validate_geometries(pfas_table)

                    # Get PFAS statistics
                    pfas_stats = self.conn.execute(f"""
                        SELECT
                            COUNT(*) as total_records,
                            COUNT(DISTINCT substance_name) as unique_substances,
                            COUNT(DISTINCT dgu_nr) as unique_boreholes,
                            MIN(year) as min_year,
                            MAX(year) as max_year
                        FROM {pfas_table}
                    """).fetchone()

                    # Save PFAS data to cloud storage silver layer (as subdataset)
                    self._save_data_with_metadata(
                        data=pfas_table,
                        dataset=f"{self.config.dataset}_pfas",
                        source_key="geus_dataverse_pfas",
                        bucket=self.config.bucket,
                        stage="silver",
                    )
                else:
                    self.log.warning("No PFAS data path in manifest - skipping PFAS processing")

                # === Process Clean Dataset (sample-level, Deliverable 2) ===
                clean_stats = None
                clean_all_stats = None
                if clean_rds_path:
                    self.log.info("=== Processing Clean Dataset (sample-level) ===")
                    tmp_clean_path = self._download_rds_from_storage(clean_rds_path)
                    clean_df = self._convert_rds_to_dataframe(tmp_clean_path)
                    clean_pest_table, clean_all_table = self._transform_clean_data(clean_df)
                    self._validate_geometries(clean_pest_table)
                    self._validate_geometries(clean_all_table)

                    # Get clean pesticide statistics
                    clean_stats = self.conn.execute(f"""
                        SELECT
                            COUNT(*) as total_records,
                            COUNT(DISTINCT stofnr) as unique_substances,
                            COUNT(DISTINCT dgu_nr) as unique_boreholes,
                            MIN(year) as min_year,
                            MAX(year) as max_year
                        FROM {clean_pest_table}
                    """).fetchone()

                    # Get clean all-groups statistics
                    clean_all_stats = self.conn.execute(f"""
                        SELECT
                            COUNT(*) as total_records,
                            COUNT(DISTINCT parameter_group) as unique_groups,
                            COUNT(DISTINCT dgu_nr) as unique_boreholes,
                            MIN(year) as min_year,
                            MAX(year) as max_year
                        FROM {clean_all_table}
                    """).fetchone()

                    # Save clean pesticide data to silver layer
                    self._save_data_with_metadata(
                        data=clean_pest_table,
                        dataset="geus_clean_pesticides",
                        source_key="geus_clean_pesticides",
                        bucket=self.config.bucket,
                        stage="silver",
                    )

                    # Save all-groups data (pest + pfas + nitrate + mfs + etc.)
                    self._save_data_with_metadata(
                        data=clean_all_table,
                        dataset="geus_clean_all",
                        source_key="geus_clean_all",
                        bucket=self.config.bucket,
                        stage="silver",
                    )
                else:
                    self.log.warning(
                        "No clean dataset path in manifest - skipping clean dataset processing"
                    )

                self.log.info("GEUS Dataverse silver job completed successfully")

                # Build result with statistics for both datasets
                result = {
                    "dataset": self.config.dataset,
                    "processed_at": datetime.now().isoformat(),
                    "status": "completed",
                    "statistics": {
                        "pesticides": {
                            "total_records": pest_stats[0],
                            "unique_substances": pest_stats[1],
                            "unique_boreholes": pest_stats[2],
                            "year_range": f"{pest_stats[3]}-{pest_stats[4]}",
                        },
                    },
                    # Backwards compatible fields
                    "total_records": pest_stats[0],
                    "unique_substances": pest_stats[1],
                    "unique_boreholes": pest_stats[2],
                    "year_range": f"{pest_stats[3]}-{pest_stats[4]}",
                }

                if pfas_stats:
                    result["statistics"]["pfas"] = {
                        "total_records": pfas_stats[0],
                        "unique_substances": pfas_stats[1],
                        "unique_boreholes": pfas_stats[2],
                        "year_range": f"{pfas_stats[3]}-{pfas_stats[4]}",
                    }

                if clean_stats:
                    result["statistics"]["clean_pesticides"] = {
                        "total_records": clean_stats[0],
                        "unique_substances": clean_stats[1],
                        "unique_boreholes": clean_stats[2],
                        "year_range": f"{clean_stats[3]}-{clean_stats[4]}",
                    }

                if clean_all_stats:
                    result["statistics"]["clean_all"] = {
                        "total_records": clean_all_stats[0],
                        "unique_groups": clean_all_stats[1],
                        "unique_boreholes": clean_all_stats[2],
                        "year_range": f"{clean_all_stats[3]}-{clean_all_stats[4]}",
                    }

                return result

            except Exception as e:
                self.log.error(f"Error in silver processing: {e}")
                import traceback

                self.log.error(traceback.format_exc())
                return None

            finally:
                # Clean up temporary files
                if tmp_pest_path and tmp_pest_path.exists():
                    tmp_pest_path.unlink(missing_ok=True)
                if tmp_pfas_path and tmp_pfas_path.exists():
                    tmp_pfas_path.unlink(missing_ok=True)
                if tmp_clean_path and tmp_clean_path.exists():
                    tmp_clean_path.unlink(missing_ok=True)

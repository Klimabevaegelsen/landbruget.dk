#!/usr/bin/env python3
"""
Danish Statistics API Pipeline - Silver Layer

This pipeline processes the raw bronze layer data from Danmarks Statistik API
and transforms it into clean, harmonized silver layer data following the
medallion architecture.

Silver layer principles:
- Clean and harmonize raw data
- Apply consistent naming conventions
- Proper data types and null handling
- Store in Parquet format for efficient processing
- No transformation dependencies on other data sources
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import ibis
from ibis import _

# Add parent directories to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))
from common.storage_interface import GCSStorage, LocalStorage, StorageInterface

# Configure ibis backend
ibis.options.interactive = True
con = ibis.duckdb.connect()


def setup_logging(level: str) -> None:
    """Configure logging with appropriate format and level"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="Process bronze layer DST data to silver layer"
    )
    parser.add_argument(
        "--bronze-dir", default="./bronze", help="Bronze layer input directory"
    )
    parser.add_argument(
        "--silver-dir", default="./silver", help="Silver layer output directory"
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=["HST77", "GARTN1", "FRO", "HALM1"],
        help="Tables to process (default: all implemented tables)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force reprocessing even if silver data exists",
    )

    return parser.parse_args()


def get_storage_interface() -> StorageInterface:
    """Get the appropriate storage interface based on environment"""
    gcs_bucket = os.getenv("GCS_BUCKET")
    environment = os.getenv("ENVIRONMENT", "dev")

    if environment == "prod" and gcs_bucket:
        logging.info(f"Using GCS storage with bucket: {gcs_bucket}")
        return GCSStorage(gcs_bucket)
    else:
        base_dir = os.getenv("LOCAL_STORAGE_DIR", "bronze/dst")
        logging.info(f"Using local storage with base directory: {base_dir}")
        return LocalStorage(base_dir)


def find_latest_bronze_data(
    storage: StorageInterface, table_id: str
) -> Optional[Dict[str, Any]]:
    """Find and load the most recent bronze data for a table"""

    if isinstance(storage, LocalStorage):
        bronze_base = Path(storage.base_dir)
        bronze_files = list(bronze_base.glob(f"*/{table_id}_data.json"))

        if not bronze_files:
            return None

        # Sort by directory name (date) and get the most recent
        bronze_files.sort(reverse=True)
        latest_file = bronze_files[0]

        try:
            with open(latest_file) as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load bronze data from {latest_file}: {e}")
            return None

    else:
        # For GCS storage, we'd need to implement listing and reading
        # This would require extending the storage interface
        logging.warning("GCS bronze data reading not yet implemented")
        return None


def load_table_metadata(storage: StorageInterface, table_id: str) -> Dict[str, Any]:
    """Load table metadata from bronze layer"""

    if isinstance(storage, LocalStorage):
        bronze_base = Path(storage.base_dir)

        # Look for the most recent metadata files - simplified paths
        data_metadata_files = list(bronze_base.glob(f"*/{table_id}_data_metadata.json"))
        tableinfo_metadata_files = list(
            bronze_base.glob(f"*/{table_id}_tableinfo_metadata.json")
        )
        tableinfo_files = list(bronze_base.glob(f"*/{table_id}_tableinfo.json"))

        metadata = {}

        if data_metadata_files:
            data_metadata_files.sort(reverse=True)
            try:
                with open(data_metadata_files[0]) as f:
                    metadata["data_processing"] = json.load(f)
            except Exception as e:
                logging.warning(f"Could not load data metadata: {e}")

        if tableinfo_metadata_files:
            tableinfo_metadata_files.sort(reverse=True)
            try:
                with open(tableinfo_metadata_files[0]) as f:
                    metadata["tableinfo_processing"] = json.load(f)
            except Exception as e:
                logging.warning(f"Could not load tableinfo metadata: {e}")

        if tableinfo_files:
            tableinfo_files.sort(reverse=True)
            try:
                with open(tableinfo_files[0]) as f:
                    metadata["table_structure"] = json.load(f)
            except Exception as e:
                logging.warning(f"Could not load table structure: {e}")

    return metadata


def load_dst_json_into_duckdb(json_data: Dict[str, Any], table_name: str):
    """Load DST API JSONSTAT response directly into DuckDB using ibis"""

    if "dataset" not in json_data:
        raise ValueError("Invalid JSONSTAT format: missing 'dataset'")

    dataset = json_data["dataset"]

    if "dimension" not in dataset or "value" not in dataset:
        raise ValueError("Invalid JSONSTAT format: missing 'dimension' or 'value'")

    dimensions = dataset["dimension"]
    values = dataset["value"]

    logging.info(f"Processing JSONSTAT with dimensions: {list(dimensions.keys())}")

    # Build dimension mappings
    dim_info = {}
    for dim_id, dim_data in dimensions.items():
        if dim_id == "id":  # Skip the id field
            continue
        if "category" in dim_data and "label" in dim_data["category"]:
            dim_info[dim_id] = {
                "index": dim_data["category"]["index"],
                "label": dim_data["category"]["label"],
                "size": len(dim_data["category"]["index"]),
            }

    # Calculate dimensions for index calculation
    dim_names = [d for d in dimensions["id"] if d in dim_info]
    dim_sizes = [dim_info[d]["size"] for d in dim_names]

    logging.info(f"Dimension names: {dim_names}")
    logging.info(f"Dimension sizes: {dim_sizes}")

    # Convert JSONSTAT to records
    records = []
    total_cells = 1
    for size in dim_sizes:
        total_cells *= size

    logging.info(f"Total expected cells: {total_cells}")

    for i in range(total_cells):
        if i >= len(values):
            break

        # Calculate dimension indices
        record = {}
        temp_i = i

        for j, (dim_name, dim_size) in enumerate(
            zip(reversed(dim_names), reversed(dim_sizes))
        ):
            dim_index = temp_i % dim_size
            temp_i = temp_i // dim_size

            # Get the key for this dimension index
            dim_keys = list(dim_info[dim_name]["index"].keys())
            if dim_index < len(dim_keys):
                dim_key = dim_keys[dim_index]
                record[dim_name] = dim_info[dim_name]["label"].get(dim_key, dim_key)

        # Add the value
        record["INDHOLD"] = values[i]
        records.append(record)

    logging.info(f"Created {len(records)} records")
    if records:
        logging.info(f"Sample record: {records[0]}")

    # Create a table directly in DuckDB
    con.raw_sql(f"DROP TABLE IF EXISTS {table_name}")

    # Insert data using DuckDB's JSON capabilities
    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(records, f)
        temp_path = f.name

    try:
        # Load JSON file into DuckDB
        con.raw_sql(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_json('{temp_path}', auto_detect=true)
        """)

        logging.info(f"Successfully created table {table_name}")

        # Return ibis table
        table = con.table(table_name)
        logging.info(f"Retrieved ibis table: {type(table)}")
        return table
    except Exception as e:
        logging.error(f"Error creating or retrieving table: {e}")
        raise
    finally:
        # Clean up temp file
        import os

        if os.path.exists(temp_path):
            os.unlink(temp_path)


def process_hst77_data(json_data: Dict[str, Any], metadata: Dict[str, Any]):
    """Process HST77 harvest results data"""
    logging.info("Processing HST77 harvest results data")

    # Load JSON data directly into DuckDB
    df = load_dst_json_into_duckdb(json_data, "hst77_raw")
    logging.info(f"Loaded data with columns: {df.columns}")

    # DEBUG: Print schema and sample data
    try:
        logging.info(f"Table schema: {df.schema()}")
        sample = df.head(5).execute()
        logging.info(f"Sample data: {sample}")
    except Exception as e:
        logging.error(f"Error getting schema or sample: {e}")
        return None

    try:
        # HST77 specific transformations (using descriptive text values from data)
        df_transformed = df.mutate(
            # Add crop category groupings based on descriptive text
            crop_category=ibis.cases(
                (_.AFGRØDE.contains("hvede"), "Grains"),
                (_.AFGRØDE.contains("byg"), "Grains"),
                (_.AFGRØDE.contains("rug"), "Grains"),
                (_.AFGRØDE.contains("havre"), "Grains"),
                (_.AFGRØDE.contains("majs"), "Grains"),
                (_.AFGRØDE.contains("triticale"), "Grains"),
                (_.AFGRØDE.contains("korn"), "Grains"),
                (_.AFGRØDE.contains("raps"), "Rapeseed"),
                (_.AFGRØDE.contains("ærter"), "Legumes"),
                (_.AFGRØDE.contains("bønner"), "Legumes"),
                (_.AFGRØDE.contains("bælgsæd"), "Legumes"),
                (_.AFGRØDE.contains("halm"), "Straw"),
                (_.AFGRØDE.contains("kartofler"), "Root vegetables"),
                (_.AFGRØDE.contains("roer"), "Root vegetables"),
                (_.AFGRØDE.contains("rodfrugter"), "Root vegetables"),
                (_.AFGRØDE.contains("græs"), "Grass and fodder"),
                (_.AFGRØDE.contains("lucerne"), "Grass and fodder"),
                (_.AFGRØDE.contains("grøntfoder"), "Grass and fodder"),
                (_.AFGRØDE.contains("efterslæt"), "Grass and fodder"),
                else_="Other",
            )
        )
        logging.info(f"After transformations, columns: {df_transformed.columns}")
    except Exception as e:
        logging.error(f"Error in transformations: {e}")
        return None

    try:
        # Standardize column names using select with aliases
        df_renamed = df_transformed.select(
            [
                _.OMRÅDE.name("region"),
                _.AFGRØDE.name("crop_type"),
                _.MÆNGDE4.name("measurement_unit"),
                _.Tid.name("year"),
                _.INDHOLD.name("value"),
                _.ContentsCode.name("contents_code"),
                _.crop_category,
            ]
        )
        logging.info(f"After renaming, columns: {df_renamed.columns}")
    except Exception as e:
        logging.error(f"Error in renaming: {e}")
        return None

    try:
        # Handle missing data and data types
        df_clean = df_renamed.mutate(
            value=ibis.cases(
                (_.value.isnull(), None),
                else_=_.value.cast("float64"),
            ),
            year=_.year.cast("int32"),
            table_source=ibis.literal("HST77"),
            processed_at=ibis.literal(datetime.now().isoformat()),
            source_system=ibis.literal("Danmarks Statistik API"),
        )
        logging.info(f"Final columns: {df_clean.columns}")
    except Exception as e:
        logging.error(f"Error in final cleaning: {e}")
        return None

    return df_clean


def process_gartn1_data(json_data: Dict[str, Any], metadata: Dict[str, Any]):
    """Process GARTN1 fruit and vegetable production data"""
    logging.info("Processing GARTN1 fruit and vegetable production data")

    # Load JSON data directly into DuckDB
    df = load_dst_json_into_duckdb(json_data, "gartn1_raw")

    # GARTN1 specific transformations (using descriptive text values from data)
    df_transformed = df.mutate(
        # Add crop category groupings based on descriptive text
        crop_category=ibis.cases(
            (_.AFGRØDE.contains("kål"), "Cabbage varieties"),
            (_.AFGRØDE.contains("blomkål"), "Cabbage varieties"),
            (_.AFGRØDE.contains("broccoli"), "Cabbage varieties"),
            (_.AFGRØDE.contains("rosenkål"), "Cabbage varieties"),
            (_.AFGRØDE.contains("salat"), "Leafy vegetables"),
            (_.AFGRØDE.contains("spinat"), "Leafy vegetables"),
            (_.AFGRØDE.contains("purløg"), "Leafy vegetables"),
            (_.AFGRØDE.contains("løg"), "Root and fruit vegetables"),
            (_.AFGRØDE.contains("gulerødder"), "Root and fruit vegetables"),
            (_.AFGRØDE.contains("radiser"), "Root and fruit vegetables"),
            (_.AFGRØDE.contains("tomater"), "Root and fruit vegetables"),
            (_.AFGRØDE.contains("agurker"), "Root and fruit vegetables"),
            (_.AFGRØDE.contains("ærter"), "Root and fruit vegetables"),
            (_.AFGRØDE.contains("bønner"), "Root and fruit vegetables"),
            (_.AFGRØDE.contains("jordbær"), "Fruits and berries"),
            (_.AFGRØDE.contains("hindbær"), "Fruits and berries"),
            (_.AFGRØDE.contains("solbær"), "Fruits and berries"),
            (_.AFGRØDE.contains("kirsebær"), "Fruits and berries"),
            (_.AFGRØDE.contains("æbler"), "Fruits and berries"),
            (_.AFGRØDE.contains("pærer"), "Fruits and berries"),
            (_.AFGRØDE.contains("blommer"), "Fruits and berries"),
            else_="Other",
        )
    )

    # Standardize column names using select with aliases
    df_renamed = df_transformed.select(
        [
            _.OMRÅDE.name("region"),
            _.AFGRØDE.name("crop_type"),
            _.TAL.name("measurement_unit"),
            _.Tid.name("year"),
            _.INDHOLD.name("value"),
            _.crop_category,
        ]
    )

    # Handle missing data and data types
    df_clean = df_renamed.mutate(
        value=ibis.cases(
            (_.value.isnull(), None),
            else_=_.value.cast("float64"),
        ),
        year=_.year.cast("int32"),
        table_source=ibis.literal("GARTN1"),
        processed_at=ibis.literal(datetime.now().isoformat()),
        source_system=ibis.literal("Danmarks Statistik API"),
    )

    return df_clean


def process_fro_data(json_data: Dict[str, Any], metadata: Dict[str, Any]):
    """Process FRO seed production data"""
    logging.info("Processing FRO seed production data")

    # Load JSON data directly into DuckDB
    df = load_dst_json_into_duckdb(json_data, "fro_raw")

    # FRO specific transformations (using descriptive text values from data)
    df_transformed = df.mutate(
        # Add crop category groupings based on descriptive text
        crop_category=ibis.cases(
            (_.AFGRØDE.contains("kløver"), "Grass legumes"),
            (_.AFGRØDE.contains("lucerne"), "Grass legumes"),
            (_.AFGRØDE.contains("vikke"), "Grass legumes"),
            (_.AFGRØDE.contains("græs"), "Grass seeds"),
            (_.AFGRØDE.contains("rajgræs"), "Grass seeds"),
            (_.AFGRØDE.contains("fescue"), "Grass seeds"),
            (_.AFGRØDE.contains("hundegræs"), "Grass seeds"),
            (_.AFGRØDE.contains("timothe"), "Grass seeds"),
            (_.AFGRØDE.contains("I ALT"), "All seeds"),
            else_="Other",
        )
    )

    # Standardize column names using select with aliases (FRO doesn't have OMRÅDE)
    df_renamed = df_transformed.select(
        [
            ibis.literal("National").name("region"),  # Add region column
            _.AFGRØDE.name("crop_type"),
            _.MÆNGDE4.name("measurement_unit"),
            _.Tid.name("year"),
            _.INDHOLD.name("value"),
            _.crop_category,
        ]
    )

    # Handle missing data and data types
    df_clean = df_renamed.mutate(
        value=ibis.cases(
            (_.value.isnull(), None),
            else_=_.value.cast("float64"),
        ),
        year=_.year.cast("int32"),
        table_source=ibis.literal("FRO"),
        processed_at=ibis.literal(datetime.now().isoformat()),
        source_system=ibis.literal("Danmarks Statistik API"),
    )

    return df_clean


def process_halm1_data(json_data: Dict[str, Any], metadata: Dict[str, Any]):
    """Process HALM1 straw yield and usage data"""
    logging.info("Processing HALM1 straw yield and usage data")

    # Load JSON data directly into DuckDB
    df = load_dst_json_into_duckdb(json_data, "halm1_raw")

    # HALM1 specific transformations (using descriptive text values from data)
    df_transformed = df.mutate(
        # Add crop category groupings based on descriptive text
        crop_category=ibis.cases(
            (_.AFGRØDE.contains("hvede"), "Grains"),
            (_.AFGRØDE.contains("byg"), "Grains"),
            (_.AFGRØDE.contains("rug"), "Grains"),
            (_.AFGRØDE.contains("havre"), "Grains"),
            (_.AFGRØDE.contains("majs"), "Grains"),
            (_.AFGRØDE.contains("triticale"), "Grains"),
            (_.AFGRØDE.contains("korn"), "Grains"),
            (_.AFGRØDE.contains("raps"), "Rapeseed"),
            (_.AFGRØDE.contains("ærter"), "Legumes"),
            (_.AFGRØDE.contains("bønner"), "Legumes"),
            (_.AFGRØDE.contains("bælgsæd"), "Legumes"),
            (_.AFGRØDE.contains("I ALT"), "All crops"),
            else_="Other",
        )
    )

    # Standardize column names using select with aliases
    df_renamed = df_transformed.select(
        [
            _.OMRÅDE.name("region"),
            _.AFGRØDE.name("crop_type"),
            _.ENHED.name("measurement_unit"),
            _.ANVENDELSE.name("usage_type"),
            _.Tid.name("year"),
            _.INDHOLD.name("value"),
            _.crop_category,
        ]
    )

    # Handle missing data and data types
    df_clean = df_renamed.mutate(
        value=ibis.cases(
            (_.value.isnull(), None),
            else_=_.value.cast("float64"),
        ),
        year=_.year.cast("int32"),
        table_source=ibis.literal("HALM1"),
        processed_at=ibis.literal(datetime.now().isoformat()),
        source_system=ibis.literal("Danmarks Statistik API"),
    )

    return df_clean


def save_silver_data(
    df,
    storage: StorageInterface,
    table_id: str,
    timestamp: str,
    silver_dir: str = "silver/dst",
) -> None:
    """Save processed data to silver layer in Parquet format using DuckDB native export"""

    logging.info(f"Saving {table_id} data with columns: {df.columns}")

    # Create paths following silver layer structure
    parquet_filename = f"{table_id.lower()}_processed.parquet"
    metadata_filename = f"{table_id.lower()}_metadata.json"

    # Get record count and columns from ibis (no pandas needed)
    record_count = df.count().execute()
    columns = list(df.columns)

    # Create a temporary table name for the export
    temp_table_name = f"{table_id.lower()}_export"

    # Create the table in DuckDB for export
    con.raw_sql(f"DROP TABLE IF EXISTS {temp_table_name}")
    con.raw_sql(f"CREATE TABLE {temp_table_name} AS SELECT * FROM ({df.compile()})")

    if isinstance(storage, LocalStorage):
        # For local storage, use separate silver directory structure - simplified
        from pathlib import Path

        silver_base = Path(silver_dir)
        local_dir = silver_base / timestamp
        local_dir.mkdir(parents=True, exist_ok=True)

        parquet_path = local_dir / parquet_filename
        metadata_path = local_dir / metadata_filename

        # Use DuckDB's native COPY command to export directly to parquet
        con.raw_sql(f"""
            COPY (SELECT * FROM {temp_table_name}) 
            TO '{parquet_path}' (FORMAT 'parquet')
        """)

        logging.info(f"Exported {record_count} records directly to {parquet_path}")

    else:
        # For GCS storage, export to temp file first then upload
        import tempfile

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            # Export to temporary parquet file using DuckDB
            con.raw_sql(f"""
                COPY (SELECT * FROM {temp_table_name}) 
                TO '{temp_path}' (FORMAT 'parquet')
            """)

            # Upload to GCS using storage interface
            gcs_path = f"{timestamp}/{parquet_filename}"

            # Read temp parquet and save via storage interface
            import pandas as pd  # Only used here for GCS upload compatibility

            temp_df = pd.read_parquet(temp_path)
            storage.save_parquet(temp_df, gcs_path)

            logging.info(f"Exported {record_count} records to GCS at {gcs_path}")

        finally:
            # Clean up temp file
            import os

            if os.path.exists(temp_path):
                os.unlink(temp_path)

        metadata_path = f"{timestamp}/{metadata_filename}"

    # Save metadata
    metadata = {
        "table_id": table_id,
        "processed_at": datetime.now().isoformat(),
        "columns": columns,
        "record_count": record_count,
        "file_path": f"{timestamp}/{parquet_filename}",
        "format": "parquet",
        "processing_pipeline": "DST API Silver Layer",
        "layer": "silver",
        "pipeline_name": "dst_api_pipeline",
        "source_system": "Danmarks Statistik API",
    }

    if isinstance(storage, LocalStorage):
        # Save metadata locally to silver directory
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        logging.info(f"Saved metadata to {metadata_path}")
    else:
        # Save metadata to GCS
        storage.save_json(metadata, metadata_path)
        logging.info(f"Saved metadata to GCS at {metadata_path}")

    logging.info(
        f"Successfully saved {record_count} records using DuckDB native export"
    )


def main_with_args(args: argparse.Namespace) -> bool:
    """Main silver layer processing with provided arguments"""
    setup_logging(args.log_level)

    logging.info("Starting DST API Silver Layer processing")

    # Initialize storage interface
    storage = get_storage_interface()

    # Processing functions for each table
    processors = {
        "HST77": process_hst77_data,
        "GARTN1": process_gartn1_data,
        "FRO": process_fro_data,
        "HALM1": process_halm1_data,
    }

    # Create timestamped output directory
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    processed_tables = []

    for table_id in args.tables:
        try:
            logging.info(f"Processing table: {table_id}")

            # Find and load latest bronze data
            bronze_data = find_latest_bronze_data(storage, table_id)
            if not bronze_data:
                logging.warning(f"No bronze data found for table {table_id}")
                continue

            logging.info(f"Found bronze data for {table_id}")

            # Load metadata
            metadata = load_table_metadata(storage, table_id)

            # Process data using appropriate processor
            if table_id in processors:
                logging.info(f"Calling processor for {table_id}")
                try:
                    df_processed = processors[table_id](bronze_data, metadata)
                    logging.info(f"Processor completed for {table_id}")
                except Exception as e:
                    logging.error(f"Error in processor for {table_id}: {e}")
                    raise

                # Save to silver layer
                try:
                    save_silver_data(
                        df_processed, storage, table_id, timestamp, args.silver_dir
                    )
                    processed_tables.append(table_id)
                    logging.info(f"Successfully processed {table_id}")
                except Exception as e:
                    logging.error(f"Error saving data for {table_id}: {e}")
                    raise
            else:
                logging.warning(f"No processor available for table {table_id}")

        except Exception as e:
            logging.error(f"Failed to process table {table_id}: {e}")
            continue

    # Create summary metadata
    summary = {
        "processing_timestamp": timestamp,
        "processed_tables": processed_tables,
        "total_tables": len(processed_tables),
        "processing_status": "completed" if processed_tables else "failed",
        "layer": "silver",
        "pipeline_name": "dst_api_pipeline",
        "source_system": "Danmarks Statistik API",
    }

    # Save summary to silver directory
    if isinstance(storage, LocalStorage):
        from pathlib import Path

        silver_base = Path(args.silver_dir)
        summary_dir = silver_base / timestamp
        summary_dir.mkdir(parents=True, exist_ok=True)
        summary_file = summary_dir / "processing_summary.json"
        with open(summary_file, "w") as f:
            json.dump(summary, f, indent=2)
        logging.info(f"Saved summary to {summary_file}")
    else:
        # Save summary to GCS
        summary_path = f"{timestamp}/processing_summary.json"
        storage.save_json(summary, summary_path)

    logging.info("Silver layer processing completed successfully")
    logging.info(
        f"Processed {len(processed_tables)} tables: {', '.join(processed_tables)}"
    )
    logging.info(f"Output saved with timestamp: {timestamp}")

    return len(processed_tables) > 0


def main():
    """Main silver layer processing with command line parsing"""
    args = parse_args()
    success = main_with_args(args)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

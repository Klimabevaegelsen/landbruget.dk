import json
import logging
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import ibis
from dotenv import load_dotenv

# Import config
# Import table creation functions
# from . import entities # Removed entity import
# from . import herd_sizes  # Removed since functionality is now in herds.py
from . import (
    animal_movements,
    antibiotic_usage,
    config,
    herds,
    properties,
    property_vet_events,
    vet_practices,
)

# Import helpers
from .helpers import (
    _create_and_save_lookup,
    get_latest_bronze_dir,
    run_xml_parser,
)

# Configure logging
log_file_path = Path(__file__).resolve().parent / "silver_processing.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=log_file_path,
    filemode="w",
)

logging.info("--- Script execution started ---")

# Load environment variables
load_dotenv()


# --- Main Processing Logic ---
def process_chr_data(
    bronze_dir: Optional[Path] = None,
    silver_dir: Path = None,
    in_memory_data: Optional[Dict[str, Dict[str, List[Any]]]] = None,
    export_timestamp: Optional[str] = None,
):
    """Main function to process CHR data from bronze to silver.

    Args:
        bronze_dir: Path to the bronze data directory (used if in_memory_data is None).
        silver_dir: Path to the silver data output directory.
        in_memory_data: Optional dictionary containing buffered bronze data.
        export_timestamp: The timestamp string used for the bronze export (YYYYMMDD_HHMMSS).
                          Required if loading from files as fallback.
    """
    logging.info("--- Starting CHR Silver Processing --- ")
    # Create silver directory if it doesn't exist
    silver_dir.mkdir(parents=True, exist_ok=True)

    # Determine data source mode (prefer memory)
    load_from_memory = in_memory_data is not None
    load_from_files_fallback = bronze_dir is not None and export_timestamp is not None

    if not load_from_memory and not load_from_files_fallback:
        logging.error(
            "Cannot process silver data: Either in_memory_data or (bronze_dir and export_timestamp for fallback) must be provided."
        )
        sys.exit(1)

    source_mode_log = "in-memory buffer" if load_from_memory else f"files in {bronze_dir}/{export_timestamp} (fallback)"
    logging.info(f"Silver processing source mode: {source_mode_log}")

    # --- Define Input File Paths or Data Sources ---
    if in_memory_data:
        # Extract data from memory
        besaetning_list_data = in_memory_data.get("besaetning_list", {}).get("json", [])
        besaetning_details_data = in_memory_data.get("besaetning_details", {}).get("json", [])
        diko_flytninger_data = in_memory_data.get("diko_flytninger", {}).get("json", [])
        ejendom_oplysninger_data = in_memory_data.get("ejendom_oplysninger", {}).get("json", [])
        ejendom_vet_events_data = in_memory_data.get("ejendom_vet_events", {}).get("json", [])
        vetstat_antibiotics_data = in_memory_data.get("vetstat_antibiotics", {}).get("xml", [])

        # Write VetStat XML to temp file if needed
        vetstat_antibiotics_xml_path = None
        if vetstat_antibiotics_data:
            # Ensure the temporary XML file is cleaned up
            temp_xml_path_obj = silver_dir / "_temp_vetstat.xml"
            # DEBUG(Added): Define a path for the saved XML in case of issues
            saved_xml_path_obj = silver_dir / f"_DEBUG_FAILED_vetstat_{export_timestamp or 'unknown'}.xml"
            try:
                with open(temp_xml_path_obj, "w") as f:
                    # Add separator compatible with VetStat XML parser's expectations
                    f.write("\n<!-- RAW_RESPONSE_SEPARATOR -->\n".join(vetstat_antibiotics_data))
                vetstat_antibiotics_xml_path = temp_xml_path_obj  # Assign path only if successfully written
                # DEBUG(Added): Log size of temp XML before parsing
                try:
                    xml_size = vetstat_antibiotics_xml_path.stat().st_size
                    logging.info(
                        f"Created temporary VetStat XML file: {vetstat_antibiotics_xml_path} (Size: {xml_size} bytes)"
                    )
                except Exception as e_stat:
                    logging.warning(f"Could not get size of temp XML file {vetstat_antibiotics_xml_path}: {e_stat}")
                    logging.info(f"Created temporary VetStat XML file: {vetstat_antibiotics_xml_path}")
                # --- END DEBUG ---
            except Exception as e_write:
                logging.error(
                    f"Failed to write temporary VetStat XML file: {e_write}",
                    exc_info=True,
                )
                # Ensure path is None if write failed
                vetstat_antibiotics_xml_path = None
                # Clean up potentially partially written file
                if temp_xml_path_obj.exists():
                    try:
                        temp_xml_path_obj.unlink()
                    except OSError:
                        pass
            # NOTE: Cleanup happens in the finally block of the XML parsing section below
    else:
        # Use file paths as before
        besaetning_list_path = bronze_dir / "besaetning_list.json"
        besaetning_details_path = bronze_dir / "besaetning_details.json"
        diko_flytninger_path = bronze_dir / "diko_flytninger.json"
        ejendom_oplysninger_path = bronze_dir / "ejendom_oplysninger.json"
        ejendom_vet_events_path = bronze_dir / "ejendom_vet_events.json"
        vetstat_antibiotics_xml_path = bronze_dir / "vetstat_antibiotics.xml"

    # Define intermediate path for parsed XML (placed in silver dir for easier cleanup)
    vetstat_antibiotics_jsonl_path = silver_dir / "_intermediate_vetstat.jsonl"

    # --- 1. Pre-process VetStat XML to JSONL ---
    vetstat_loaded = False
    temp_xml_created_in_silver = load_from_memory and vetstat_antibiotics_xml_path is not None
    try:
        if vetstat_antibiotics_xml_path and vetstat_antibiotics_xml_path.exists():
            try:
                run_xml_parser(vetstat_antibiotics_xml_path, vetstat_antibiotics_jsonl_path)
                if vetstat_antibiotics_jsonl_path.exists() and vetstat_antibiotics_jsonl_path.stat().st_size > 0:
                    vetstat_loaded = True
                    logging.info(f"Successfully created intermediate VetStat JSONL: {vetstat_antibiotics_jsonl_path}")
                else:
                    logging.warning(
                        f"XML parser ran but output file is empty or missing: {vetstat_antibiotics_jsonl_path}"
                    )
                    # DEBUG(Added): Save the failed XML file if parsing failed/was empty
                    if vetstat_antibiotics_xml_path.exists():
                        try:
                            vetstat_antibiotics_xml_path.rename(saved_xml_path_obj)
                            logging.warning(
                                f"DEBUG(Added): Saved problematic XML to {saved_xml_path_obj} for inspection."
                            )
                            vetstat_antibiotics_xml_path = None  # Ensure it's not cleaned up later normally
                        except Exception as e_save:
                            logging.error(
                                f"DEBUG(Added): Failed to save problematic XML {vetstat_antibiotics_xml_path}: {e_save}"
                            )
                    # --- END DEBUG ---
            except (FileNotFoundError, RuntimeError, Exception) as e:
                logging.error(
                    f"Failed to process VetStat XML: {e}. Proceeding without antibiotic data.",
                    exc_info=True,
                )
                # DEBUG(Added): Save the failed XML file if parsing failed/was empty
                if (
                    vetstat_antibiotics_xml_path and vetstat_antibiotics_xml_path.exists()
                ):  # Check again as path might be None
                    try:
                        vetstat_antibiotics_xml_path.rename(saved_xml_path_obj)
                        logging.warning(f"DEBUG(Added): Saved problematic XML to {saved_xml_path_obj} for inspection.")
                        vetstat_antibiotics_xml_path = None  # Ensure it's not cleaned up later normally
                    except Exception as e_save:
                        logging.error(
                            f"DEBUG(Added): Failed to save problematic XML {vetstat_antibiotics_xml_path}: {e_save}"
                        )
                # --- END DEBUG ---
                # Ensure path is None if failed
                if vetstat_antibiotics_jsonl_path.exists():
                    try:
                        vetstat_antibiotics_jsonl_path.unlink()  # Clean up failed attempt
                    except OSError:
                        pass
        else:
            logging.warning(
                f"VetStat XML file not found or not provided ({'in-memory path was' if load_from_memory else 'bronze path was'} {vetstat_antibiotics_xml_path}). Skipping antibiotic data processing."
            )
            vetstat_antibiotics_jsonl_path = None
    finally:
        # Clean up the temporary XML file created from in-memory data
        if temp_xml_created_in_silver and vetstat_antibiotics_xml_path and vetstat_antibiotics_xml_path.exists():
            try:
                vetstat_antibiotics_xml_path.unlink()
                logging.info(f"Cleaned up temporary VetStat XML file: {vetstat_antibiotics_xml_path}")
            except OSError as e_del:
                logging.warning(f"Could not delete temporary VetStat XML file {vetstat_antibiotics_xml_path}: {e_del}")

    # --- 2. Initialize Ibis and DuckDB Connection ---
    logging.info("Initializing Ibis with DuckDB backend (in-memory)")
    try:
        con = ibis.duckdb.connect()  # In-memory by default
        # Install necessary DuckDB extensions if not already present
        con.con.sql("INSTALL httpfs;")
        con.con.sql("LOAD httpfs;")
        con.con.sql("INSTALL spatial;")
        con.con.sql("LOAD spatial;")
        con.con.sql("INSTALL json;")
        con.con.sql("LOAD json;")
        logging.info("DuckDB extensions httpfs, spatial, json loaded.")
    except Exception as e:
        logging.error(f"Failed to initialize DuckDB or load extensions: {e}", exc_info=True)
        sys.exit(1)

    # --- 3. Load Bronze Data into Ibis Tables ---
    logging.info("Loading bronze data into Ibis tables...")
    raw_tables = {}

    # Define sources and their corresponding keys/paths
    sources_to_load = {
        "bes_list": {"mem_key": "besaetning_list", "file_key": "besaetning_list.json"},
        "bes_details": {
            "mem_key": "besaetning_details",
            "file_key": "besaetning_details.json",
        },
        "diko_flyt": {"mem_key": "diko_flytninger", "file_key": "diko_flytninger.json"},
        "chr_dyr_animal_movements": {
            "mem_key": "chr_dyr_animal_movements",
            "file_key": "chr_dyr_animal_movements.json",
        },
        "ejendom_oplys": {
            "mem_key": "ejendom_oplysninger",
            "file_key": "ejendom_oplysninger.json",
        },
        "ejendom_vet": {
            "mem_key": "ejendom_vet_events",
            "file_key": "ejendom_vet_events.json",
        },
        # Vetstat is handled separately due to XML -> JSONL preprocessing
    }

    for table_name, source_info in sources_to_load.items():
        input_source = None
        source_desc = "unknown"
        json_data_str = None  # To hold data for logging if needed

        # Define a helper to handle date serialization for JSON
        def date_serializer(obj):
            if isinstance(obj, date):  # Correctly handle date objects
                return obj.isoformat()
            raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

        # Reset load status for each table
        successfully_loaded = False

        # --- Attempt 1: Load from Memory using DuckDB Python API --- #
        if load_from_memory:
            logging.info(f"Attempting to load '{table_name}' from in-memory buffer...")
            data = in_memory_data.get(source_info["mem_key"], {}).get("json", [])
            if data and isinstance(data, list):
                logging.info(f"Found {len(data)} records in memory for {source_info['mem_key']}")
                # Convert list of dicts to Pandas DataFrame for robust handling - REMOVED THIS STEP
                # Instead, write to temp JSONL and use read_json
                temp_jsonl_path = None
                temp_file = None
                try:
                    # Create a temporary file to write JSONL data
                    # Use silver_dir to ensure it's within accessible/writable space if run in restricted envs
                    temp_file = tempfile.NamedTemporaryFile(
                        mode="w",
                        encoding="utf-8",
                        suffix=".jsonl",
                        delete=False,  # Keep the file until manually deleted
                        dir=silver_dir,  # Place temp file in silver dir
                        prefix=f"temp_{table_name}_",
                    )
                    temp_jsonl_path = Path(temp_file.name)
                    logging.info(
                        f"Writing in-memory data for '{table_name}' to temporary JSONL: {temp_jsonl_path.name}"
                    )

                    for record in data:
                        # Ensure complex objects are handled by json.dumps
                        try:
                            # Revised JSONL writing:
                            json_string = json.dumps(record, default=str)  # Serialize to string first
                            temp_file.write(json_string + "\n")  # Write string + newline
                        except TypeError as e_json:
                            logging.warning(
                                f"Skipping record due to JSON serialization error for table '{table_name}': {e_json}. Record sample: {str(record)[:200]}..."
                            )
                            continue  # Skip bad records

                    temp_file.flush()  # Ensure all data is written
                    temp_file.close()  # Close the file handle

                    logging.info(f"Finished writing temporary JSONL for '{table_name}'. Attempting read_json_auto...")

                    # Use DuckDB SQL directly to read JSONL and create the table
                    # read_json_auto handles schema inference and newline delimited format
                    # Pass maximum_object_size directly as a parameter to the function
                    # Set to 1GB (1073741824 bytes)
                    max_obj_size_bytes = 1073741824
                    con.con.sql(
                        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{str(temp_jsonl_path)}', maximum_object_size={max_obj_size_bytes});"
                    )
                    raw_tables[table_name] = con.table(table_name)  # Get Ibis table reference

                    successfully_loaded = True
                    source_desc = (
                        f"in-memory buffer via temp JSONL ({temp_jsonl_path.name}) for '{source_info['mem_key']}'"
                    )
                    logging.info(f"Successfully loaded {source_desc} into table '{table_name}' using read_json_auto.")
                    schema = raw_tables[table_name].schema()
                    logging.info(f"Schema for {table_name} (from read_json_auto): {schema}")

                except Exception as e_mem_jsonl:
                    logging.error(
                        f"Failed to load '{table_name}' from memory via temp JSONL: {e_mem_jsonl}",
                        exc_info=True,
                    )
                    # Clean up potentially created table on failure
                    try:
                        con.con.sql(f"DROP TABLE IF EXISTS {table_name};")
                    except Exception:
                        pass
                finally:
                    # Clean up the temporary JSONL file
                    if temp_jsonl_path and temp_jsonl_path.exists():
                        try:
                            temp_jsonl_path.unlink()
                            logging.info(f"Removed temporary JSONL file: {temp_jsonl_path.name}")
                        except OSError as e_del:
                            logging.warning(f"Could not delete temporary JSONL file {temp_jsonl_path.name}: {e_del}")
                    if temp_file and not temp_file.closed:
                        temp_file.close()  # Ensure closed if error occurred before explicit close
            else:
                logging.warning(
                    f"No data (or not a list) found in memory buffer for {source_info['mem_key']}. Will attempt file fallback if configured."
                )

        # --- Attempt 2: Load from File using Ibis (Fallback) --- #
        if not successfully_loaded and load_from_files_fallback:
            logging.info(f"Attempting to load '{table_name}' from file (fallback mode)...")
            timestamped_bronze_dir = bronze_dir / export_timestamp
            path = timestamped_bronze_dir / source_info["file_key"]

            if path.exists():
                input_source = str(path)
                source_desc = f"file '{path.relative_to(bronze_dir.parent)}' (fallback)"

                logging.info(f"Loading {source_desc} into table '{table_name}' using ibis.read_json...")
                try:
                    con.con.sql(f"DROP TABLE IF EXISTS {table_name};")  # Ensure clean slate
                    # Use newline_delimited format and auto_detect
                    raw_tables[table_name] = con.read_json(input_source, format="newline_delimited", auto_detect=True)
                    successfully_loaded = True
                    logging.info(
                        f"Successfully loaded {source_desc} into table '{table_name}' (using newline_delimited and auto_detect)."
                    )

                    # Log schema for debugging
                    schema = raw_tables[table_name].schema()
                    logging.info(f"Schema for {table_name}: {schema}")

                except Exception as e_file:
                    logging.error(
                        f"Fallback file loading failed for {source_desc} into table '{table_name}': {e_file}",
                        exc_info=True,
                    )
                    try:
                        with open(input_source, "r", encoding="utf-8") as f_err:
                            logging.error(f"File content sample (first 1000 chars): {f_err.read(1000)}...")
                    except Exception as read_err:
                        logging.error(f"Could not read file {input_source} to log sample: {read_err}")
                    # Ensure table doesn't exist if load failed
                    try:
                        con.con.sql(f"DROP TABLE IF EXISTS {table_name};")
                    except Exception:
                        pass
            else:
                logging.warning(f"Fallback file not found: {path}. Cannot load table '{table_name}'.")

        if not successfully_loaded:
            logging.error(f"Failed to load table '{table_name}' from all available sources.")

    # Handle VetStat separately (reading from the pre-processed JSONL file in silver)
    # Construct path within the silver directory
    vetstat_antibiotics_jsonl_path = silver_dir / "_intermediate_vetstat.jsonl"
    if vetstat_antibiotics_jsonl_path.exists():  # Check if it exists in silver
        logging.info(f"Loading pre-processed VetStat data from {vetstat_antibiotics_jsonl_path.name}...")
        try:
            raw_tables["vetstat"] = con.read_json(str(vetstat_antibiotics_jsonl_path), format="newline_delimited")
            logging.info("Successfully loaded vetstat data.")
            schema = raw_tables["vetstat"].schema()
            logging.info(f"Schema for vetstat: {schema}")
        except Exception as e:
            logging.error(f"Error loading vetstat JSONL data: {e}")
            logging.error(f"DEBUG(Added): Vetstat JSONL load failed from path: {vetstat_antibiotics_jsonl_path}")
    else:
        logging.warning(
            f"Skipping VetStat table loading as pre-processed file {vetstat_antibiotics_jsonl_path} is not available."
        )

    # --- Check if essential tables were loaded ---
    if "bes_details" not in raw_tables:
        logging.error("Essential table 'bes_details' could not be loaded. Aborting processing.")
        sys.exit(1)
    if "ejendom_oplys" not in raw_tables:
        logging.error("Essential table 'ejendom_oplys' could not be loaded. Aborting processing.")
        sys.exit(1)

    # --- DEBUG(Added): Directly print DESCRIBE output for bes_details ---
    if "bes_details" in raw_tables:
        logging.info("DEBUG(Added): Attempting to print DESCRIBE bes_details output...")
        print("\\n--- DEBUG: DESCRIBE bes_details TABLE ---", flush=True)  # Add marker and flush
        try:
            con.con.sql("DESCRIBE bes_details;").show()
            print("--- END DEBUG: DESCRIBE bes_details TABLE ---\\n", flush=True)  # Add marker and flush
        except Exception as e_describe:
            logging.error(
                f"DEBUG(Added): Error executing DESCRIBE bes_details: {e_describe}",
                exc_info=True,
            )
            print(
                f"--- DEBUG: ERROR DESCRIBING bes_details: {e_describe} ---\\n",
                flush=True,
            )
    else:
        logging.warning("DEBUG(Added): 'bes_details' table not found in raw_tables for DESCRIBE.")
        print("--- DEBUG: bes_details TABLE NOT FOUND ---\\n", flush=True)
    # --- END DEBUG ---

    # --- START DEBUG (Added): Describe unnested BesStr structure ---
    # Add comment for easy removal later
    if "bes_details" in raw_tables:
        logging.info("DEBUG(Added): Attempting to describe unnested BesStr structure...")
        print("\\n--- DEBUG: DESCRIBE UNNESTED BesStr STRUCT ---", flush=True)
        try:
            # Query to get the structure of the items within the BesStr list
            # We only need one sample, hence LIMIT 1 after the first unnest
            describe_query = """ \
            DESCRIBE SELECT size_info \
            FROM ( \
                SELECT UNNEST(Response) AS r \
                FROM bes_details LIMIT 1 \
            ), UNNEST(r.Besaetning.BesStr) AS size_info; \
            """
            con.con.sql(describe_query).show()
            print("--- END DEBUG: DESCRIBE UNNESTED BesStr STRUCT ---\\n", flush=True)
        except Exception as e_describe_besstr:
            # Log potential errors, e.g., if BesStr is empty in the first record
            logging.error(
                f"DEBUG(Added): Error executing DESCRIBE on unnested BesStr: {e_describe_besstr}",
                exc_info=False,
            )  # Don't need full traceback here
            print(
                f"--- DEBUG: ERROR DESCRIBING UNNESTED BesStr: {e_describe_besstr} ---\\n",
                flush=True,
            )
    # --- END DEBUG (Added) ---

    # --- 4. Create & Populate Lookup Tables ---
    logging.info("Creating and saving lookup tables...")
    lookup_tables = {}

    # Create age_groups lookup (simpler source)
    try:
        if "vetstat" in raw_tables:
            lookup_tables["age_groups"] = _create_and_save_lookup(
                con,
                raw_tables["vetstat"],
                pk_col="Aldersgruppekode",
                name_col="Aldersgruppe",
                output_path=silver_dir / "age_groups.parquet",
                table_name="age_groups",
            )
        else:
            logging.warning("Could not create age_groups lookup: 'vetstat' table missing.")
    except Exception as e:
        logging.error(f"Failed age_groups lookup creation: {e}")

    # --- 5. Process Silver Steps in Order ---
    context = {
        "bes_details_table": raw_tables.get("bes_details"),
        "diko_flyt_table": raw_tables.get("diko_flyt"),
        "chr_dyr_animal_movements_table": raw_tables.get("chr_dyr_animal_movements"),
        "ejendom_oplys_table": raw_tables.get("ejendom_oplys"),
        "ejendom_vet_table": raw_tables.get("ejendom_vet"),
        "vetstat_table": raw_tables.get("vetstat"),
        "lookup_tables": lookup_tables,
    }

    # Define silver steps in order
    silver_steps = [
        # 'silver_entities', # Removed this step
        "silver_vet_practices",
        "silver_properties",
        "silver_property_owners",  # Added property owners step
        "silver_property_users",  # Added property users step
        "silver_herds",
        "silver_herd_owners",  # Added herd owners step
        "silver_herd_users",  # Added herd users step
        "silver_herd_sizes",
        "silver_animal_movements",
        "silver_chr_dyr_animal_movements",  # Added CHR_dyr animal movements step
        "silver_property_vet_events",
        "silver_antibiotic_usage",
    ]

    # Process each silver step
    for step in silver_steps:
        logging.info(f"Processing silver step: {step}")
        try:
            # Removed silver_entities step block entirely
            if step == "silver_vet_practices":
                vet_practices_table = vet_practices.create_vet_practices_table(
                    con, context.get("bes_details_table"), silver_dir
                )
                context["vet_practices_table"] = vet_practices_table

            elif step == "silver_properties":
                properties_table = properties.create_properties_table(
                    con, context.get("ejendom_oplys_table"), silver_dir
                )
                context["properties_table"] = properties_table

            elif step == "silver_property_owners":  # Added step for property owners
                property_owners_table = properties.create_property_owners_table(
                    con, context.get("ejendom_oplys_table"), silver_dir
                )
                # Optionally add to context if needed: context['property_owners_table'] = property_owners_table

            elif step == "silver_property_users":  # Added step for property users
                property_users_table = properties.create_property_users_table(
                    con, context.get("ejendom_oplys_table"), silver_dir
                )
                # Optionally add to context if needed: context['property_users_table'] = property_users_table

            elif step == "silver_herds":
                herds_table = herds.create_herds_table(
                    con,
                    context.get("bes_details_table"),
                    # context.get('entity_map_table'), # Removed dependency
                    silver_dir,
                )
                context["herds_table"] = herds_table

            elif step == "silver_herd_owners":  # Added step for herd owners
                herd_owners_table = herds.create_herd_owners_table(con, context.get("bes_details_table"), silver_dir)
                # Optionally add to context if needed: context['herd_owners_table'] = herd_owners_table

            elif step == "silver_herd_users":  # Added step for herd users
                herd_users_table = herds.create_herd_users_table(con, context.get("bes_details_table"), silver_dir)
                # Optionally add to context if needed: context['herd_users_table'] = herd_users_table

            elif step == "silver_herd_sizes":
                herd_sizes_table = herds.create_herd_sizes_table(con, context.get("bes_details_table"), silver_dir)

            elif step == "silver_animal_movements":
                animal_movements_table = animal_movements.create_animal_movements_table(
                    con, context.get("diko_flyt_table"), silver_dir
                )

            elif step == "silver_chr_dyr_animal_movements":
                chr_dyr_animal_movements_table = animal_movements.create_chr_dyr_animal_movements_table(
                    con, context.get("chr_dyr_animal_movements_table"), silver_dir
                )

            elif step == "silver_property_vet_events":
                property_vet_events_table = property_vet_events.create_property_vet_events_table(
                    con,
                    context.get("ejendom_vet_table"),  # Reverted to original context get
                    context.get("lookup_tables", {}),
                    silver_dir,
                )

            elif step == "silver_antibiotic_usage":
                antibiotic_usage_table = antibiotic_usage.create_antibiotic_usage_table(
                    con,
                    context.get("vetstat_table"),
                    context.get("lookup_tables", {}),
                    silver_dir,
                )

        except Exception as e:
            logging.error(f"Error in silver step {step}: {e}", exc_info=True)
            # Continue with next step instead of failing completely
            continue

    # --- 13. Cleanup Intermediate Files ---
    if vetstat_antibiotics_jsonl_path and vetstat_antibiotics_jsonl_path.exists():
        try:
            vetstat_antibiotics_jsonl_path.unlink()
            logging.info(f"Removed intermediate file: {vetstat_antibiotics_jsonl_path}")
        except OSError as e:
            logging.warning(f"Could not remove intermediate file {vetstat_antibiotics_jsonl_path}: {e}")

    logging.info(f"Silver data processing finished. Output located in: {silver_dir}")


if __name__ == "__main__":
    # --- Determine Input and Output Directories ---
    try:
        logging.info("Determining input bronze directory...")
        # Use config constants
        if config.BRONZE_DATE_FOLDER_OVERRIDE:
            input_bronze_dir = config.BRONZE_BASE_DIR / config.BRONZE_DATE_FOLDER_OVERRIDE
            if not input_bronze_dir.is_dir():
                raise FileNotFoundError(f"Specified bronze directory does not exist: {input_bronze_dir}")
            logging.info(f"Using specified bronze data directory: {input_bronze_dir.name}")
        else:
            # Need to pass the base dir explicitly now
            input_bronze_dir = get_latest_bronze_dir(config.BRONZE_BASE_DIR)  # This call logs info
        logging.info(f"Determined input bronze directory: {input_bronze_dir}")
    except FileNotFoundError as e:
        logging.error(f"Error determining bronze data directory: {e}")
        sys.exit(1)

    # Create timestamped output directory
    processing_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Use config constant
    output_silver_dir = config.SILVER_BASE_DIR / processing_timestamp
    logging.info(f"Determined output silver directory: {output_silver_dir}")

    # --- Execute Processing ---
    try:
        logging.info("Starting process_chr_data function...")
        process_chr_data(
            bronze_dir=input_bronze_dir, silver_dir=output_silver_dir
        )  # process_chr_data needs to be defined above
        logging.info("Finished process_chr_data function.")
    except Exception as e:
        logging.critical(f"An unhandled error occurred during data processing: {e}", exc_info=True)
        sys.exit(1)

    logging.info("--- Script execution finished ---")

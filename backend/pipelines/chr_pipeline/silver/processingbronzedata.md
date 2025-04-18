# CHR Bronze to Silver Data Processing Plan

## 1. Objective

Transform the raw CHR (Central Husbandry Register) data from the bronze layer (JSON and XML files) into a clean, harmonized, and normalized set of tables in the silver layer. This silver layer data will be structured for easier analysis and potential loading into a relational database, adhering to the guidelines in `backend/README.md` where applicable, but prioritizing normalization and user-specified PII handling.

## 2. Input Data (Bronze Layer)

Located in `backend/data/bronze/chr/<date_fetched>/`:

*   `besaetning_list.json`: List of herd numbers (`Besaetningsnummer`).
*   `besaetning_details.json`: Detailed information per herd, including owner (`Ejer`), user (`Bruger`), species, usage, vet practice (`BesPraksis`), size (`BesStr`), status dates. Contains potential CVR and CPR numbers.
*   `diko_flytninger.json`: Animal movement records (`Flytninger`) between herds/locations.
*   `ejendom_oplysninger.json`: Property (`Ejendom`) details including address, coordinates (`StaldKoordinater`), administrative info (`FVST`), and associated herds (`Besaetninger`) on the property.
*   `ejendom_vet_events.json`: Veterinary events (`VeterinaereHaendelser`) linked to properties (CHR numbers).
*   `vetstat_antibiotics.xml`: Concatenated SOAP XML responses containing antibiotic usage data (`ns2:Data`) linked via CVR and CHR numbers.

## 3. Silver Layer Schema (Normalized Tables)

These tables will be stored as individual Parquet/GeoParquet files.

**Lookup Tables:** (Populated from distinct values in source data)

*   **`species`**
    *   `species_code` (INT, PK)
    *   `species_name` (TEXT)
*   **`usage_types`**
    *   `usage_code` (INT, PK)
    *   `usage_name` (TEXT)
*   **`trade_statuses`**
    *   `trade_code` (INT, PK)
    *   `trade_text` (TEXT)
*   **`diseases`**
    *   `disease_code` (TEXT, PK)
    *   `disease_name` (TEXT)
*   **`vet_statuses`**
    *   `vet_status_code` (TEXT, PK)
    *   `vet_status_text` (TEXT)
*   **`age_groups`** (From VetStat)
    *   `age_group_code` (INT, PK)
    *   `age_group_name` (TEXT)

**Entity & Location Tables:**

*   **`entities`**
    *   `entity_id` (TEXT, PK - Generated UUID v4)
    *   `cvr_number` (TEXT, nullable, unique within non-nulls?) - From `Ejer.CvrNummer`, `Bruger.CvrNummer`, `vetstat:CVRNummer`
    *   `cpr_number` (TEXT, nullable, unique within non-nulls?) - From `Ejer.CprNummer`, `Bruger.CprNummer`. **Note:** Kept as per user request, potentially sensitive PII.
    *   `name` (TEXT)
    *   `address` (TEXT)
    *   `postal_code` (INT)
    *   `postal_district` (TEXT)
    *   `city_name` (TEXT, nullable)
    *   `municipality_code` (INT)
    *   `municipality_name` (TEXT)
    *   `country` (TEXT)
    *   `phone_number` (TEXT, nullable)
    *   `mobile_number` (TEXT, nullable)
    *   `email` (TEXT, nullable)
    *   `address_protection` (BOOLEAN, nullable)
    *   `marketing_protection` (BOOLEAN, nullable)
*   **`vet_practices`**
    *   `practice_number` (INT, PK)
    *   `name` (TEXT)
    *   `address` (TEXT)
    *   `city_name` (TEXT, nullable)
    *   `postal_code` (INT)
    *   `postal_district` (TEXT)
    *   `phone_number` (TEXT, nullable)
    *   `mobile_number` (TEXT, nullable)
    *   `email` (TEXT, nullable)
*   **`properties`** (Output as GeoParquet)
    *   `chr_number` (INT, PK)
    *   `address` (TEXT)
    *   `city_name` (TEXT, nullable)
    *   `postal_code` (INT)
    *   `postal_district` (TEXT)
    *   `municipality_code` (INT)
    *   `municipality_name` (TEXT)
    *   `food_region_number` (INT)
    *   `food_region_name` (TEXT)
    *   `vet_dept_name` (TEXT)
    *   `vet_section_name` (TEXT)
    *   `geo_geometry` (GEOMETRY, Point, EPSG:4326)
    *   `geo_coord_x_source` (FLOAT) - Original X coordinate
    *   `geo_coord_y_source` (FLOAT) - Original Y coordinate
    *   `geo_crs_source` (TEXT) - e.g., 'EPSG:25832'
    *   `date_created` (DATE)
    *   `date_updated` (DATE)

**Core Data Tables:**

*   **`herds`**
    *   `herd_number` (INT, PK)
    *   `chr_number` (INT, FK -> `properties.chr_number`)
    *   `species_code` (INT, FK -> `species.species_code`)
    *   `usage_code` (INT, FK -> `usage_types.usage_code`)
    *   `business_type_name` (TEXT)
    *   `trade_code` (INT, FK -> `trade_statuses.trade_code`)
    *   `owner_entity_id` (TEXT, FK -> `entities.entity_id`)
    *   `user_entity_id` (TEXT, FK -> `entities.entity_id`)
    *   `vet_practice_number` (INT, FK -> `vet_practices.practice_number`)
    *   `is_organic` (BOOLEAN)
    *   `date_created` (DATE)
    *   `date_updated` (DATE)
    *   `date_terminated` (DATE, nullable)
    *   `last_size_update_date` (DATE)
*   **`herd_sizes`**
    *   `herd_number` (INT, FK -> `herds.herd_number`)
    *   `size_date` (DATE, PK component)
    *   `size_description` (TEXT, PK component)
    *   `count` (INT)
*   **`animal_movements`**
    *   `movement_id` (TEXT, PK - Generated UUID v4)
    *   `reporting_herd_number` (INT, FK -> `herds.herd_number`)
    *   `movement_date` (DATE)
    *   `contact_type` (TEXT) - 'Til' or 'Fra'
    *   `counterparty_chr_number` (INT)
    *   `counterparty_herd_number` (INT)
    *   `counterparty_business_type` (TEXT)
*   **`property_vet_events`**
    *   `event_id` (TEXT, PK - Generated UUID v4)
    *   `chr_number` (INT, FK -> `properties.chr_number`)
    *   `species_code` (INT, FK -> `species.species_code`)
    *   `disease_code` (TEXT, FK -> `diseases.disease_code`)
    *   `vet_status_code` (TEXT, FK -> `vet_statuses.vet_status_code`)
    *   `disease_level_code` (TEXT, nullable)
    *   `vet_status_date` (DATE)
    *   `remark` (TEXT, nullable)
    *   `has_vet_problems` (BOOLEAN) - From parent level in source JSON
*   **`antibiotic_usage`**
    *   `usage_id` (TEXT, PK - Generated UUID v4)
    *   `cvr_number` (TEXT, FK -> `entities.cvr_number`) - **Note:** Linking via CVR here assumes VetStat always provides CVR. We might need to look up `entity_id` based on CVR instead.
    *   `chr_number` (INT, FK -> `properties.chr_number`)
    *   `year` (INT)
    *   `month` (INT)
    *   `species_code` (INT, FK -> `species.species_code`)
    *   `age_group_code` (INT, FK -> `age_groups.age_group_code`)
    *   `avg_usage_rolling_9m` (FLOAT)
    *   `avg_usage_rolling_12m` (FLOAT)
    *   `animal_days` (FLOAT) - Or DECIMAL for precision
    *   `animal_doses` (FLOAT) - Or DECIMAL for precision
    *   `add_per_100_dyr_per_dag` (FLOAT)
    *   `limit_value` (FLOAT)
    *   `municipality_code` (INT)
    *   `municipality_name` (TEXT)
    *   `region_code` (INT)
    *   `region_name` (TEXT)

## 4. Technology Stack

*   **Language:** Python 3.x
*   **Core Processing:** `ibis-framework[duckdb]` (for data manipulation and querying)
*   **Data Loading/Parsing:**
    *   `pandas` (potentially for initial JSON flattening/handling)
    *   `lxml` or `xml.etree.ElementTree` (for parsing `vetstat_antibiotics.xml`)
*   **File I/O:** `pyarrow` (for Parquet/GeoParquet)
*   **Geospatial:**
    *   `pyproj` (for CRS definitions/transformations)
    *   `shapely` or `geopandas` (for Point geometry creation, can also write GeoParquet)
    *   DuckDB Spatial Extension (alternative for geometry creation/handling within Ibis/DuckDB)
*   **Utilities:** `uuid` (for generating UUIDs)

## 5. Processing Steps

1.  **Setup:**
    *   Create Python script: `backend/pipelines/chr_silver_processing.py`.
    *   Import necessary libraries.
    *   Define input directory (bronze data path) and output directory (silver data path, including processing date).
    *   Establish DuckDB connection via Ibis.
2.  **Load Bronze Data:**
    *   Read all `.json` files into memory (e.g., using `json.load` or `pandas.read_json`).
    *   Read `vetstat_antibiotics.xml`. Split the content by `<!-- RAW_RESPONSE_SEPARATOR -->`. Parse each resulting XML chunk (e.g., using `lxml.etree.fromstring`) to extract data from `<ns2:Data>` elements within each SOAP response. Store extracted data (e.g., list of dictionaries or Pandas DataFrame).
3.  **Create & Populate Lookup Tables:**
    *   Iterate through the loaded data (`besaetning_details.json`, `ejendom_vet_events.json`, `vetstat_antibiotics.xml`).
    *   Extract unique code/text pairs for `species`, `usage_types`, `trade_statuses`, `diseases`, `vet_statuses`, `age_groups`.
    *   Create Ibis tables/DataFrames for each lookup table.
    *   Persist lookup tables as Parquet files.
4.  **Create & Populate `entities` Table:**
    *   Gather all unique combinations of `CvrNummer`, `CprNummer`, `Navn`, `Adresse`, etc., from `besaetning_details.json` (`Ejer`, `Bruger`) and `vetstat_antibiotics.xml` (`ns2:CVRNummer` and associated fields).
    *   For each unique entity (identified by CVR/CPR), generate a `uuid.uuid4().hex` as `entity_id`.
    *   Store `cvr_number`, `cpr_number` (handling nulls), and other details.
    *   Clean text fields (strip whitespace).
    *   Map boolean flags (`Reklamebeskyttelse`, `Adressebeskyttelse`).
    *   Persist as Parquet file. Maintain a mapping (e.g., dict) of `(cvr, cpr)` tuples to `entity_id` for later FK assignment.
5.  **Create & Populate `vet_practices` Table:**
    *   Extract unique practice details from `besaetning_details.json` (`BesPraksis`).
    *   Use `PraksisNr` as the primary key.
    *   Clean text fields.
    *   Persist as Parquet file.
6.  **Create & Populate `properties` Table:**
    *   Process `ejendom_oplysninger.json`.
    *   Use `ChrNummer` as the primary key.
    *   Extract address, administrative info.
    *   Create Point geometry from `StaldKoordinatX`, `StaldKoordinatY`. Assume source CRS (e.g., EPSG:25832), transform to EPSG:4326. Store original coords and CRS name. Validate geometry.
    *   Cast types (dates).
    *   Persist as **GeoParquet** file.
7.  **Create & Populate `herds` Table:**
    *   Process `besaetning_details.json`.
    *   Use `BesaetningsNummer` as the primary key.
    *   Assign Foreign Keys:
        *   `chr_number` (link to `properties`).
        *   `species_code`, `usage_code`, `trade_code` (link to lookup tables).
        *   `owner_entity_id`, `user_entity_id` (look up `entity_id` from the mapping created in step 4 based on owner/user CVR/CPR).
        *   `vet_practice_number` (link to `vet_practices`).
    *   Map `Oekologisk` to boolean `is_organic`.
    *   Cast types (dates).
    *   Persist as Parquet file.
8.  **Create & Populate `herd_sizes` Table:**
    *   Process `besaetning_details.json`, iterating through the `BesStr` list for each herd.
    *   Assign `herd_number` FK. Use `BesStrDatoAjourfoert` as `size_date`.
    *   Use `size_date` and `BesaetningsStoerrelseTekst` as composite PK.
    *   Persist as Parquet file.
9.  **Create & Populate `animal_movements` Table:**
    *   Process `diko_flytninger.json`, iterating through the `Flytninger` list.
    *   Generate `movement_id` (UUID).
    *   Assign `reporting_herd_number` FK.
    *   Cast types (dates).
    *   Persist as Parquet file.
10. **Create & Populate `property_vet_events` Table:**
    *   Process `ejendom_vet_events.json`, iterating through `VeterinaerHaendelse`.
    *   Generate `event_id` (UUID).
    *   Assign FKs: `chr_number`, `species_code`, `disease_code`, `vet_status_code`.
    *   Extract `has_vet_problems` boolean from the parent level.
    *   Cast types (dates).
    *   Persist as Parquet file.
11. **Create & Populate `antibiotic_usage` Table:**
    *   Process the parsed VetStat data.
    *   Generate `usage_id` (UUID).
    *   Assign FKs: `chr_number`, `species_code`, `age_group_code`.
    *   Assign `cvr_number` (or look up `entity_id` based on CVR if preferred).
    *   Cast types (numerics - consider DECIMAL for `animal_days`, `animal_doses`).
    *   Persist as Parquet file.
12. **Data Cleaning & Validation (Ongoing):**
    *   Apply consistently during steps 3-11.
    *   Standardize text casing (lowercase for codes/enums?).
    *   Strip leading/trailing whitespace from text fields.
    *   Handle null values appropriately (use `None`, `math.nan`, or Ibis/Pandas equivalents).
    *   Ensure correct data types (int, float, boolean, date, geometry).
    *   Validate geometries (e.g., check if coordinates are within expected bounds for Denmark).
13. **Output:**
    *   Save each final Ibis table/DataFrame to the specified silver directory (`backend/data/silver/chr/<processing_date>/`) using `.to_parquet()` or `geopandas.to_parquet()` (for `properties`). Use Snappy compression.

## 6. PII Handling Note

As per user request, this plan includes storing raw `cpr_number` values found in the source data within the `entities` table. This deviates from the general guideline in `backend/README.md` which suggests removing or replacing PII like CPR numbers with UUIDs.

**Implications:**
*   The silver layer `entities` table will contain sensitive personal data.
*   Downstream access controls and anonymization steps (potentially in the Gold layer or API layer) will be crucial if this data is to be shared or used more widely.

Consider adding a boolean flag column like `has_cpr` to the `entities` table to easily identify records containing this sensitive data.

## 7. Output Location

`backend/data/silver/chr/YYYYMMDD_HHMMSS/` (where timestamp reflects processing time) containing files like:

*   `species.parquet`
*   `entities.parquet`
*   `properties.geoparquet`
*   `herds.parquet`
*   ...etc.

## 8. Next Steps

*   Review and confirm this plan.
*   Begin implementing the Python script `backend/pipelines/chr_silver_processing.py` following these steps.
*   Develop helper functions for common tasks (data loading, cleaning, geometry creation, XML parsing).
*   Add logging throughout the script.
*   Implement basic tests for key transformation logic.

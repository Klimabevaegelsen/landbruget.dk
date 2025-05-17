# Pipelines 
This backend infrastructure powers our open-source data processing platform, designed to transform raw data into clean, harmonized, and analysis-ready datasets. We follow a structured medallion architecture to ensure data quality, maintainability, and ease of contribution. This documentation outlines our technical stack, coding standards, and pipeline architecture to help contributors understand our approach and contribute effectively.

Programming language used in the backend is Python. SQL may be used to execute transform operations - in that case please ensure the python equivalent is logged (we recommend using [sql_to_ibis](https://github.com/zbrookle/sql_to_ibis) for that)

## Stack

Pipelines run through will be mostly run on Github workflows and actions.

We use a [medallion architecture](https://www.databricks.com/glossary/medallion-architecture) for our data storage and pipeline infrastructure.

Bronze and Silver layer processes may run in the same pipeline. Gold layer processes will run in separate pipelines.

## Overall
- Consider whether we should fetch and store the data or whether we can provide a link in the frontend or fetch the data at runtime.
- Do not share identifiers and credentials in commits.
- If you're concerned whether your data contains PII data, reach out to the maintainers before committing data.
- Data should in the dev environment be saved and processed locally, ideally using DuckDB and Ibis.
- Data should in the prod environment be saved on GCS for Bronze layer, GCS for Silver and Supabase for Gold layer, ideally using DuckDB and Ibis.
- CRS conversions of geospatial data should be avoided (as it may changes the data).
- Utilise optimal Github actions  ensuring separation of responsibility for modules.
- Do add comments in the code and do add logs for steps within each module.
- Each step module should have its own tests.


## LLMs 
- If LLMs are used in your pipeline, your code should use [OpenRouter](https://openrouter.ai/) instead of querying the LLM APIs directly.
- Please test output consistency.
- Be mindful of costs and test different models to check if the cheapest models can be sufficient.
- Be mindful of sending PII to LLM APIs. If you do, be mindful of which provider you use.

## Lint and Types
We expect (soon) to use [Pydantic](https://docs.pydantic.dev/latest/) and [Ruff](https://github.com/astral-sh/ruff) to keep things nice and clean.
  
## Bronze layer
- The Bronze layer's purpose is to fetch and store the raw data.
- Data should be captured raw and stored immediately - **crucially, NO transformation or cleaning should occur at this stage**. The goal is to preserve the data exactly as it arrived from the source.
- If source allows different output formats, Parquet/GeoParquet should be preferred, followed by JSON/GeoJSON. If the data is geospatial, the preferred CRS should be EPSG:4326 and then EPSG:25832.
- Each Bronze layer dataset should be stored under its own folder in the Raw folder with a folder name in English that clearly reflects the data content, and a date for the data fetch.
- Each Bronze layer dataset should be stored in GCS in prod, include some metadata about the data source, incl. provenance, whether it requires a recurrent request for access, etc.

## Silver layer
- The Silver layer process' purpose is to clean and harmonise the raw data.
- Silver layer data should not depend on other existing data sources (that happens in the Gold layer stage).

### Processing
[DuckDB](https://duckdb.org/docs/stable/) and [Ibis](https://ibis-project.org/) should be preferred to process the data, instead of shapely, pandas and geopandas (where possible).

### Storage
- In prod environment, Silver layer data should be kept in prod in GCS.
- In dev environment, it should be stored locally in Parquet/Geoparquet.
- Nested data should be kept in separate tables.
- Each Silver layer dataset should be stored under its own folder in the Processed folder with a folder name in English that clearly reflects the data content, and a date for the processing.
- In the future, a slice of the data (most probably a single municipality) should be saved separately, so contributers can help out on the Gold layer pipelines without having to have access to the raw data source or download the entire dataset.

### Naming conventions
- In English 
- File names: lowercase and underscore, max 5 words.
- Feature names: lowercase and underscore, max 5 words.
- Geospatial field names should start with "geo_".
- Common feature names: 
- CVR Number: cvr_number
- Geometry fields: geometry
- CHR Number: chr_number
- Herd Number: herd_number
- Kommune: municipality
- Dyrearttekst: species_name
- År: year
- Marknummber: field_id
- Markbloknummer: field_block_id

### Data values
- Null values should be ... null.
- Consider enums instead of text (where relevant).

### Geospatial data
- Geospatial data should be stored with EPSG:4326.
- Geometries should be validated. Common validations can be found [here](https://github.com/chrieke/geojson-invalid-geometry) and should be available in most libraries.
- Grid cell geometries should be dissolved when relevant.
- If separate but similar datasets are unlikely to be used individually and where there is a high chance of geographic overlap (water projects for example), feel free save a dissolved version(s) of the datasets as well.
- Index the data (here's a guide to do it with [DuckDB](https://cloudnativegeo.org/blog/2025/01/using-duckdbs-hilbert-function-with-geoparquet/))

### Types
- Ensure types are casted as they should be (for example column containing only numbers should be stored as numbers, not text).
- Dates should be stored as date and represent a calendar date (year, month and day) without any time-of-day information.
- Timestamps should be stored as datetime and represent a specific moment in time, including both the date and the time, including the year, month, day, hour, minute, second and microsecond.
- Ensure æ,ø,å and other funky characters etc are stored properly.
- Booleans should be stored as 1s and 0s.
- Lat/lon coordinates (and all geospatial data) should be stored as a geometry features, not text.

### PII
- Silver layer data should be expected to be publicly accessible.
- Some datasets include PII (beginning of CPR numbers or unique personal ids for parcel owners). These should be changed to unique UUIDv4 - or removed.
- Marketing protection notices should be kept.

## (WIP) Gold layer
- If a process uncovers the identity of anonymised individuals or companies through the cross-matching of datasets, reach out to the maintainers before committing data.
- Data should be fetched from GCS.

# (WIP) APIs
- We use Supabase

 

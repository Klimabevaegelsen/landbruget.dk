# Schema

Machine-readable data catalog and relationship documentation for the Landbruget.dk database.

## Files

### `data_catalog.json`
Machine-readable catalog of all datasets in the project. Contains 183 datasets with metadata including:
- Dataset name, display name, and medallion layer (bronze/silver/gold)
- R2 download URL for each parquet file
- Row count, file size in bytes, and column count
- AI-generated descriptions for each dataset and its columns

Generated automatically. Used by the data explorer frontend to enable browsing and querying datasets.

### `relationships.md`
Documents how tables in the database relate to each other, enabling cross-table joins. Covers:
- **Core identifiers**: CVR (company, 8 digits), CHR (herd, 6 digits), BFE (cadastral parcel), Field UUID, Company UUID
- **Type safety**: CVR is stored as string in some tables and integer in others -- always use `TRY_CAST` when joining
- **Join patterns**: Which tables share which identifiers
- **UUID generation**: Company UUIDs are generated deterministically from CVR via `landbrugsdata_company_uuid()`

### `example_queries.md`
DuckDB SQL query examples organized by category:
- Basic data exploration (company counts, fields per municipality)
- Company analysis
- Livestock and animal health
- Pesticide usage
- Geographic and environmental analysis
- Temporal analysis and advanced aggregations

Queries include both Danish and English descriptions of what each query answers.

## Core Identifiers

All data in the project is joinable through these standardized identifiers (see `relationships.md` for full details):

| Identifier | Format | Purpose |
|------------|--------|---------|
| CVR | 8 digits | Company identification |
| CHR | 6 digits | Livestock herd/production site |
| BFE | Variable | Cadastral land parcel |
| Field UUID | UUID | Agricultural field (deterministic from geometry) |
| Company UUID | UUID | Internal company key (deterministic from CVR) |

## Using the Catalog

Query `data_catalog.json` to find datasets programmatically:

```python
import json

with open("schema/data_catalog.json") as f:
    catalog = json.load(f)

# Find all silver-layer datasets
silver = [d for d in catalog["datasets"] if d["layer"] == "silver"]

# Find datasets with a specific column
has_cvr = [d for d in catalog["datasets"]
           if "cvr_number" in d.get("columnDescriptions", {})]
```

Each dataset entry includes a `url` field pointing to the parquet file on R2, which can be queried directly with DuckDB:

```sql
SELECT * FROM 'https://pub-....r2.dev/silver/companies/data.parquet' LIMIT 10;
```

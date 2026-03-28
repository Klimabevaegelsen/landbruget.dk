# Backend Scripts

Utility scripts for backend operations.

## generate_schema_docs.py

Auto-generates schema documentation from Parquet files for Gemini File Search and human consumption.

### Purpose

This script reads Parquet files from cloud storage or local cache and generates comprehensive markdown documentation including:

- **schema/tables.md**: List of all tables with row counts, columns, and metadata
- **schema/columns.md**: Detailed column-level information with types, nullable status, and statistics

### Requirements

```bash
pip install pyarrow duckdb
```

### Usage

**From cloud storage (requires authentication):**

```bash
# Generate docs from default gold/silver layers
python generate_schema_docs.py

# Specify custom cloud storage paths
python generate_schema_docs.py --gcs-path "gs://landbruget-data/gold/**" --gcs-path "gs://landbruget-data/silver/**"

# Custom output directory
python generate_schema_docs.py --output-dir docs/schema
```

**From local cache:**

```bash
# Use local Parquet files
python generate_schema_docs.py --local-cache /path/to/parquet/files

# Multiple local paths
python generate_schema_docs.py --local-path /path/to/gold --local-path /path/to/silver
```

### Environment Variables

```bash
# Cloud storage authentication (HMAC preferred)
export GCS_BUCKET=landbruget-data
export GCS_ACCESS_KEY_ID=<your-access-key>
export GCS_SECRET_ACCESS_KEY=<your-secret-key>

# Alternative: Service account
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--local-cache PATH` | Local cache directory with Parquet files | None |
| `--bucket BUCKET` | Storage bucket name | `GCS_BUCKET` env var |
| `--output-dir DIR` | Output directory for markdown files | `schema` |
| `--gcs-path PATTERN` | Cloud storage path pattern to search (can be repeated) | `gs://{bucket}/gold`, `gs://{bucket}/silver` |
| `--local-path PATH` | Local path to search (can be repeated) | None |

### Output Format

**schema/tables.md:**
```markdown
# Landbruget.dk Data Tables

| Table Name | Row Count | Columns | Last Updated |
|------------|-----------|---------|--------------|
| field_data | 1,234,567 | 25 | 2024-01-15 |
| ...

### field_data

**File:** `gs://bucket/gold/field_data/data.parquet`
**Row Count:** 1,234,567
**Columns:** 25
```

**schema/columns.md:**
```markdown
## field_data

**Row Count:** 1,234,567

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| cvr | string | No | Company registration number |
| area_ha | double | No | Field area in hectares |
| ...

### Column Statistics

| Column | Min | Max | Unique | Avg | Null % |
|--------|-----|-----|--------|-----|--------|
| area_ha | 0.1 | 1250.5 | 125000 | 15.4 | 0.0% |
```

### Integration with Pipelines

This script follows the patterns established in:
- `/backend/common/duckdb_processor.py` - DuckDB setup and operations
- `/backend/common/storage_interface.py` - Cloud storage access patterns
- `/backend/common/schema_documentation.py` - Schema documentation utilities

### Examples

**Generate docs for unified pipeline output:**

```bash
python generate_schema_docs.py \
  --gcs-path "gs://landbruget-data/gold/**" \
  --output-dir docs/schema/unified_pipeline
```

**Generate docs from local development cache:**

```bash
python generate_schema_docs.py \
  --local-cache ~/data/landbruget/parquet \
  --output-dir schema
```

**Generate docs for specific tables only:**

```bash
python generate_schema_docs.py \
  --gcs-path "gs://landbruget-data/gold/field_data/**" \
  --gcs-path "gs://landbruget-data/gold/chr_data/**"
```

### Error Handling

The script handles common errors gracefully:

- **No cloud credentials**: Falls back to local-only mode
- **Missing files**: Skips with warning and continues
- **Invalid Parquet files**: Logs error but continues processing other files
- **DuckDB statistics failure**: Skips statistics but includes basic schema

### Performance Notes

- Uses PyArrow for fast Parquet metadata reading
- DuckDB for efficient statistics generation
- Processes files in sequence to avoid memory issues
- Suitable for hundreds of tables with millions of rows

### Maintenance

**To update script:**
1. Edit `backend/scripts/generate_schema_docs.py`
2. Test with local cache first
3. Run full test with cloud storage access
4. Update this README if CLI options change

**Common issues:**

- **"No Parquet files found"**: Check cloud storage path and credentials
- **"Could not load httpfs extension"**: Install DuckDB with extensions
- **Memory errors**: Process fewer files at once or use local cache

---

## upload_schema.py

Upload schema documentation markdown files to Gemini File Search Store for enhanced LLM queries.

### Purpose

This script uploads schema documentation (tables.md, columns.md, relationships.md, example_queries.md) to a Gemini File Search Store, enabling Gemini models to access comprehensive database schema information for better query generation and analysis.

### Requirements

```bash
pip install google-genai
```

### Usage

**Create new store:**

```bash
# Create a new File Search Store
python upload_schema.py --create-store

# Create with custom name
python upload_schema.py --create-store --store-name "Production Schema Docs"
```

**Upload to existing store:**

```bash
# Upload using store ID from environment
python upload_schema.py --store-id <store-id>

# Or set in .env file and run
export GEMINI_FILE_SEARCH_STORE_ID=<store-id>
python upload_schema.py
```

**Dry run (test without uploading):**

```bash
# Show what would be uploaded
python upload_schema.py --store-id <store-id> --dry-run
```

**Advanced options:**

```bash
# Upload without deleting old files first
python upload_schema.py --store-id <store-id> --no-clean

# Use custom schema directory
python upload_schema.py --store-id <store-id> --schema-dir docs/schema

# Verbose logging
python upload_schema.py --store-id <store-id> --verbose
```

### Environment Variables

```bash
# Required
export GOOGLE_API_KEY=<your-gemini-api-key>

# Optional (can also use --store-id)
export GEMINI_FILE_SEARCH_STORE_ID=<store-id>
```

### Command-Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--create-store` | Create a new File Search Store | N/A |
| `--store-id ID` | Existing store ID to upload to | `GEMINI_FILE_SEARCH_STORE_ID` env var |
| `--store-name NAME` | Display name for new store | "Landbruget.dk Schema Documentation" |
| `--schema-dir DIR` | Directory containing schema markdown files | `schema` |
| `--dry-run` | Show what would be uploaded without uploading | False |
| `--no-clean` | Don't delete old files before uploading | False (clean by default) |
| `--verbose`, `-v` | Show detailed progress | False |

### Workflow

1. **Generate schema docs** (run first):
   ```bash
   python generate_schema_docs.py --output-dir schema
   ```

2. **Create Gemini store** (one-time):
   ```bash
   python upload_schema.py --create-store
   # Copy the store ID to your .env file
   ```

3. **Upload schema docs** (after regeneration):
   ```bash
   python upload_schema.py --store-id <store-id>
   ```

### Schema Files Uploaded

By default, the script uploads these files from the `schema/` directory:

- **tables.md** - List of all tables with row counts and columns
- **columns.md** - Detailed column information with types and statistics
- **relationships.md** - Foreign key relationships and join patterns
- **example_queries.md** - Common query examples and patterns

Any additional `.md` files in the schema directory will also be uploaded.

### File Management

- **Clean by default**: Old files are deleted before uploading fresh versions
- **No duplicates**: Ensures the store only contains the latest schema documentation
- **Incremental**: Can upload without cleaning using `--no-clean` flag

### Output Format

```
============================================================
Uploading schema documentation
Store ID: abc123
Schema directory: schema
============================================================

Listing files in store: abc123
Found 4 files in store
Deleting: tables.md (documents/abc123-doc1)
Deleting: columns.md (documents/abc123-doc2)
✅ Deleted 4 files

Found 4 files to upload:
  - schema/tables.md
  - schema/columns.md
  - schema/relationships.md
  - schema/example_queries.md

Uploading: tables.md (45.2 KB)
✅ Uploaded: tables.md
Uploading: columns.md (123.8 KB)
✅ Uploaded: columns.md

============================================================
Upload completed in 12.3 seconds
Files uploaded: 4
Files deleted: 4
Files skipped: 0
Errors: 0
============================================================
```

### Integration with Gemini API

After uploading, use the store ID in Gemini API requests:

```python
from google import genai

client = genai.Client(api_key="your-api-key")

# Query with file search
response = client.models.generate_content(
    model="gemini-2.0-flash-exp",
    contents="Show me all tables with CVR columns",
    config={
        "tools": [
            {
                "file_search": {
                    "corpus": f"corpora/{store_id}"
                }
            }
        ]
    }
)
```

### Error Handling

The script handles common errors gracefully:

- **Missing API key**: Clear error message with setup instructions
- **No store ID**: Prompts to create new store or specify ID
- **Missing files**: Skips with warning and continues
- **Upload failures**: Logs error but continues with remaining files
- **Invalid store ID**: Clear error message from API

### Performance Notes

- Uses Google GenAI SDK (NOT google-generativeai)
- Uploads files sequentially to avoid rate limits
- Cleans old files before uploading to prevent duplicates
- Suitable for schema docs up to several hundred MB total
- Model used: `gemini-2.0-flash-exp` (latest flash model)

### Best Practices

1. **Regenerate schema docs regularly**: Run `generate_schema_docs.py` weekly or after schema changes
2. **Keep store clean**: Let the script delete old files (default behavior)
3. **Use dry-run first**: Test with `--dry-run` before uploading to production
4. **Store ID in .env**: Set `GEMINI_FILE_SEARCH_STORE_ID` to avoid passing `--store-id` every time
5. **Version control**: Don't commit `.env` file with API keys

### Troubleshooting

**"Missing required environment variable: GOOGLE_API_KEY"**
- Set your Gemini API key in `.env` file or environment
- Get API key from: https://aistudio.google.com/app/apikey

**"No store ID provided"**
- Either run with `--create-store` to create new store
- Or use `--store-id <id>` to specify existing store
- Or set `GEMINI_FILE_SEARCH_STORE_ID` in `.env` file

**"ERROR: google-genai package not installed"**
- Install with: `pip install google-genai`
- Note: Use `google-genai`, NOT `google-generativeai`

**"No schema files found to upload"**
- Run `generate_schema_docs.py` first to create schema documentation
- Or specify custom schema directory with `--schema-dir`

### Maintenance

**To update script:**
1. Edit `backend/scripts/upload_schema.py`
2. Test with `--dry-run` flag
3. Update this README if CLI options change

**Recommended schedule:**
- Generate schema docs: Weekly or after schema changes
- Upload to Gemini: After each schema doc generation
- Clean old files: Automatic (default behavior)

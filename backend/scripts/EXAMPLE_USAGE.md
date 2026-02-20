# Schema Documentation Scripts - Example Usage

This document provides practical examples of using `generate_schema_docs.py` and `upload_schema.py` in different scenarios.

## Quick Start

### 1. Generate Documentation from Local Parquet Files

If you have Parquet files locally (e.g., from pipeline development):

```bash
# Create a test Parquet file
cd backend/scripts
python3 << 'EOF'
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

# Create sample data
data = {
    'cvr': ['12345678', '87654321', '11111111'],
    'company_name': ['Test Farm A/S', 'Agriculture Inc', 'Green Fields'],
    'area_ha': [150.5, 320.8, 85.2],
    'crop_type': ['wheat', 'barley', 'wheat'],
    'year': [2024, 2024, 2024]
}
df = pd.DataFrame(data)

# Create output directory
Path('test_data/gold').mkdir(parents=True, exist_ok=True)

# Save as Parquet with metadata
table = pa.Table.from_pandas(df)
metadata = {
    b'description': b'Sample agricultural data',
    b'source': b'test_generator',
    b'created': b'2024-01-01'
}
table = table.replace_schema_metadata(metadata)

pq.write_table(table, 'test_data/gold/farm_data.parquet')
print("✅ Created test_data/gold/farm_data.parquet")
EOF

# Generate schema documentation
python generate_schema_docs.py --local-path test_data/gold --output-dir schema_output

# View results
ls -la schema_output/
cat schema_output/tables.md
cat schema_output/columns.md

# Clean up
rm -rf test_data schema_output
```

### 2. Generate Documentation from GCS

If you have access to the production GCS bucket:

```bash
# Set up environment
export GCS_BUCKET=landbruget-data
export GCS_ACCESS_KEY_ID=your_access_key
export GCS_SECRET_ACCESS_KEY=your_secret_key

# Generate docs from default locations (gold and silver layers)
python generate_schema_docs.py --output-dir docs/schema

# Or specify exact paths
python generate_schema_docs.py \
  --gcs-path "gs://landbruget-data/gold/field_data/**" \
  --gcs-path "gs://landbruget-data/gold/chr_data/**" \
  --output-dir docs/schema/selected_tables
```

### 3. Integration with Pipeline Development

When developing a new pipeline:

```bash
# After running your pipeline locally with Parquet output
cd backend/pipelines/my_new_pipeline

# Run the pipeline
python main.py

# Generate documentation for your output
python ../../scripts/generate_schema_docs.py \
  --local-path ./output \
  --output-dir ./docs/schema

# Review the generated docs
cat docs/schema/tables.md
```

## Advanced Usage

### Multiple Data Sources

Combine local cache with specific GCS tables:

```bash
python generate_schema_docs.py \
  --local-cache ~/data/landbruget/cache \
  --gcs-path "gs://landbruget-data/gold/latest/**" \
  --output-dir combined_schema
```

### Custom Output Location

Save documentation in a specific location:

```bash
python generate_schema_docs.py \
  --local-cache ./data \
  --output-dir ./documentation/database_schema
```

### Pipeline-Specific Documentation

Generate docs for a specific pipeline's output:

```bash
# Unified pipeline
python generate_schema_docs.py \
  --gcs-path "gs://landbruget-data/gold/**" \
  --output-dir docs/schemas/unified_pipeline

# CHR pipeline
python generate_schema_docs.py \
  --gcs-path "gs://landbruget-data/chr/gold/**" \
  --output-dir docs/schemas/chr_pipeline
```

## Integration with GitHub Actions

Example workflow for automatic schema documentation updates:

```yaml
name: Update Schema Documentation

on:
  schedule:
    - cron: '0 2 * * 1'  # Weekly on Monday at 2 AM
  workflow_dispatch:

jobs:
  generate-docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install pyarrow duckdb

      - name: Generate schema documentation
        env:
          GCS_BUCKET: ${{ secrets.GCS_BUCKET }}
          GCS_ACCESS_KEY_ID: ${{ secrets.GCS_ACCESS_KEY_ID }}
          GCS_SECRET_ACCESS_KEY: ${{ secrets.GCS_SECRET_ACCESS_KEY }}
        run: |
          cd backend/scripts
          python generate_schema_docs.py --output-dir ../../docs/schema

      - name: Commit documentation
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add docs/schema/
          git commit -m "docs: update schema documentation [automated]" || echo "No changes"
          git push
```

## Troubleshooting

### Issue: "No Parquet files found"

**Solution:** Check your paths and credentials

```bash
# Verify GCS credentials
gcloud auth application-default print-access-token

# Test GCS access
gsutil ls gs://landbruget-data/gold/

# Use local cache instead
python generate_schema_docs.py --local-cache ./data
```

### Issue: "Could not load httpfs extension"

**Solution:** Reinstall DuckDB with extensions

```bash
pip install --upgrade duckdb
python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"
```

### Issue: Memory errors with large files

**Solution:** Process files in batches

```bash
# Process only specific tables
python generate_schema_docs.py \
  --gcs-path "gs://landbruget-data/gold/small_table/**" \
  --output-dir schema_batch_1

python generate_schema_docs.py \
  --gcs-path "gs://landbruget-data/gold/medium_table/**" \
  --output-dir schema_batch_2
```

## Expected Output Structure

After running the script, you'll have:

```
schema/
├── tables.md       # Summary of all tables
├── columns.md      # Detailed column information
└── [optional files generated by future versions]
```

## Next Steps

1. **Review generated documentation** - Check tables.md and columns.md
2. **Upload to documentation system** - Share with team or upload to wiki
3. **Integrate with Gemini** - Use for AI-powered data exploration
4. **Automate updates** - Set up scheduled runs via GitHub Actions

## Real-World Example

Here's a complete example from pipeline development to documentation:

```bash
# 1. Develop a new pipeline
cd backend/pipelines/my_pipeline
python main.py

# 2. Pipeline outputs Parquet to local directory
ls -la output/gold/
# -> farm_summary.parquet (125 MB, 1.2M rows)
# -> crop_statistics.parquet (85 MB, 850K rows)

# 3. Generate documentation
cd ../..
python scripts/generate_schema_docs.py \
  --local-path pipelines/my_pipeline/output/gold \
  --output-dir docs/schema/my_pipeline

# 4. Review results
cat docs/schema/my_pipeline/tables.md
# Shows: 2 tables, 2M total rows, 45 columns

# 5. Share with team
git add docs/schema/my_pipeline/
git commit -m "docs: add schema documentation for my_pipeline"
git push
```

---

## Upload Schema to Gemini File Search

### Complete Workflow: Generate and Upload

```bash
cd backend/scripts

# 1. Generate schema documentation
python generate_schema_docs.py --output-dir schema

# 2. Create Gemini store (one-time setup)
export GOOGLE_API_KEY=your_gemini_api_key
python upload_schema.py --create-store

# Output shows store ID, add it to .env:
# GEMINI_FILE_SEARCH_STORE_ID=abc123xyz

# 3. Upload schema files to Gemini
python upload_schema.py --store-id abc123xyz

# Or use environment variable
export GEMINI_FILE_SEARCH_STORE_ID=abc123xyz
python upload_schema.py
```

### Test Before Uploading (Dry Run)

```bash
# Show what would be uploaded without actually uploading
python upload_schema.py --store-id abc123xyz --dry-run
```

Output:
```
============================================================
[DRY RUN] Uploading schema documentation
Store ID: abc123xyz
Schema directory: schema
============================================================

Listing files in store: abc123xyz
Found 2 files in store
[DRY RUN] Would delete: tables.md (documents/abc123xyz-doc1)
[DRY RUN] Would delete: columns.md (documents/abc123xyz-doc2)
[DRY RUN] Would delete 2 files

Found 4 files to upload:
  - schema/tables.md
  - schema/columns.md
  - schema/relationships.md
  - schema/example_queries.md

[DRY RUN] Would upload: tables.md (45.2 KB)
[DRY RUN] Would upload: columns.md (123.8 KB)
[DRY RUN] Would upload: relationships.md (15.3 KB)
[DRY RUN] Would upload: example_queries.md (8.7 KB)

============================================================
[DRY RUN] Upload completed in 2.1 seconds
Files uploaded: 0
Files deleted: 0
Files skipped: 0
Errors: 0
============================================================
```

### Upload Without Cleaning Old Files

```bash
# Keep old files and just add new ones
python upload_schema.py --store-id abc123xyz --no-clean
```

### Custom Schema Directory

```bash
# Upload from non-standard location
python upload_schema.py \
  --store-id abc123xyz \
  --schema-dir docs/database/schema
```

### Automated Weekly Update

Set up automated schema documentation and upload:

```bash
#!/bin/bash
# weekly-schema-update.sh

set -e

cd backend/scripts

# 1. Generate fresh schema docs
echo "Generating schema documentation..."
python generate_schema_docs.py --output-dir schema

# 2. Upload to Gemini
echo "Uploading to Gemini..."
python upload_schema.py

echo "✅ Schema documentation updated successfully"
```

Add to crontab:
```bash
# Run every Monday at 3 AM
0 3 * * 1 /path/to/weekly-schema-update.sh
```

### Use with Gemini API

After uploading, query your schema with Gemini:

```python
from google import genai

# Initialize client
client = genai.Client(api_key="your-api-key")
store_id = "abc123xyz"  # Your store ID

# Query schema information
response = client.models.generate_content(
    model="gemini-2.0-flash-exp",
    contents="What tables contain CVR numbers? Show me their column names.",
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

print(response.text)
```

Example queries you can ask:
- "Show me all tables with geospatial data"
- "What columns are available in the field_data table?"
- "Which tables can I join using CVR numbers?"
- "Give me example queries for accessing farm ownership data"

### Integration with CI/CD

Add to GitHub Actions workflow:

```yaml
name: Update Gemini Schema Store

on:
  schedule:
    - cron: '0 3 * * 1'  # Weekly on Monday at 3 AM
  workflow_dispatch:

jobs:
  update-schema:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install pyarrow duckdb google-genai

      - name: Generate schema documentation
        env:
          GCS_BUCKET: ${{ secrets.GCS_BUCKET }}
          GCS_ACCESS_KEY_ID: ${{ secrets.GCS_ACCESS_KEY_ID }}
          GCS_SECRET_ACCESS_KEY: ${{ secrets.GCS_SECRET_ACCESS_KEY }}
        run: |
          cd backend/scripts
          python generate_schema_docs.py --output-dir schema

      - name: Upload to Gemini
        env:
          GOOGLE_API_KEY: ${{ secrets.GOOGLE_API_KEY }}
          GEMINI_FILE_SEARCH_STORE_ID: ${{ secrets.GEMINI_FILE_SEARCH_STORE_ID }}
        run: |
          cd backend/scripts
          python upload_schema.py

      - name: Commit documentation
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add backend/scripts/schema/
          git commit -m "docs: update schema documentation [automated]" || echo "No changes"
          git push
```

### Troubleshooting Upload Issues

**"Missing required environment variable: GOOGLE_API_KEY"**

```bash
# Get API key from https://aistudio.google.com/app/apikey
export GOOGLE_API_KEY=your_key_here

# Add to .env file
echo "GOOGLE_API_KEY=your_key_here" >> backend/scripts/.env
```

**"No store ID provided"**

```bash
# Create new store
python upload_schema.py --create-store

# Or specify existing store
python upload_schema.py --store-id abc123xyz

# Or add to .env file
echo "GEMINI_FILE_SEARCH_STORE_ID=abc123xyz" >> backend/scripts/.env
```

**"ERROR: google-genai package not installed"**

```bash
# Install correct package (NOT google-generativeai)
pip install google-genai

# Verify installation
python -c "from google import genai; print('✅ google-genai installed')"
```

**"No schema files found to upload"**

```bash
# Generate schema docs first
python generate_schema_docs.py --output-dir schema

# Verify files exist
ls -la schema/
```

### Development Workflow

When developing new features that change the schema:

```bash
# 1. Run your pipeline locally
cd backend/pipelines/my_pipeline
python main.py

# 2. Generate updated schema docs
cd ../../scripts
python generate_schema_docs.py \
  --local-path ../pipelines/my_pipeline/output \
  --output-dir schema

# 3. Test upload with dry-run
python upload_schema.py --dry-run

# 4. Upload to development store
python upload_schema.py --store-id dev-store-id

# 5. Test queries with Gemini
python test_gemini_queries.py

# 6. Once verified, upload to production store
python upload_schema.py --store-id prod-store-id
```

## Questions?

If you encounter issues or have questions:
1. Check the [README.md](README.md) for detailed options
2. Review the [troubleshooting section](#troubleshooting) above
3. Check existing pipeline patterns in `/backend/common/`
4. Ask in the team channel or create a GitHub issue

# /run-pipeline - Execute Data Pipeline

Execute a data pipeline with proper environment setup and validation.

## Usage

```
/run-pipeline <pipeline-name>
```

Examples:
```
/run-pipeline unified_pipeline
/run-pipeline chr_pipeline
/run-pipeline svineflytning_pipeline
```

## Available Pipelines

| Pipeline | Purpose | Est. Time |
|----------|---------|-----------|
| `unified_pipeline` | 18+ Danish govt data sources | 30-60 min |
| `chr_pipeline` | Livestock tracking (CHR) | 10-20 min |
| `svineflytning_pipeline` | Pig movement tracking | 10-15 min |
| `drive_data_pipeline` | Regulatory compliance | 5-10 min |
| `bmd_scraper` | Pesticide database | 15-20 min |

## Process

### 1. Validate Environment

```bash
cd $CLAUDE_PROJECT_DIR/backend
source venv/bin/activate

# Check required env vars
echo "Checking environment..."
python -c "
import os
required = ['SUPABASE_URL', 'SUPABASE_KEY', 'GCS_BUCKET']
missing = [v for v in required if not os.getenv(v)]
if missing:
    print(f'Missing: {missing}')
    exit(1)
print('Environment OK')
"
```

### 2. Navigate to Pipeline

```bash
cd pipelines/$ARGUMENTS
```

### 3. Check Pipeline README

```bash
# Read pipeline-specific documentation
cat README.md
```

### 4. Run Pipeline

```bash
python main.py
```

### 5. Monitor Progress

Watch for:
- Bronze layer fetch progress
- Silver layer transformation logs
- Gold layer upload status
- Any errors or warnings

### 6. Validate Output

After completion:
```bash
# Check GCS for new files
gsutil ls gs://landbruget-data/bronze/$ARGUMENTS/

# Check Supabase for updated data
supabase db query "SELECT COUNT(*) FROM [table]"
```

## Output Format

```
## Pipeline Execution: $ARGUMENTS

### Environment
- ✅ Virtual environment active
- ✅ SUPABASE_URL configured
- ✅ SUPABASE_KEY configured
- ✅ GCS_BUCKET configured

### Execution Log
```
[timestamp] Starting Bronze layer...
[timestamp] Fetching from source: https://...
[timestamp] Bronze complete: 1,234 records
[timestamp] Starting Silver layer...
[timestamp] Transforming data...
[timestamp] CVR formatting: 1,234 → 1,230 valid
[timestamp] Silver complete: 1,230 records
[timestamp] Starting Gold layer...
[timestamp] Uploading to Supabase...
[timestamp] Gold complete
```

### Results
- **Records processed**: 1,234
- **Valid after cleaning**: 1,230 (99.7%)
- **Duration**: 12m 34s
- **GCS path**: `gs://landbruget-data/gold/$ARGUMENTS/[date]/`

### Data Quality
- CVR format valid: 1,230/1,230 ✅
- No duplicates found ✅
- Geospatial CRS: EPSG:4326 ✅
```

## Troubleshooting

### "Module not found"
```bash
cd $CLAUDE_PROJECT_DIR/backend
source venv/bin/activate
pip install -r requirements.txt
```

### GCS Authentication Error
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Supabase Connection Error
```bash
supabase status
supabase link --project-ref <ref>
```

### Memory Error
```python
# Use chunked processing
for chunk in pd.read_csv('large.csv', chunksize=10000):
    process(chunk)
```

## Pipeline Structure Reference

```
pipelines/<name>/
├── README.md          # Documentation
├── .env.example       # Required env vars
├── requirements.txt   # Dependencies
├── main.py           # Entry point
├── bronze/           # Raw data fetch
├── silver/           # Cleaning/transform
├── gold/             # Aggregation/upload
└── tests/            # Unit tests
```

## After Running

1. Verify data in Supabase Dashboard
2. Check GCS for archived files
3. Run data quality checks: `/validate-data`
4. Update pipeline documentation if needed

## Downstream Dependencies

After pipeline execution completes, check `pipeline_dependencies.yml` for downstream dependencies.

If the pipeline has downstream dependencies:
1. List them with their workflow files
2. Show the full cascade chain (transitive dependencies)
3. Offer to trigger them:
   ```bash
   gh workflow run <workflow>.yml --ref main
   ```

Example output after running unified_pipeline:
```
### Downstream Pipelines Affected
The following pipelines depend on unified_pipeline and should be re-run:

Direct:
  - field_area_analysis (field_area_analysis_multi_stage.yml)
  - field_production (field_production_matrix.yml)
  - pesticide_disaggregation (pesticide_disaggregation_matrix.yml)

Transitive:
  - pesticide_proximity → generate_pmtiles → pmtiles_cache_warmup
  - pesticide_compliance
  - h3_pfas_analysis → generate_pmtiles → pmtiles_cache_warmup

Trigger all downstream? (gh workflow run ...)
```

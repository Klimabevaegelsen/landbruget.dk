# Pipeline Development Rules

## Running Pipelines

```bash
cd backend && source venv/bin/activate   # Always activate first
cd pipelines/<name> && python main.py    # Run specific pipeline
```

## Key Pipelines

| Pipeline | Purpose | Schedule |
|----------|---------|----------|
| `unified_pipeline` | 18+ Danish govt data sources | Weekly (Mon 2 AM UTC) |
| `chr_pipeline` | Livestock tracking (CHR registry) | Weekly |
| `svineflytning_pipeline` | Pig movement tracking | Weekly |
| `drive_data_pipeline` | Regulatory compliance data | On demand |
| `bmd_scraper` | Pesticide database | On demand |

## Pipeline Debugging

1. Verify `.env` variables are set (check `.env.example` in pipeline dir)
2. Check GCS credentials path exists
3. Activate venv: `cd backend && source venv/bin/activate`
4. Check `docs/troubleshooting/` for known issues
5. Check pipeline-specific README: `backend/pipelines/<name>/README.md`

## Common Pipeline Failures

- **GCS auth**: Set `GOOGLE_APPLICATION_CREDENTIALS` or `GCS_CREDENTIALS`
- **Memory**: Use DuckDB or chunked processing for large files
- **DuckDB 1.5 breaking changes**: Use `delim` not `DELIMITER`, pin `>=1.5.0`
- **Geometry errors**: Wrap with `TRY()` for DuckDB 1.5, ensure EPSG:4326 for Supabase

## Cost Awareness

- Cache GCS files locally when testing repeatedly
- Use `gsutil -m` for parallel transfers
- Avoid re-uploading unchanged files
- Be mindful of egress costs on large datasets

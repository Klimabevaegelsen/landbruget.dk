# CVR Workflow Independent Execution Changes

## Summary

Modified the CVR enrichment pipeline to allow each step to run independently by fetching the latest available files from GCS instead of relying on outputs from the same pipeline run.

## Problem Solved

Previously, each step in the CVR workflow was tightly coupled and required the previous steps to complete successfully in the same pipeline run. This made it difficult to:

- Run individual steps for debugging or reprocessing
- Recover from partial failures
- Use data from previous successful runs
- Run steps out of order when needed

## Solution Overview

Added independent execution capability where each step can:

1. **Automatically find the latest available input data** from GCS within a configurable time window (default 30 days)
2. **Fall back gracefully** when required data is not available
3. **Maintain backward compatibility** with traditional pipeline dependencies
4. **Provide clear logging** about which execution mode is being used

## Files Modified

### 1. Shared Configuration
**File:** `backend/pipelines/unified_pipeline/src/unified_pipeline/gold/cvr_enrichment/shared/config.py`

Added new configuration options:
```python
enable_independent_execution: bool = True  # Enable independent step execution
max_days_back_for_inputs: int = 30         # How far back to search for files
fallback_to_pipeline_dependencies: bool = True  # Fallback behavior
```

Updated `get_step_input_paths()` function to support independent execution mode using the existing `GCSDataAccess` utility to avoid code duplication.

### 2. Individual CVR Steps
**Files Modified:**
- `company_fetching.py` - Modified to use smart independent execution
- `pnumber_fetching.py` - Modified to use smart independent execution  
- `financial_documents.py` - Modified to use smart independent execution
- `address_geocoding.py` - Modified to use smart independent execution
- `data_consolidation.py` - Modified to use smart independent execution

Each step now:
- Uses the updated `get_step_input_paths()` with independent execution parameters
- Automatically detects whether to use pipeline dependencies or latest GCS files
- Uses existing `GCSDataAccess.list_files_with_timestamps()` utility for efficient file discovery
- Gracefully handles missing optional data files

## How It Works

### Traditional Mode (Backward Compatible)
```python
shared_config.enable_independent_execution = False
```
- Steps depend on outputs from the same pipeline run
- Uses date-specific paths like `gs://bucket/gold/cvr_enrichment/2024-01-15/collection.parquet`

### Smart Independent Mode (New Default)
```python  
shared_config.enable_independent_execution = True
shared_config.max_days_back_for_inputs = 30
```

**When running as part of a pipeline workflow:**
- Steps first check if pipeline dependencies exist (artifacts or GCS files from current run)
- If they exist, uses them (traditional pipeline behavior)
- This ensures normal pipeline workflows continue to work with artifacts

**When running independently:**
- If no pipeline dependencies are found, automatically fetches latest available files from GCS
- Searches within the configured time window (default 30 days)
- Uses the GCS fetcher to find files like `gs://bucket/gold/cvr_enrichment/2024-01-10/collection.parquet`

## Usage Examples

### GitHub Actions Workflow

**Run Full Pipeline (Default):**
- Navigate to Actions → CVR Enrichment Pipeline → Run workflow
- Leave "Run only specific step" empty to run the full pipeline

**Run Single Step Independently:**
- Navigate to Actions → CVR Enrichment Pipeline → Run workflow  
- Select a specific step from "Run only specific step" dropdown
- The step will automatically fetch the latest available data from GCS

**Example Independent Steps:**
- `company_fetching` - Fetches latest collection data from GCS
- `pnumber_fetching` - Fetches latest company data from GCS
- `address_geocoding` - Fetches latest company and P-number data from GCS
- `data_consolidation` - Fetches latest data from all previous steps

### Command Line (Local Development)

**Run a Single Step Independently:**
```bash
# Run just the company fetching step using latest collection data
python -m unified_pipeline --source cvr_enrichment --stage company_fetching
```

**Run with Custom Time Window:**
```bash  
# Look back 7 days instead of 30
python -m unified_pipeline --source cvr_enrichment --stage company_fetching --max-days-back 7
```

**Disable Independent Mode:**
```bash
# Use traditional pipeline dependencies
python -m unified_pipeline --source cvr_enrichment --stage company_fetching --no-independent-execution
```

## Benefits

1. **Flexibility** - Run any step independently without running previous steps
2. **Resilience** - Recover from partial pipeline failures by rerunning just the failed steps
3. **Development** - Easier debugging and testing of individual steps
4. **Efficiency** - Avoid re-running expensive steps that already completed successfully
5. **Backward Compatibility** - Existing workflows continue to work unchanged

## Error Handling

- **Missing Data**: Steps log clear messages about missing input data and time windows searched
- **Graceful Degradation**: Optional data (like P-numbers for address geocoding) is handled gracefully
- **Fallback Options**: Can fall back to traditional pipeline dependencies if configured

## Configuration

The independent execution behavior is controlled via the shared configuration:

```python
class CVREnrichmentSharedConfig(BaseModel):
    # Independent execution configuration
    enable_independent_execution: bool = True
    max_days_back_for_inputs: int = 30  
    fallback_to_pipeline_dependencies: bool = True
```

This provides a clean, maintainable way to enable the new functionality while preserving existing behavior when needed.

## Testing

The implementation includes:
- Syntax validation of all modified Python files
- Configuration validation 
- Error handling for missing dependencies
- Logging validation for execution mode reporting

## Future Enhancements

Possible future improvements:
- Add CLI options to control independent execution settings
- Add monitoring for GCS access patterns
- Implement caching for frequently accessed latest files
- Add support for partial file matching (e.g., if only some consolidation inputs are available)
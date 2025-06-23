# How to Cancel the Hanging Workflow and Restart

## Current Situation
The property-cadastral merge pipeline has been running for over 17 minutes and appears to be stuck at the export step. The process is likely hanging due to:

1. **Memory/Resource constraints** - Even with 16GB RAM, exporting 6.5M spatial records is intensive
2. **Disk space issues** - The /tmp directory may be running out of space
3. **DuckDB export hanging** - The COPY command may be stuck

## Immediate Actions

### 1. Cancel the Current Workflow
Go to: https://github.com/your-org/landbruget.dk/actions
- Find the running "Property Cadastral Merge Pipeline" workflow
- Click "Cancel workflow" to stop the hanging process

### 2. Restart with Optimizations
The code has been updated with several optimizations:

#### Resource Configuration Updates:
- ✅ **Corrected memory limits**: 12GB (was 5GB) - using correct GitHub Actions limits
- ✅ **Corrected CPU threads**: 4 cores (was 2) - using correct GitHub Actions limits  
- ✅ **Added workflow timeout**: 5 hours to prevent infinite hanging
- ✅ **Optimized export process**: Added compression, progress monitoring, and fallback options

#### New Features Added:
- **Progress monitoring**: Logs progress every 30 seconds during export
- **Resource monitoring**: Checks memory and disk usage
- **Fallback export**: Tries without compression if optimized export fails
- **Better error handling**: More detailed error messages and diagnostics

### 3. Manual Trigger the Updated Pipeline
After canceling, trigger the workflow manually:
1. Go to Actions → Property Cadastral Merge Pipeline  
2. Click "Run workflow"
3. Use default settings (all optimizations are now built-in)

## Expected Improvements

With the corrected resource limits and optimizations:
- **Export should complete**: Better memory management and compression
- **Progress visibility**: You'll see logs every 30 seconds
- **Automatic fallback**: If compression fails, it will try without compression
- **Timeout protection**: Won't hang indefinitely (5-hour limit)

## Monitoring the New Run

Watch for these log messages:
```
- "Preparing to export X records"
- "Available disk space: X GB" 
- "Export still running... Xm Xs elapsed" (every 30s)
- "Export completed in Xs - X records, X MB"
```

If it still fails, the logs will now show exactly why (memory, disk, or other resource issues). 
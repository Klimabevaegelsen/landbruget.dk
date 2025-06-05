# Google Drive Data Pipeline Troubleshooting Guide

This guide helps you diagnose and resolve common issues that may occur when running the Google Drive Data Pipeline.

## Common Issues and Solutions

### Authentication and Access Issues

#### Issue: Google Drive API authentication failed

**Symptoms:**
- Error messages containing "Invalid credentials"
- "Permission denied" errors when accessing Google Drive
- Pipeline exits with authentication-related errors

**Solutions:**
1. Verify that the `GOOGLE_APPLICATION_CREDENTIALS` environment variable points to a valid service account key file
2. Check that the service account key file has not expired
3. Ensure the service account has the necessary Drive API permissions
4. Confirm the target Google Drive folder is shared with the service account email address
5. Try re-downloading the service account key file

#### Issue: Cannot access Google Drive folder

**Symptoms:**
- Empty file list despite folder having files
- "Folder not found" errors
- Permission errors when listing folder contents

**Solutions:**
1. Verify the `GOOGLE_DRIVE_FOLDER_ID` is correct
2. Ensure the folder is shared with the service account email
3. Check sharing permissions on the folder (service account needs at least reader access)
4. Try accessing a parent folder to verify access is working

### File Processing Issues

#### Issue: Files fail to download

**Symptoms:**
- Error messages containing "Download failed"
- Bronze layer contains metadata but no actual files
- Pipeline reports download errors in the summary

**Solutions:**
1. Check network connectivity
2. Verify file permissions in Google Drive
3. For large files, ensure enough disk space is available
4. Try increasing the download timeout in the settings
5. Check for rate limiting issues and adjust throttling settings

#### Issue: Transformation fails for specific files

**Symptoms:**
- Silver layer processing fails for certain files
- Error logs mentioning specific file formats or parsing errors
- Silver processing reports errors but Bronze completed successfully

**Solutions:**
1. Check the format and content of the problematic files
2. For Excel files, ensure they are not password-protected or corrupted
3. For PDF files, verify they are text-based (not scanned images) or enable OCR
4. Consider adding file-specific transformers for unsupported formats
5. Examine the logs for specific parsing errors

### Performance Issues

#### Issue: Pipeline runs too slowly

**Symptoms:**
- Processing takes much longer than expected
- CPU usage is low despite long processing times
- Memory usage grows steadily during execution

**Solutions:**
1. Increase the `MAX_WORKERS` environment variable for more parallel processing
2. Process smaller batches of files by using subfolder filters
3. Use file type filters to focus on specific file types
4. For large Excel files, consider adjusting chunk size settings
5. Ensure the machine has adequate resources (CPU, memory, disk I/O)

#### Issue: Pipeline consumes too much memory

**Symptoms:**
- Out of memory errors
- System becomes unresponsive during processing
- Pipeline crashes with memory-related errors

**Solutions:**
1. Decrease the `MAX_WORKERS` setting to reduce concurrent processing
2. Process files in smaller batches
3. Adjust buffer sizes for file reading operations
4. Enable streaming processing for large files
5. Check for memory leaks in custom transformers

### Storage Issues

#### Issue: Cannot write to data directories

**Symptoms:**
- Permission denied errors when writing files
- Pipeline fails when creating output directories
- Files are created but empty or incomplete

**Solutions:**
1. Ensure the process has write permissions to the data directories
2. Check available disk space
3. Verify that no other process is locking the output files
4. If using Docker, ensure volume mounts are configured correctly
5. For GCS storage, verify that service account has proper storage permissions

#### Issue: File corruption in Bronze layer

**Symptoms:**
- Checksum validation failures when processing Bronze to Silver
- Files have 0 bytes or unexpected content
- Metadata indicates different size than actual file

**Solutions:**
1. Check for interrupted downloads due to network issues
2. Verify the integrity of the source files in Google Drive
3. Re-run the pipeline with Bronze only to refresh the files
4. Check for disk errors or storage problems
5. Ensure adequate storage space throughout the pipeline run

## Diagnosing Problems

### Enabling Debug Logging

For detailed logging to diagnose issues:

```bash
python -m drive_data_pipeline.main --log-level DEBUG
```

Debug logs include:
- Detailed API request/response information
- File-by-file processing status
- Internal function call parameters
- Timing information for performance analysis

### Checking Log Files

The pipeline generates detailed logs in the `logs/` directory. Key log files:

- `pipeline.log`: Main log file with all message levels
- `error.log`: Contains only ERROR level messages
- `debug.log`: Generated when running with DEBUG log level

To analyze logs:

1. Look for ERROR level messages first
2. Search for specific file names or IDs related to failed operations
3. Check timestamps to correlate issues with system events
4. Look for patterns in errors (e.g., same file type, same subfolder)

### Testing Components Individually

You can test specific components of the pipeline separately:

```bash
# Test only Bronze layer
python -m drive_data_pipeline.main --bronze-only --verbose

# Test only Silver layer
python -m drive_data_pipeline.main --silver-only --verbose

# Test with a single subfolder
python -m drive_data_pipeline.main --subfolders "test_folder" --verbose

# Test with a single file type
python -m drive_data_pipeline.main --file-types "xlsx" --verbose
```

## Recovery Strategies

### From Failed Downloads

If Bronze layer processing fails:

1. Run the pipeline again with the same parameters
2. The pipeline will skip already downloaded files
3. Focus on specific subfolders that had issues:
   ```bash
   python -m drive_data_pipeline.main --subfolders "problem_folder" --bronze-only
   ```

### From Failed Transformations

If Silver layer processing fails:

1. Fix any issues with transformers or file handling
2. Run the pipeline with silver-only mode:
   ```bash
   python -m drive_data_pipeline.main --silver-only
   ```
3. To process only specific problematic file types:
   ```bash
   python -m drive_data_pipeline.main --silver-only --file-types "pdf"
   ```

### Complete Reset

For a complete reset of the pipeline state:

1. Delete the contents of the data directory:
   ```bash
   rm -rf data/bronze/* data/silver/*
   ```
2. Run the pipeline from scratch:
   ```bash
   python -m drive_data_pipeline.main --verbose
   ```

## Getting Additional Help

If you continue to experience issues after trying these troubleshooting steps:

1. Gather all relevant log files
2. Document the exact command used to run the pipeline
3. Note the environment variables and configuration settings
4. Record the system specifications (OS, Python version, available resources)
5. Contact the development team with this information 
# Property Owners SFTP Pipeline

This pipeline fetches Danish property ownership data from Datafordeleren's SFTP server and processes it with privacy transformations. Due to the sensitive nature of the data (contains CPR numbers) and IP whitelisting requirements, this pipeline uses a secure Google Cloud VM approach.

## Overview

- **Data Source**: Datafordeleren SFTP server (IP whitelisted)
- **Data Type**: Danish property ownership records (~8.5M features, 12GB GeoJSON)
- **Output Format**: Privacy-transformed Parquet files
- **Runtime**: Google Cloud VM (e2-standard-8, 30GB disk)
- **Frequency**: Manual trigger via GitHub Actions

## Architecture

```
GitHub Actions → Create VM → SFTP Download → Privacy Transform → Upload to GCS → Delete VM
```

### Why VM Approach?

1. **IP Whitelisting**: SFTP server requires static IP addresses
2. **Data Sensitivity**: CPR numbers require secure, isolated processing
3. **Resource Requirements**: 12GB files need substantial memory/disk
4. **Network Security**: Private Google Cloud networking

## Privacy Transformations

The pipeline applies comprehensive privacy protections:

- **CPR Numbers → UUIDs**: Consistent mapping preserving relationships
- **Remove Personal Addresses**: All residential address data removed
- **Remove Birth Dates**: Potential CPR reconstruction data removed  
- **Remove Gender Field**: Additional demographic data removed
- **Add Abroad Flag**: Derived from foreign address data (preserves residency patterns)
- **Keep Names**: Business requirement (names preserved)
- **Keep Protection Notices**: Existing privacy flags maintained

## Data Flow

### Bronze Layer (Raw Data)
- Original ZIP file from SFTP
- Extracted GeoJSON (12GB)
- **No transformations applied**

### Silver Layer (Processed Data)
- Privacy-transformed Parquet files
- CRS auto-detected and converted to EPSG:4326 (WGS84) as required
- Schema-normalized across all batches
- Compressed and optimized for analysis

## Configuration

Copy `env.example` to `.env` and configure:

```bash
# Required: SFTP access (managed via Secret Manager)
PROJECT_ID=landbrugsdata-1
GCS_BUCKET=landbrugsdata-raw-data

# Optional: VM configuration
VM_MACHINE_TYPE=e2-standard-8
VM_DISK_SIZE=30GB
BATCH_SIZE=500000
```

## Security Features

- **Google Secret Manager**: All credentials stored securely
- **VM Self-Deletion**: Automatic cleanup after processing
- **Private Networking**: No external access during processing
- **Encrypted Storage**: All GCS data encrypted at rest
- **Audit Logging**: Complete processing logs maintained

## Pipeline Execution

### Manual Trigger
```bash
gh workflow run property_owners_sftp_transfer_pipeline.yml
```

### Monitoring
```bash
# Check VM status
gcloud compute instances list --project=landbrugsdata-1

# View processing logs
gcloud compute instances get-serial-port-output VM_NAME --zone=europe-west1-b
```

## Error Handling

The pipeline includes comprehensive error handling:

- **Schema Normalization**: Handles varying data structures across batches
- **Memory Management**: Streaming processing prevents OOM errors
- **Network Resilience**: DNS resolution retry logic
- **Validation**: Success verification before VM deletion
- **Debugging**: VM persists on failure for investigation

## Output Structure

```
gs://landbrugsdata-raw-data/
├── bronze/property_owners/
│   └── YYYYMMDD_HHMMSS_original.zip
└── silver/property_owners/  
    └── YYYYMMDD_HHMMSS_property_owners_processed.parquet
```

## Performance

- **Processing Time**: ~18-20 minutes for 8.5M records
- **Memory Usage**: ~8GB peak (batch processing)
- **Storage**: ~200MB Parquet output from 12GB JSON input
- **Network**: ~1-2GB/min download speed

## Troubleshooting

### Common Issues

1. **Schema Mismatch**: Different property types have varying fields
   - **Solution**: Automatic schema normalization with null padding

2. **Memory Exhaustion**: Large features exceed memory limits  
   - **Solution**: 500K record batching with immediate cleanup

3. **SFTP Timeout**: Network connectivity issues
   - **Solution**: DNS retry logic with exponential backoff

4. **VM Hanging**: Processing appears stuck
   - **Solution**: Check logs for infinite loops or resource exhaustion

### Debug Mode
If pipeline fails, VM remains for debugging:
```bash
gcloud compute ssh VM_NAME --zone=europe-west1-b
sudo journalctl -u google-startup-scripts -f
```

## Compliance

This pipeline follows organizational standards where applicable:

- ✅ **Data Security**: Enhanced security for sensitive data
- ✅ **Error Handling**: Comprehensive logging and recovery
- ✅ **Storage Format**: Parquet for Silver layer
- ✅ **Documentation**: Complete pipeline documentation
- ⚠️ **Runtime**: VM approach required for security/whitelisting
- ⚠️ **Structure**: Modified structure for operational requirements

## Future Improvements

- [ ] Add data quality validation
- [ ] Implement incremental processing
- [ ] Add automated alerting
- [ ] Create data lineage tracking
- [ ] Add performance monitoring

## Contact

For questions about this pipeline, contact the data engineering team. 
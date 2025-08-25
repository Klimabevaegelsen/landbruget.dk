# Drive Pipeline Directory Structure Migration

## Overview

The Google Drive data pipeline bronze layer has been restructured to harmonize with other pipelines in the project. This is a **BREAKING CHANGE** that affects how data is organized and accessed downstream.

## Changes Made

### Before (Old Structure)
```
bronze/landbruget.dk_static_files/20250816_193759/
├── Efterafgrøder 2020.xlsx
├── Efterafgrøder 2021.xlsx
├── GKEA2021_Markplan_med_Gødningsoplysninger.xlsx
├── GKEA2022_Markplan_med_Gødningsoplysninger.xlsx
├── Goedningsregnskab_eksempel_2018_2019.pdf
├── Gødningsregnskaber 2022.xlsx
├── Gødningsregnskaber 2023.xlsx
└── ... (all files from all subfolders mixed together)
```

### After (New Structure)
```
bronze/fertiliser/20250816_193759/
├── 2023_Data/
│   └── GKEA2023_Markplan_med_Gødningsoplysninger.xlsx
├── 2024_Data/
│   └── GKEA2024_Markplan_med_Gødningsoplysninger.xlsx
└── Gødningsregnskaber 2022.xlsx

bronze/efterafgroeder/20250816_193759/
├── Efterafgrøder 2020.xlsx
├── Efterafgrøder 2021.xlsx
├── Efterafgrøder 2022.xlsx
└── Efterafgrøder 2023.xlsx

bronze/work_permits/20250816_193759/
├── Active/
│   └── permit_files...
└── Expired/
    └── old_permit_files...
```

## Key Changes

1. **Dataset-level separation**: Each Google Drive subfolder becomes its own dataset directory
2. **Harmonized naming**: Directory names are sanitized (lowercase, spaces → underscores)
3. **Preserved nesting**: Internal folder structure within each dataset is maintained
4. **Consistent pattern**: Follows the standard `bronze/{dataset_name}/{timestamp}/` pattern used by other pipelines

## Directory Name Transformations

| Original Folder Name | New Dataset Directory |
|---------------------|----------------------|
| `Fertiliser` | `bronze/fertiliser/` |
| `Work Permits` | `bronze/work_permits/` |
| `Efterafgrøder` | `bronze/efterafgroeder/` |
| `Gødning & Data` | `bronze/goedning_data/` |

## Impact on Downstream Pipelines

### Silver Layer (Drive Pipeline)
✅ **No changes needed** - The silver layer reads metadata files and handles path discovery automatically.

### External Pipelines Consuming Drive Data

🚨 **BREAKING CHANGE** - Any pipeline that directly reads from the bronze layer will need updates:

#### Before:
```python
# Old path pattern
bronze_path = "gs://landbrugsdata-raw-data/bronze/landbruget.dk_static_files/{timestamp}/"
```

#### After:
```python
# New path pattern - need to specify dataset
fertiliser_path = "gs://landbrugsdata-raw-data/bronze/fertiliser/{timestamp}/"
work_permits_path = "gs://landbrugsdata-raw-data/bronze/work_permits/{timestamp}/"
efterafgroeder_path = "gs://landbrugsdata-raw-data/bronze/efterafgroeder/{timestamp}/"
```

## Migration Steps for Affected Pipelines

### 1. Identify Affected Pipelines
Search for any code that references:
- `landbruget.dk_static_files`
- Direct bronze layer paths to drive data
- Hardcoded paths to specific drive files

### 2. Update Path References

#### Example: Fertiliser Data Pipeline
```python
# OLD
def get_fertiliser_data(timestamp):
    base_path = f"gs://landbrugsdata-raw-data/bronze/landbruget.dk_static_files/{timestamp}/"
    # Search for fertiliser files in mixed directory
    
# NEW  
def get_fertiliser_data(timestamp):
    base_path = f"gs://landbrugsdata-raw-data/bronze/fertiliser/{timestamp}/"
    # All fertiliser files are now in dedicated directory
```

#### Example: Multi-dataset Processing
```python
# OLD
def process_drive_data(timestamp):
    base_path = f"gs://landbrugsdata-raw-data/bronze/landbruget.dk_static_files/{timestamp}/"
    # Process all files together
    
# NEW
def process_drive_data(timestamp):
    datasets = ['fertiliser', 'work_permits', 'efterafgroeder']
    for dataset in datasets:
        dataset_path = f"gs://landbrugsdata-raw-data/bronze/{dataset}/{timestamp}/"
        # Process each dataset separately
```

### 3. Update Configuration Files

Update any configuration files, environment variables, or workflow definitions that reference the old paths.

### 4. Update Documentation

Update any documentation, README files, or data catalogs that reference the old structure.

## Benefits of New Structure

1. **Consistency**: Matches the pattern used by all other pipelines
2. **Clarity**: Each data type has its own clear namespace
3. **Scalability**: Easy to add new data types without mixing concerns
4. **Performance**: Smaller, focused datasets for better query performance
5. **Maintenance**: Easier to manage and understand data organization

## Discovery Commands

### Find Current Datasets
```bash
# List all current drive datasets
gsutil ls gs://landbrugsdata-raw-data/bronze/ | grep -E "(fertiliser|work_permits|efterafgroeder)"
```

### Find Latest Data
```bash
# Find latest fertiliser data
gsutil ls gs://landbrugsdata-raw-data/bronze/fertiliser/ | tail -1

# Find latest work permits data  
gsutil ls gs://landbrugsdata-raw-data/bronze/work_permits/ | tail -1
```

### Explore Dataset Structure
```bash
# Explore fertiliser dataset structure
gsutil ls -r gs://landbrugsdata-raw-data/bronze/fertiliser/20250816_193759/

# Explore work permits dataset structure
gsutil ls -r gs://landbrugsdata-raw-data/bronze/work_permits/20250816_193759/
```

## Testing Migration

1. **Backup**: Ensure you have backups of any critical pipelines before migration
2. **Test with latest data**: Use the most recent timestamp to test your updated paths
3. **Validate outputs**: Ensure your pipeline produces the same results with the new structure
4. **Monitor**: Watch for any errors in logs after deployment

## Rollback Plan

If immediate rollback is needed:
1. The old data structure will remain available until the next drive pipeline run
2. Revert the bronze storage manager changes in `backend/pipelines/drive_data_pipeline/bronze/storage.py`
3. Re-run the drive pipeline to restore the old structure

## Questions or Issues

If you encounter issues during migration:
1. Check the pipeline logs for specific path errors
2. Verify timestamp formats match the expected pattern
3. Ensure your GCS permissions include access to the new dataset directories
4. Contact the data platform team for assistance

---

**Migration Checklist:**
- [ ] Identify all affected pipelines
- [ ] Update hardcoded path references  
- [ ] Update configuration files
- [ ] Test with latest data
- [ ] Update documentation
- [ ] Deploy and monitor

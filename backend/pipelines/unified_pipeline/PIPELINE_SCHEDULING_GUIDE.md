# Pipeline Scheduling Guide

This guide explains the comprehensive scheduling system implemented for the unified pipeline, including different execution frequencies, dependency management, and workflow orchestration.

## Overview

The unified pipeline now supports **three distinct scheduling frequencies**:

- **Manual**: Pipelines that run only when explicitly triggered
- **Weekly**: Pipelines that run automatically every Monday at 2 AM UTC  
- **Monthly**: Pipelines that run automatically on the 1st of every month at 1 AM UTC

## Scheduling Configuration

### Pipeline Frequencies

#### Manual Pipelines
These pipelines require explicit manual triggering and are not scheduled automatically:

- **Soil types** - Danish soil types data from Environmental Portal
- **Wetlands** - Wetlands data processing  
- **Water typology** - Water typology data (lakes, coastal waters, watercourses)
- **Pesticide disaggregation** - Pesticide disaggregation gold layer processing
- **Pesticide compliance** - Pesticide regulatory compliance analysis
- **Worker safety** - Worker safety data processing
- **Work permits** - Work permits data processing
- **DMI** - Danish Meteorological Institute climate data

#### Weekly Pipelines
These pipelines run automatically every Monday at 2 AM UTC:

- **CVR enrichment** - CVR company data enrichment pipeline (also runs monthly in batch 2)

*Note: CHR pipeline runs separately with its own dedicated workflow due to complexity*

#### Monthly Pipelines
These pipelines run automatically on the 1st of every month at 1 AM UTC:

**Foundation Sources (Batch 1 - no dependencies):**
- **DST** - Danish Statistics data
- **FVM WFS** - FVM WFS Agricultural Data (triggers matrix workflow)  
- **Cadastral** - Danish cadastral parcels
- **BNBO** - Well Protection Areas
- **DAGI** - Danish Administrative Geographic Division
- **Water projects** - Water projects data
- **Arbejdstilsynet inspections** - Work Environment Authority inspections

**Dependent Sources (Batch 2 - run after foundation):**
- **CVR enrichment** - Runs after all foundation pipelines complete (also runs weekly)
- **Property cadastral merge** - Depends on cadastral
- **Field production** - Depends on FVM and DST  
- **Field area analysis** - Depends on FVM, BNBO, and Water projects

## Dependency Management

### Dependency Rules

The system enforces proper execution order through dependency management:

1. **Foundation pipelines** run first (no dependencies)
2. **Dependent pipelines** wait for their dependencies to complete
3. **Parallel execution** within each batch for efficiency
4. **Sequential batches** ensure proper data flow

### Example Dependency Chain

```
Monthly Execution Flow:
┌─────────────────────────────────────────────────────────┐
│ Batch 1 (Parallel): Foundation Sources                 │
│ ├─ DST (60min)                                          │
│ ├─ FVM WFS (240min via matrix)                          │
│ ├─ Cadastral (120min)                                   │
│ ├─ BNBO (90min)                                         │
│ ├─ DAGI (60min)                                         │
│ ├─ Water projects (75min)                               │
│ └─ Arbejdstilsynet inspections (45min)                  │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│ Batch 2 (Parallel): Dependent Sources                  │
│ ├─ CVR enrichment (180min) ← All foundation complete   │
│ ├─ Property cadastral merge (90min) ← Cadastral        │
│ ├─ Field production (150min) ← FVM + DST               │
│ └─ Field area analysis (180min) ← FVM + BNBO + Water   │
└─────────────────────────────────────────────────────────┘

Weekly Execution Flow (separate from monthly):
┌─────────────────────────────────────────────────────────┐
│ Weekly Schedule (Every Monday 2 AM UTC)                │
│ └─ CVR enrichment (180min)                              │
└─────────────────────────────────────────────────────────┘
```

## GitHub Actions Workflows

### Workflow Structure

Three separate workflows handle different scheduling frequencies:

1. **`unified_pipeline_weekly.yml`** - Weekly scheduled pipelines
2. **`unified_pipeline_monthly.yml`** - Monthly scheduled pipelines with dependency batching
3. **`unified_pipeline.yml`** - Manual execution and testing (renamed from original)

### Workflow Features

#### Weekly Workflow
- Runs every Monday at 2 AM UTC
- Sequential execution for resource management
- Enhanced monitoring for long-running jobs
- Automatic cleanup of temporary files

#### Monthly Workflow
- Runs on 1st of every month at 1 AM UTC
- **Two-job structure** with proper dependency handling:
  - `monthly-foundation`: Foundation sources (parallel execution)
  - `monthly-dependent`: Dependent sources (waits for foundation to complete)
- Memory optimization for intensive jobs
- Resource monitoring and cleanup

#### Manual Workflow
- On-demand execution for testing and manual runs
- Supports all pipeline sources
- Maintains original matrix job functionality for FVM WFS
- Includes pesticide processing matrix jobs

## CLI Management

### Scheduling Commands

The system includes CLI commands for managing schedules:

```bash
# List all pipeline schedules
python -m unified_pipeline.cli_scheduling list-schedules

# Show execution order for monthly pipelines
python -m unified_pipeline.cli_scheduling execution-order --frequency monthly

# Validate scheduling configuration
python -m unified_pipeline.cli_scheduling validate

# Show detailed info for a specific pipeline
python -m unified_pipeline.cli_scheduling info cadastral
```

### Example CLI Output

```bash
$ python -m unified_pipeline.cli_scheduling list-schedules --frequency monthly --show-dependencies

📋 Pipeline Schedules (10 pipelines)
================================================================================
┌─────────────────────────┬───────────┬──────────┬──────────────┬─────────────────┬──────────────────┐
│ Source                  │ Frequency │ Priority │ Duration (min)│ Description     │ Dependencies     │
├─────────────────────────┼───────────┼──────────┼──────────────┼─────────────────┼──────────────────┤
│ dst                     │ monthly   │ 1        │ 60           │ Danish Stat...  │ None             │
│ fvm_wfs                 │ monthly   │ 2        │ 240          │ FVM WFS Agr...  │ None             │
│ cadastral               │ monthly   │ 3        │ 120          │ Danish cada...  │ None             │
│ property_cadastral_merge│ monthly   │ 10       │ 90           │ Property-Ca...  │ cadastral        │
│ field_production        │ monthly   │ 11       │ 150          │ Field produ...  │ fvm_wfs,dst      │
│ field_area_analysis     │ monthly   │ 12       │ 180          │ Field area ...  │ fvm_wfs,bnbo,... │
└─────────────────────────┴───────────┴──────────┴──────────────┴─────────────────┴──────────────────┘

📊 Summary:
  Monthly: 10 pipelines
  Total estimated duration: 1065 minutes (17.8 hours)
```

## Configuration Files

### Scheduling Configuration

The scheduling system is configured in:
- `src/unified_pipeline/model/scheduling.py` - Main scheduling configuration
- `src/unified_pipeline/cli_scheduling.py` - CLI management commands

### Key Configuration Elements

```python
# Example scheduling configuration
Source.cadastral: PipelineScheduleConfig(
    frequency=ScheduleFrequency.MONTHLY,
    priority=3,
    description="Danish cadastral parcels via WFS",
    estimated_duration_minutes=120
),

Source.property_cadastral_merge: PipelineScheduleConfig(
    frequency=ScheduleFrequency.MONTHLY,
    depends_on=[Source.cadastral],  # Dependency definition
    priority=10,  # Higher priority number = runs later
    description="Property-Cadastral merge gold layer",
    estimated_duration_minutes=90
)
```

## Benefits

### Improved Resource Management
- **Parallel execution** within dependency batches
- **Sequential batches** prevent resource conflicts
- **Memory optimization** for intensive pipelines

### Better Reliability  
- **Dependency validation** prevents incomplete data processing
- **Automatic retry** capabilities in GitHub Actions
- **Resource monitoring** prevents out-of-memory failures

### Enhanced Visibility
- **Clear scheduling information** in logs
- **Estimated durations** for planning
- **Dependency tracking** for troubleshooting

### Operational Efficiency
- **Automated scheduling** reduces manual intervention
- **Proper sequencing** ensures data consistency
- **Batch processing** optimizes compute resources

## Troubleshooting

### Common Issues

1. **Dependency Validation Errors**
   ```bash
   # Check for configuration errors
   python -m unified_pipeline.cli_scheduling validate
   ```

2. **Missing Dependencies**
   - Check that prerequisite pipelines completed successfully
   - Verify GCS data availability
   - Review GitHub Actions workflow logs

3. **Resource Issues**
   - Monitor memory usage in workflow logs
   - Check disk space availability
   - Review cleanup procedures

### Monitoring

- **GitHub Actions logs** provide detailed execution information
- **Resource monitoring** tracks memory and disk usage
- **Dependency validation** runs at pipeline startup

## Migration from Original System

The original `unified_pipeline.yml` workflow has been restructured:

- **Scheduled execution** moved to frequency-specific workflows
- **Manual execution** preserved for testing and ad-hoc runs
- **Matrix jobs** maintained for complex pipelines (FVM WFS, Field Area Analysis)
- **Pesticide processing** remains in manual workflow

This ensures backward compatibility while providing enhanced scheduling capabilities.

## Future Enhancements

Potential improvements to the scheduling system:

1. **GCS-based dependency checking** - Verify actual data availability
2. **Dynamic scheduling** - Adjust timing based on data freshness
3. **Failure recovery** - Automatic retry with exponential backoff
4. **Performance optimization** - Resource allocation based on pipeline requirements
5. **Notification system** - Alerts for failed dependencies or long-running jobs

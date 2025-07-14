# CHR Pipeline Concurrency Limits

This document outlines the concurrency limits implemented in the CHR pipeline to prevent overwhelming servers and reduce resource usage.

## GitHub Actions Workflow Limits

### Workflow-level Concurrency
- **Setting**: `concurrency.group: chr-pipeline`
- **Effect**: Prevents multiple CHR pipeline runs from interfering with each other
- **Behavior**: `cancel-in-progress: true` cancels previous runs when a new one starts

### Matrix Job Limits
The following matrix jobs now have configurable concurrency limits:

#### 1. Parallel Group 1 (diko, spf_su)
- **Default**: 2 concurrent jobs
- **Configurable**: Via `max_concurrent_jobs` input parameter
- **Jobs**: `diko` and `spf_su` data collection

#### 2. Monthly Animal Movements
- **Default**: 6 concurrent jobs
- **Configurable**: Via `max_concurrent_jobs` input parameter
- **Jobs**: Processes animal movements by month (e.g., 2022-01, 2022-02, etc.)

#### 3. Parallel Group 2 (ejendom, vetstat)
- **Default**: 2 concurrent jobs
- **Configurable**: Via `max_concurrent_jobs` input parameter
- **Jobs**: `ejendom` and `vetstat` data collection

## Application-level Limits

### Main Pipeline Workers
- **Environment Variable**: `CHR_MAX_WORKERS`
- **Default**: 5 (reduced from 10)
- **Configurable**: Via environment variable or command line `--workers`
- **Effect**: Limits ThreadPoolExecutor concurrent threads

### SPF-SU API Requests
- **Environment Variable**: `SPF_SU_MAX_WORKERS`
- **Default**: 3 (reduced from 5)
- **Configurable**: Via environment variable
- **Effect**: Limits asyncio semaphore for concurrent API requests

## Configuration Options

### GitHub Actions Inputs
```yaml
max_concurrent_jobs:
  description: 'Maximum number of concurrent matrix jobs'
  type: string
  default: '6'
```

### Environment Variables
```bash
CHR_MAX_WORKERS=3           # Main pipeline thread pool size
SPF_SU_MAX_WORKERS=2        # SPF-SU API concurrency limit
```

### Command Line Arguments
```bash
python main.py --workers 3  # Override default worker count
```

## Recommended Settings

### For Production (GitHub Actions)
- `max_concurrent_jobs: 6` (default)
- `CHR_MAX_WORKERS: 3`
- `SPF_SU_MAX_WORKERS: 2`

### For Development/Testing
- `max_concurrent_jobs: 2` (lower to reduce server load)
- `CHR_MAX_WORKERS: 2`
- `SPF_SU_MAX_WORKERS: 1`

### For Heavy Load Scenarios
- `max_concurrent_jobs: 2` (minimum to prevent server overload)
- `CHR_MAX_WORKERS: 2`
- `SPF_SU_MAX_WORKERS: 1`

## Impact on Performance

### Benefits
- Reduced server load on FVM and other external APIs
- Lower memory usage per GitHub Actions runner
- Better reliability with fewer timeout errors
- Improved resource sharing across concurrent jobs

### Trade-offs
- Slightly longer total execution time
- More controlled resource usage
- Better predictability of execution times

## Monitoring

The pipeline logs concurrency settings at startup:
```
Using 3 concurrent workers for rate limiting
Processing chunk 1/10 (25 herds)
```

Monitor for:
- Timeout errors (may need to reduce concurrency)
- Slow execution (may need to increase concurrency)
- Memory issues (reduce both matrix and worker concurrency) 
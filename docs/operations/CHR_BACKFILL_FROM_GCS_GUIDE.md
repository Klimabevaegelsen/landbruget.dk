# CHR One-Time Backfill from Existing GCS Data

> **Purpose**: Create a comprehensive historical dataset by reprocessing existing bronze data in GCS, avoiding unnecessary API calls while filling data gaps for incremental processing.

---

## Overview

Before switching to incremental processing, we need to create a complete historical dataset by leveraging existing GCS bronze data. Based on our data audit, we have:

- **Animal Movements**: 15 years of data (2011-2025) in monthly bronze runs
- **Svineflytning**: 4 years of data (2022-2025) in weekly bronze runs
- **Antibiotic Usage**: 1 month of data (July 2025) in silver runs
- **Main CHR Data**: Available in recent bronze/silver runs

## Strategy

### Phase 1: Reprocess Existing Bronze Data (Recommended)

- **No new API calls** - use existing bronze data
- **Fast execution** - just data transformation
- **Validate/improve** silver processing logic
- **Build complete historical dataset**

### Phase 2: Targeted Gap Filling (If Needed)

- **Selective API calls** for critical missing periods
- **Minimal government API impact**
- **Focus on high-value data gaps**

---

## Implementation Steps

### Step 1: Data Inventory and Planning

#### 1.1 Run Comprehensive Data Audit

```bash
# Create audit script to analyze existing GCS data
python -c "
import subprocess
import json
from datetime import datetime

# Get all available CHR bronze timestamps
result = subprocess.run(['gsutil', 'ls', 'gs://landbrugsdata-raw-data/bronze/chr/'],
                       capture_output=True, text=True)
timestamps = [line.strip().split('/')[-2] for line in result.stdout.strip().split('\n') if line]

print(f'Available CHR bronze timestamps: {len(timestamps)}')
for ts in sorted(timestamps)[-10:]:  # Show last 10
    print(f'  {ts}')
"
```

#### 1.2 Identify Best Bronze Data Sources

Based on our audit findings:

**Animal Movements (Priority: High)**:

```bash
# Monthly bronze runs with 15 years of historical data
gs://landbrugsdata-raw-data/bronze/chr/20250810_075014_2025-01/chr_dyr_movement_summaries.parquet
gs://landbrugsdata-raw-data/bronze/chr/20250810_075014_2025-02/chr_dyr_movement_summaries.parquet
# ... through 2025-08 (covers 2011-2025)
```

**Svineflytning (Priority: Medium)**:

```bash
# Weekly bronze runs with 4 years of data
gs://landbrugsdata-raw-data/bronze/svineflytning/20250901_065848/svineflytning.json
gs://landbrugsdata-raw-data/bronze/svineflytning/20250825_065657/svineflytning.json
# ... covers 2022-2025
```

**Antibiotic Usage (Priority: Low)**:

```bash
# Only July 2025 data available - limited backfill value
gs://landbrugsdata-raw-data/silver/chr/20250822_161056/antibiotic_usage.parquet
```

### Step 2: Create Backfill Workflows

#### 2.1 CHR Historical Backfill Workflow

Create `.github/workflows/chr_historical_backfill.yml`:

```yaml
name: CHR Historical Backfill from GCS

on:
  workflow_dispatch:
    inputs:
      data_source:
        description: "Data source to backfill"
        type: choice
        required: true
        options:
          - animal_movements
          - all_chr_data
          - svineflytning
        default: "animal_movements"
      bronze_timestamps:
        description: "Comma-separated bronze timestamps to process"
        type: string
        required: true
        default: "20250810_075014_2025-01,20250810_075014_2025-02,20250810_075014_2025-03,20250810_075014_2025-04,20250810_075014_2025-05,20250810_075014_2025-06,20250810_075014_2025-07,20250810_075014_2025-08"
      target_date_range:
        description: "Target date range for validation (YYYY-MM-DD to YYYY-MM-DD)"
        type: string
        required: true
        default: "2011-01-01 to 2025-08-31"
      skip_existing_silver:
        description: "Skip processing if target silver data already exists"
        type: boolean
        default: true
      dry_run:
        description: "Dry run - validate data without processing"
        type: boolean
        default: false

jobs:
  backfill-animal-movements:
    if: ${{ inputs.data_source == 'animal_movements' || inputs.data_source == 'all_chr_data' }}
    runs-on: ubuntu-latest
    timeout-minutes: 480 # 8 hours for large historical datasets

    permissions:
      contents: "read"
      id-token: "write"

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install uv
        run: |
          curl -LsSf https://astral.sh/uv/install.sh | sh
          echo "$HOME/.cargo/bin" >> $GITHUB_PATH

      - name: Install dependencies
        run: |
          cd backend/pipelines/unified_pipeline
          uv pip install --system -e .
          cd ../chr_pipeline
          uv pip install --system -e .

      - id: "auth"
        name: "Authenticate to Google Cloud"
        uses: "google-github-actions/auth@v2"
        with:
          credentials_json: "${{ secrets.GCP_SA_KEY }}"

      - name: "Set up Cloud SDK"
        uses: "google-github-actions/setup-gcloud@v2"

      - name: Validate Bronze Data Availability
        run: |
          echo "🔍 Validating bronze data availability..."
          IFS=',' read -ra TIMESTAMPS <<< "${{ inputs.bronze_timestamps }}"

          for timestamp in "${TIMESTAMPS[@]}"; do
            echo "Checking: gs://landbrugsdata-raw-data/bronze/chr/$timestamp/"
            if gsutil ls "gs://landbrugsdata-raw-data/bronze/chr/$timestamp/" > /dev/null 2>&1; then
              echo "✅ $timestamp - Available"
              gsutil ls "gs://landbrugsdata-raw-data/bronze/chr/$timestamp/*movement*" || echo "   ⚠️ No movement files found"
            else
              echo "❌ $timestamp - Not found"
              exit 1
            fi
          done

      - name: Set up environment variables
        env:
          GCS_BUCKET: ${{ secrets.GCS_BUCKET }}
          GOOGLE_CLOUD_PROJECT: ${{ secrets.GOOGLE_CLOUD_PROJECT }}
          SUPABASE_URL: ${{ secrets.SUPABASE_URL }}
          SUPABASE_ANON_KEY: ${{ secrets.SUPABASE_ANON_KEY }}
        run: |
          cd backend/pipelines/chr_pipeline
          echo "GCS_BUCKET=$GCS_BUCKET" >> .env
          echo "GOOGLE_CLOUD_PROJECT=$GOOGLE_CLOUD_PROJECT" >> .env
          echo "SUPABASE_URL=$SUPABASE_URL" >> .env
          echo "SUPABASE_ANON_KEY=$SUPABASE_ANON_KEY" >> .env
          echo "CHR_PROCESSING_MODE=backfill" >> .env
          mkdir -p /tmp/data/silver/chr

      - name: Process Historical Data
        working-directory: backend/pipelines/chr_pipeline
        run: |
          if [ "${{ inputs.dry_run }}" = "true" ]; then
            echo "🔍 DRY RUN MODE - Validating data only"
          else
            echo "🚀 PROCESSING MODE - Creating historical dataset"
          fi

          IFS=',' read -ra TIMESTAMPS <<< "${{ inputs.bronze_timestamps }}"
          TOTAL=${#TIMESTAMPS[@]}
          CURRENT=0

          for timestamp in "${TIMESTAMPS[@]}"; do
            CURRENT=$((CURRENT + 1))
            echo ""
            echo "📊 Processing $CURRENT/$TOTAL: $timestamp"
            
            if [ "${{ inputs.dry_run }}" = "true" ]; then
              # Dry run - just validate
              echo "  🔍 Validating bronze data structure..."
              gsutil ls -la "gs://landbrugsdata-raw-data/bronze/chr/$timestamp/" | head -10
              
              # Check for movement data specifically
              if gsutil ls "gs://landbrugsdata-raw-data/bronze/chr/$timestamp/*movement*" > /dev/null 2>&1; then
                echo "  ✅ Movement data found"
              else
                echo "  ⚠️ No movement data found"
              fi
              
            else
              # Full processing
              echo "  🔄 Processing bronze data through silver layer..."
              
              # Set the specific bronze timestamp for processing
              export CHR_FORCE_BACKFILL_TIMESTAMP="$timestamp"
              
              # Run silver processing using the bronze timestamp
              python -m main --steps silver_all --log-level INFO --progress
              
              if [ $? -eq 0 ]; then
                echo "  ✅ Successfully processed $timestamp"
              else
                echo "  ❌ Failed to process $timestamp"
                exit 1
              fi
            fi
            
            # Brief pause between timestamps
            sleep 5
          done

          echo ""
          echo "🎉 Backfill processing completed!"

      - name: Validate Output Data
        if: ${{ inputs.dry_run == 'false' }}
        run: |
          echo "🔍 Validating backfill output..."

          # Check silver output directory
          if gsutil ls "gs://landbrugsdata-raw-data/silver/chr/" | grep -E '[0-9]{8}_[0-9]{6}' | tail -5; then
            echo "✅ Silver data generated"
            
            # Check for movement data in latest silver run
            LATEST_SILVER=$(gsutil ls "gs://landbrugsdata-raw-data/silver/chr/" | grep -E '[0-9]{8}_[0-9]{6}' | sort | tail -1 | sed 's|/$||')
            
            if gsutil ls "$LATEST_SILVER/*movement*" > /dev/null 2>&1; then
              echo "✅ Movement data found in silver output"
              gsutil ls -la "$LATEST_SILVER/*movement*"
            else
              echo "⚠️ No movement data in silver output"
            fi
          else
            echo "❌ No silver data generated"
            exit 1
          fi

  backfill-svineflytning:
    if: ${{ inputs.data_source == 'svineflytning' || inputs.data_source == 'all_chr_data' }}
    runs-on: ubuntu-latest
    timeout-minutes: 240 # 4 hours for svineflytning data

    permissions:
      contents: "read"
      id-token: "write"

    steps:
      - uses: actions/checkout@v4

      # Similar setup steps as above...

      - name: Process Svineflytning Historical Data
        run: |
          echo "🐷 Processing Svineflytning historical data..."
          echo "📅 Date range: 2022-2025 (4 years)"

          # Set processing mode for historical range
          export SVINEFLYTNING_PROCESSING_MODE=full

          cd backend/pipelines/svineflytning_pipeline
          python main.py --stage all --start-date 2022-01-01 --end-date 2025-09-14 --log-level INFO --progress

  validate-complete-dataset:
    needs: [backfill-animal-movements, backfill-svineflytning]
    if: always() && !cancelled()
    runs-on: ubuntu-latest

    steps:
      - name: Validate Complete Historical Dataset
        run: |
          echo "🔍 Final validation of complete historical dataset..."

          # Validate CHR data
          echo "CHR Data Coverage:"
          gsutil ls -la "gs://landbrugsdata-raw-data/silver/chr/" | tail -10

          # Validate Svineflytning data  
          echo "Svineflytning Data Coverage:"
          gsutil ls -la "gs://landbrugsdata-raw-data/silver/svineflytning/" | tail -10

          echo "✅ Historical backfill validation completed"
```

### Step 3: Execution Plan

#### 3.1 Pre-Backfill Checklist

- [ ] **Backup current Supabase data** (if any)
- [ ] **Verify GCS access** and authentication
- [ ] **Test with single timestamp** first (dry run)
- [ ] **Check GitHub Actions runner limits** (8-hour timeout)
- [ ] **Coordinate with team** - backfill will take several hours

#### 3.2 Execution Sequence

**Week 1: Animal Movements (Highest Value)**

```bash
# Run backfill for animal movements (15 years of data)
# Priority: High - this gives us the most historical value
Timestamps: 20250810_075014_2025-01 through 20250810_075014_2025-08
Expected Duration: 4-6 hours
Expected Output: ~15 years of animal movement data (2011-2025)
```

**Week 2: Svineflytning (Medium Value)**

```bash
# Run backfill for svineflytning (4 years of data)
# Priority: Medium - good historical coverage
Date Range: 2022-01-01 to 2025-09-14
Expected Duration: 2-3 hours
Expected Output: 4 years of pig movement data
```

**Week 3: Validation and Gap Analysis**

```bash
# Validate complete dataset and identify any remaining gaps
# Run data quality checks
# Plan any targeted API backfill for critical missing periods
```

#### 3.3 Manual Execution Commands

For manual execution or testing:

```bash
# Test single timestamp (dry run)
gh workflow run chr_historical_backfill.yml \
  -f data_source=animal_movements \
  -f bronze_timestamps=20250810_075014_2025-08 \
  -f target_date_range="2011-01-01 to 2025-08-31" \
  -f dry_run=true

# Full animal movements backfill
gh workflow run chr_historical_backfill.yml \
  -f data_source=animal_movements \
  -f bronze_timestamps="20250810_075014_2025-01,20250810_075014_2025-02,20250810_075014_2025-03,20250810_075014_2025-04,20250810_075014_2025-05,20250810_075014_2025-06,20250810_075014_2025-07,20250810_075014_2025-08" \
  -f target_date_range="2011-01-01 to 2025-08-31" \
  -f dry_run=false

# Svineflytning backfill
gh workflow run chr_historical_backfill.yml \
  -f data_source=svineflytning \
  -f target_date_range="2022-01-01 to 2025-09-14" \
  -f dry_run=false
```

### Step 4: Post-Backfill Validation

#### 4.1 Data Quality Checks

```python
# Create validation script: scripts/validation/validate_historical_backfill.py
import duckdb

def validate_backfill_completeness():
    """Validate the historical backfill was successful."""

    conn = duckdb.connect()

    # Check animal movements coverage
    print("🔍 Validating Animal Movements Coverage...")
    result = conn.execute("""
    SELECT
        MIN(movement_date) as earliest_date,
        MAX(movement_date) as latest_date,
        COUNT(*) as total_movements,
        COUNT(DISTINCT chr_number) as unique_herds
    FROM read_parquet('gs://landbrugsdata-raw-data/silver/chr/*/chr_animal_movements.parquet')
    """).fetchall()

    for row in result:
        print(f"  Date range: {row[0]} to {row[1]}")
        print(f"  Total movements: {row[2]:,}")
        print(f"  Unique herds: {row[3]:,}")

        # Validate we have 10+ years of data
        if row[0] and str(row[0])[:4] <= "2015":
            print("  ✅ Historical coverage looks good (10+ years)")
        else:
            print("  ⚠️ Limited historical coverage")

    # Check svineflytning coverage
    print("\n🔍 Validating Svineflytning Coverage...")
    # Similar validation for svineflytning data...

    print("\n✅ Backfill validation completed")

if __name__ == "__main__":
    validate_backfill_completeness()
```

#### 4.2 Performance Impact Assessment

After backfill completion:

```bash
# Measure data volume created
gsutil du -sh gs://landbrugsdata-raw-data/silver/chr/
gsutil du -sh gs://landbrugsdata-raw-data/silver/svineflytning/

# Check Supabase data volume (if synced)
# Validate incremental processing is ready to start
```

### Step 5: Transition to Incremental Processing

Once backfill is complete:

1. **Update pipeline schedules** to use incremental mode by default
2. **Set incremental processing as default** in workflows
3. **Monitor first incremental runs** to ensure smooth transition
4. **Document the new operational procedures**

---

## Expected Outcomes

### Data Coverage After Backfill

- **Animal Movements**: 15 years (2011-2025) - **Excellent historical coverage**
- **Svineflytning**: 4 years (2022-2025) - **Good recent coverage**
- **Antibiotic Usage**: 1 month (July 2025) - **Limited, but current**
- **Main CHR Data**: Current complete dataset

### Performance Benefits

- **No API strain** during backfill (using existing bronze data)
- **Fast processing** (data transformation only)
- **Complete historical foundation** for incremental processing
- **Ready for 95% processing time reduction** going forward

### Timeline

- **Week 1**: Animal movements backfill (4-6 hours)
- **Week 2**: Svineflytning backfill (2-3 hours)
- **Week 3**: Validation and transition to incremental mode
- **Week 4**: Monitor incremental processing performance

---

## Risk Mitigation

### Data Safety

- **Atomic operations** - each timestamp processed separately
- **Validation at each step** - verify data before proceeding
- **Rollback capability** - can revert to previous state
- **Backup existing data** before starting

### Performance

- **GitHub Actions timeout protection** (8-hour limit)
- **Progress monitoring** with detailed logging
- **Chunked processing** - one timestamp at a time
- **Resource monitoring** to avoid runner exhaustion

### Quality Assurance

- **Dry run capability** - test before full processing
- **Data validation checks** at each step
- **Comparison with existing data** where available
- **Manual verification** of key metrics

This backfill strategy leverages our existing GCS data assets to create a comprehensive historical dataset with minimal API impact, setting the foundation for efficient incremental processing going forward.

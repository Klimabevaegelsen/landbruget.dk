# Subsidies Gold Layer Analytics Pipeline Documentation

## Overview

The Subsidies Gold Layer Analytics Pipeline focuses on creating company-level aggregations and analysis from existing silver layer subsidies data. This pipeline reads from the already processed silver layer data in the GCS bucket and creates comprehensive analytical datasets for business intelligence and research purposes.

## Pipeline Architecture

```
    Silver Layer Data (GCS)
    ├── FVM Environmental Subsidies
    ├── FVM Grassland Subsidies
    ├── FVM Organic Subsidies
    └── General Subsidies Data
         ↓
     Gold Layer Analytics
    ├── Company Aggregation
    ├── Risk Analysis
    ├── Trend Calculations
    └── Multi-dimensional Analysis
         ↓
    Analytical Datasets
    ├── Company Summary
    ├── Yearly Breakdown
    ├── Category Analysis
    └── Trends Analysis
```

## Data Sources

The pipeline processes multiple types of subsidies data:

### Primary Datasets
- **Landbrugsstøtte (Agricultural Support)**: Direct payments to farmers
- **Projekttilskud (Project Subsidies)**: Project-based funding
- **De minimis støtte**: Small-scale aid under EU de minimis rules
- **Slagtepræmie (Slaughter Premiums)**: Animal welfare payments
- **Nationale midler (National Funds)**: National program funding
- **GUDP (Development Funds)**: Rural development programs
- **Plantefonden (Plant Foundation)**: Plant production support
- **SLP (Single Land Payment)**: EU direct payment scheme

### Data Location
- **Source**: Google Cloud Storage bucket `gs://landbrugsdata-raw-data/silver/subsidies/`
- **Format**: Parquet files organized by date folders
- **Update Frequency**: Irregular (typically quarterly or annually)

## Processing Stages

### Bronze Layer (`unified_pipeline.bronze.subsidies`)

**Purpose**: Raw data ingestion and basic validation

**Key Functions**:
- Download subsidies files from GCS bucket
- Basic data validation and quality checks
- Metadata addition (source file, processing timestamp)
- File type classification based on naming patterns

**Output**: Bronze tables partitioned by subsidy type and processing date

**Configuration**:
```python
SubsidiesBronzeConfig(
    bucket_name="landbrugsdata-raw-data",
    subsidies_folder="silver/subsidies",
    dataset="subsidies_bronze"
)
```

### Silver Layer (`unified_pipeline.silver.subsidies`)

**Purpose**: Data standardization and cleaning

**Key Transformations**:
1. **CVR Number Validation**: Standardize Danish company registration numbers
2. **Amount Normalization**: Convert all amounts to consistent DKK format
3. **Date Standardization**: Parse various date formats to ISO standard
4. **Company Name Cleaning**: Standardize company name formats
5. **Data Quality Scoring**: Calculate completeness and validity metrics

**Output**: Standardized silver tables by subsidy type

**Configuration**:
```python
SubsidiesSilverConfig(
    validate_cvr_format=True,
    require_valid_cvr=False,
    standardize_company_names=True,
    standardize_dates=True
)
```

### Gold Layer (`unified_pipeline.gold.company_subsidies_aggregation`)

**Purpose**: Company-level aggregation and analysis

**Key Outputs**:

1. **Company Summary** (`company_subsidies_summary`):
   - Total subsidies per company
   - Subsidy categories and types received
   - Time period coverage
   - Company size classification

2. **Yearly Breakdown** (`company_subsidies_yearly`):
   - Year-by-year subsidy amounts
   - Year-over-year change calculations
   - Risk indicators (unusual increases)

3. **Category Analysis** (`company_subsidies_categories`):
   - Breakdown by subsidy category
   - Category shares per company
   - Specialization analysis

4. **Trends Analysis** (`subsidy_trends_analysis`):
   - Multi-year trend patterns
   - Growth/decline classification
   - Volatility measurements

**Configuration**:
```python
CompanySubsidiesAggregationGoldConfig(
    analysis_start_year=2019,
    analysis_end_year=2024,
    small_company_threshold=100000.0,
    medium_company_threshold=1000000.0
)
```

## Usage

### Command Line Execution

```bash
# Run gold layer analytics pipeline
python -m unified_pipeline.subsidies_pipeline

# Enable verbose logging
python -m unified_pipeline.subsidies_pipeline --verbose
```

### Programmatic Usage

```python
from unified_pipeline.subsidies_pipeline import SubsidiesGoldPipelineOrchestrator

# Initialize orchestrator
orchestrator = SubsidiesGoldPipelineOrchestrator()

# Run gold layer analytics
results = await orchestrator.run_pipeline()
```

## Key Features

### Company Size Classification
- **Small**: < 100,000 DKK total subsidies
- **Medium**: 100,000 - 1,000,000 DKK
- **Large**: > 1,000,000 DKK

### Risk Analysis
- **High-risk increases**: >50% year-over-year growth
- **Dependency analysis**: Subsidies as % of estimated revenue
- **Volatility scoring**: Coefficient of variation in annual amounts

### Data Quality Metrics
- CVR completeness and validity
- Amount field completeness
- Date field standardization success
- Overall data quality scoring

## Output Schema

### Company Summary Table
```sql
cvr_number                 VARCHAR   -- Company registration number
total_subsidies           DECIMAL   -- Total subsidies (DKK)
average_subsidy_amount    DECIMAL   -- Average per payment
total_subsidy_count       INTEGER   -- Number of payments
years_with_subsidies      INTEGER   -- Years active
subsidy_categories        ARRAY     -- Categories received
company_size_class        VARCHAR   -- Small/Medium/Large
first_subsidy_year        INTEGER   -- First year received
last_subsidy_year         INTEGER   -- Last year received
```

### Yearly Breakdown Table
```sql
cvr_number                VARCHAR   -- Company registration number
subsidy_year             INTEGER   -- Year
total_amount             DECIMAL   -- Annual total (DKK)
subsidy_count            INTEGER   -- Number of payments
yoy_change               DECIMAL   -- Year-over-year change %
high_risk_increase       BOOLEAN   -- Risk flag
```

## Monitoring and Validation

### Pipeline Metrics
- Total companies processed
- Data completeness rates
- Processing duration
- Error rates by stage

### Data Quality Checks
- CVR format validation
- Amount reasonableness checks
- Date consistency validation
- Duplicate detection

### Performance Monitoring
- Processing time per stage
- Memory usage tracking
- File processing rates
- Error recovery statistics

## Troubleshooting

### Common Issues

1. **GCS Access Errors**
   - Verify gsutil authentication
   - Check bucket permissions
   - Validate file paths

2. **CVR Validation Failures**
   - Review CVR format patterns
   - Check for missing/invalid data
   - Adjust validation strictness

3. **Memory Issues**
   - Process files in smaller batches
   - Increase available memory
   - Optimize data types

4. **Date Parsing Errors**
   - Review date format configurations
   - Add new date patterns as needed
   - Check for data corruption

### Debugging

Enable verbose logging:
```bash
python -m unified_pipeline.subsidies_pipeline --stage all --verbose
```

Check individual stage outputs:
```sql
-- Inspect bronze data
SELECT * FROM subsidies_bronze_agricultural_support_20240101 LIMIT 10;

-- Check silver standardization
SELECT cvr_number, cvr_valid, total_standardized
FROM subsidies_silver_agricultural_support
WHERE cvr_valid = false;

-- Review gold aggregations
SELECT company_size_class, COUNT(*), AVG(total_subsidies)
FROM company_subsidies_summary
GROUP BY company_size_class;
```

## Integration Points

### Upstream Dependencies
- Raw subsidies data in GCS bucket
- Valid CVR number reference data
- Company metadata (optional)

### Downstream Consumers
- Business intelligence dashboards
- Compliance reporting systems
- Agricultural analytics platforms
- Economic research datasets

## Future Enhancements

### Planned Features
- Geographic analysis integration
- Industry sector classification
- EU compliance checking
- Real-time anomaly detection
- Cross-subsidy dependency analysis

### Data Source Expansion
- Additional government subsidy programs
- EU-level funding data
- Regional development funds
- Innovation support schemes

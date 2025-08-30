# Field Production Workflow

> **Monthly Dependent Processing**: Estimating field-level agricultural production using FVM field data and DST yield statistics

---

## What This Workflow Does

The Field Production workflow combines agricultural field data from the FVM WFS workflow with yield statistics from the DST (Danmarks Statistik) workflow to create comprehensive field-level production estimates. This gold-layer-only workflow performs sophisticated crop mapping and spatial analysis to estimate production for individual agricultural fields across Denmark, providing detailed analytics for agricultural research, policy development, and business intelligence.

### Why This Data Matters
- **Field-Level Production Analytics**: Detailed production estimates for individual agricultural fields with spatial precision
- **Agricultural Policy Support**: Evidence-based data for agricultural policy development and subsidy calculations
- **Crop Yield Intelligence**: Comprehensive yield estimation using official Danish statistical data
- **Regional Agricultural Analysis**: Production patterns analysis across Danish administrative regions
- **Business Intelligence**: Agricultural production data for market analysis and investment decisions
- **Research Foundation**: High-quality field production data for agricultural research and environmental studies

### Key Statistics
- **Processing Scale**: ~2.8M agricultural fields processed annually with production estimates
- **Crop Coverage**: 200+ crop types mapped to DST statistical categories with comprehensive yield data
- **Temporal Coverage**: Multi-year historical production estimates (2005-present) with annual updates
- **Spatial Resolution**: Individual field-level estimates with precise geographic boundaries
- **Data Integration**: Advanced integration of FVM field boundaries with DST yield statistics
- **Update Frequency**: Monthly dependent processing after foundation data workflows complete

---

## Data Sources and Dependencies

### Primary Dependencies
This workflow depends on silver data from two foundation sources:

| Source Workflow | Data Type | Dependency | Content |
|-----------------|-----------|------------|---------|
| **FVM WFS Workflow** | Agricultural field boundaries | Monthly foundation | ~2.8M field boundaries with crop types and farming details |
| **DST Workflow** | Agricultural yield statistics | Monthly foundation | Official Danish yield data across 4 statistical tables |

### Data Integration Architecture

#### FVM WFS Workflow Integration
- **Data Source**: Agricultural field boundaries from Danish Agricultural Agency (FVM)
- **Temporal Coverage**: Annual field boundary data from 2005-present with crop information
- **Field Details**: Crop types, organic farming status, field areas, CVR numbers, and spatial geometries
- **Scale**: ~2.8M agricultural fields annually with comprehensive attribute data
- **Spatial Precision**: High-precision field boundaries with UUID-based field identification

#### DST Workflow Integration
- **Data Source**: Official Danish agricultural statistics from Danmarks Statistik
- **Statistical Tables**: 4 comprehensive tables covering different agricultural sectors
  - **HST77**: Harvest statistics for major grains (wheat, barley, rye, oats)
  - **GARTN1**: Horticulture production data for fruits and vegetables
  - **FRO**: Seed production statistics for specialized crops
  - **HALM1**: Straw production data for biomass and feed calculations
- **Yield Data**: Regional and national yield statistics with temporal coverage
- **Processing**: JSONSTAT format parsing with multi-dimensional statistical data integration

### Advanced Crop Mapping System
- **Comprehensive Mapping**: 200+ field crop types mapped to DST statistical categories
- **Match Quality Assessment**: Perfect, good, and approximate matches with quality indicators
- **Multi-Table Integration**: Intelligent routing of crop types to appropriate DST statistical tables
- **Regional Fallbacks**: Regional yield data with national fallbacks for comprehensive coverage
- **Mapping Validation**: Field count validation and mapping quality assessment

---

## Data Processing Steps

### 🥇 Gold Layer: Advanced Field Production Estimation (Unified Pipeline Only)
**What happens**: We combine individual agricultural field data with official Danish yield statistics to create comprehensive field-level production estimates
**Why**: Agricultural analysis requires field-level production estimates for policy development, research, and business intelligence

**Specific processing**:

#### High-Performance Resource Management
- **Memory Optimization**: Conservative 8GB memory allocation (50% of 16GB) with 4GB OS buffer for GitHub Actions
- **CPU Optimization**: 2-thread processing (50% of 4 cores) to reduce memory pressure and ensure stability
- **Disk Management**: 10GB temporary storage allocation (71% of 14GB SSD) with aggressive cleanup
- **Batch Processing**: Single-year processing (years_per_batch = 1) for optimal memory control
- **Emergency Fallbacks**: Automatic memory monitoring with emergency cleanup and batch size reduction

#### Streaming Data Architecture
- **Year-by-Year Processing**: Individual year processing to minimize memory footprint and enable parallel matrix execution
- **GCS Streaming**: Direct GCS-to-DuckDB streaming without in-memory loading for large datasets
- **Temporary File Management**: Optimized temporary file handling with automatic cleanup
- **Resource Monitoring**: Real-time memory monitoring with automatic threshold management
- **Checkpoint Management**: Frequent checkpoints (256MB threshold) for memory-constrained environments

#### Advanced Spatial Processing with DST Zones
- **DST Zone Integration**: Spatial join with Danish statistical zones for regional yield assignment
- **Batched Spatial Joins**: Memory-efficient spatial processing (50,000 records per batch) for GitHub Actions
- **SPATIAL_JOIN Optimization**: DuckDB SPATIAL_JOIN operator utilization for optimal performance
- **Coordinate System Handling**: Proper geometry processing with spatial validation and transformation
- **Regional Assignment**: Assignment of fields to DST statistical regions for yield lookup

#### Comprehensive Crop-Yield Mapping
- **Intelligent Crop Mapping**: 200+ field crop types mapped to DST statistical categories using comprehensive mapping table
- **Multi-Table Yield Integration**: Sophisticated routing of crops to appropriate DST tables (HST77, GARTN1, FRO, HALM1)
- **Regional Yield Prioritization**: Regional yields preferred over national averages for improved accuracy
- **Match Quality Tracking**: Quality assessment (perfect, good, approximate) for yield estimation reliability
- **Fallback Yield System**: Conservative national average fallbacks for unmapped crops

#### Production Calculation Engine
- **Field-Level Calculations**: Individual field production estimates using area × yield calculations
- **Unit Standardization**: Standardized production units (hectograms - hkg) across all crop types
- **Yield Method Tracking**: Detailed tracking of yield estimation methods for transparency and validation
- **Quality Assessment**: Comprehensive coverage assessment with minimum threshold validation (30%)
- **Statistical Validation**: Multi-level validation of production estimates and coverage rates

#### Matrix Job Support and Parallel Processing
- **Single-Year Matrix Jobs**: Support for parallel processing of individual years through GitHub Actions matrix
- **Year-Specific Output**: Year-specific output paths for matrix jobs with consolidated results
- **Resource Scaling**: Dynamic resource allocation based on processing requirements
- **Time Management**: GitHub Actions time limit monitoring (5.5-hour safety margin)
- **Parallel Optimization**: Optimized for parallel execution across multiple years

#### Advanced Quality Assurance
- **Coverage Validation**: Minimum 30% yield coverage requirement with quality warnings
- **Data Completeness Assessment**: Comprehensive validation of field-yield matching success rates
- **Regional Coverage Analysis**: Year-by-year and region-by-region coverage assessment
- **Production Validation**: Statistical validation of production estimates against known benchmarks
- **Error Recovery**: Robust error handling with memory-aware recovery strategies

**Output**: Comprehensive field-level production estimates with yield methodology tracking and quality assessment

---

## Workflow Schedule and Execution

### Monthly Dependent Processing
- **Schedule**: 1st of every month at 1 AM UTC (dependent batch, priority 11)
- **Execution Type**: Automated monthly processing after foundation workflows complete (FVM WFS, DST)
- **Processing Duration**: ~2.5 hours for complete field production estimation (estimated 150 minutes)
- **Dependencies**: Requires FVM WFS field data and DST yield statistics from monthly foundation processing
- **Matrix Support**: Parallel year processing capability through dedicated matrix workflows

### Processing Performance and Optimization
- **Data Volume**: ~2.8M agricultural fields processed with production estimates annually
- **Memory Management**: Conservative memory allocation (8GB) with emergency monitoring and cleanup
- **Spatial Processing**: High-performance spatial joins using DuckDB SPATIAL_JOIN operator
- **Batch Optimization**: Memory-efficient batched processing (50,000 spatial join batch size)
- **Resource Monitoring**: Real-time memory monitoring with automatic threshold management

### Advanced Processing Features
- **Year-by-Year Processing**: Individual year processing for memory control and parallel matrix execution
- **Streaming Architecture**: GCS streaming for large dataset processing without memory loading
- **Emergency Fallbacks**: Automatic memory monitoring with emergency cleanup and batch size reduction
- **Checkpoint Management**: Frequent checkpoints (256MB) for memory-constrained GitHub Actions environment
- **Quality Monitoring**: Real-time validation of production estimation coverage and quality

### Matrix Job Architecture
- **Parallel Year Processing**: Support for processing individual years in parallel through matrix workflows
- **Resource Scaling**: Dynamic resource allocation based on year-specific processing requirements
- **Time Management**: GitHub Actions time limit monitoring with 5.5-hour safety margin
- **Year-Specific Output**: Separate output paths for matrix jobs with consolidated final results
- **Fault Tolerance**: Robust error handling with year-level isolation for processing failures

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | >95% field coverage with production estimates across all processed years |
| **Accuracy** | Good | Official DST yield data with comprehensive crop mapping (200+ crop types) |
| **Temporal Coverage** | Excellent | Multi-year historical coverage (2005-present) with annual updates |
| **Spatial Precision** | Excellent | Individual field-level estimates with precise geographic boundaries |

### Known Issues and Limitations

#### Data Integration and Processing Constraints
- **Memory Requirements**: High memory requirements (8GB minimum) for large-scale field processing
- **Processing Duration**: Extended processing time (~2.5 hours) for complete production estimation
- **Yield Data Lag**: DST yield statistics may have 1-2 year lag from field boundary data
- **Regional Yield Availability**: Some crops may lack regional yield data, requiring national averages

#### Crop Mapping and Yield Estimation Limitations
- **Crop Mapping Coverage**: ~200 crop types mapped, but some specialized crops may use fallback yields
- **Yield Estimation Methods**: Mix of regional, national, and fallback yields with varying accuracy levels
- **Organic vs. Conventional**: Limited differentiation between organic and conventional yield estimates
- **Temporal Alignment**: Field crop data and yield statistics may have different temporal reference points

#### Technical and Resource Limitations
- **GitHub Actions Constraints**: Processing optimized for 16GB RAM, 4 CPU, 14GB SSD GitHub Actions environment
- **Single-Year Processing**: Memory constraints require year-by-year processing rather than multi-year batches
- **Spatial Join Performance**: Large spatial joins require batched processing for memory management
- **Emergency Fallbacks**: Memory pressure may trigger emergency cleanup and batch size reductions

### Quality Assurance Features
- **Coverage Validation**: Minimum 30% yield coverage requirement with comprehensive quality warnings
- **Match Quality Tracking**: Detailed tracking of crop-to-DST mapping quality (perfect, good, approximate)
- **Regional Coverage Assessment**: Year-by-year and region-by-region production coverage analysis
- **Statistical Validation**: Multi-level validation of production estimates against known agricultural benchmarks

### Recommended Uses
✅ **This data is excellent for**:
- Field-level agricultural production analysis and policy development with spatial precision
- Regional agricultural productivity assessment and comparative analysis across Danish regions
- Crop-specific production estimation for agricultural research and market intelligence
- Historical production trend analysis with multi-year temporal coverage and statistical validation
- Agricultural business intelligence for investment decisions and market analysis

⚠️ **Use with caution for**:
- Real-time production analysis - Monthly processing with potential 1-2 year yield data lag
- Organic vs. conventional production differentiation - Limited organic-specific yield data
- Specialized crop analysis - Some crops may use approximate mappings or fallback yields

❌ **Not recommended for**:
- Daily production monitoring - Monthly updates, not real-time production tracking
- International production comparisons without context - Danish-specific crop types and yield methodologies
- Individual farm business planning without validation - Estimates based on regional averages, not farm-specific conditions

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **What is the estimated production for wheat fields in Jutland for 2023?** - Regional production analysis with crop-specific filtering
2. **How has barley production changed across different Danish regions over the past decade?** - Multi-year temporal analysis with regional comparison
3. **Which regions have the highest productivity for organic farming?** - Regional productivity analysis with organic farming focus
4. **What are the production estimates for fields owned by specific companies?** - Company-based production analysis using CVR-based field identification

### Example Analyses
#### Regional Crop Production Analysis
**Question**: What are the regional differences in wheat production across Denmark for 2023?
**Data Used**: Field production estimates filtered by wheat crop types with regional DST zone assignment
**Method**: Spatial aggregation by DST regions with production summation and yield averaging
**Output**: Regional wheat production totals with average yields and field count statistics
**Limitations**: Production estimates based on statistical yields, not actual harvest data; regional yields may not reflect local conditions

#### Multi-Year Production Trend Analysis
**Question**: How has total agricultural production changed in Denmark from 2015-2023?
**Data Used**: Historical field production estimates across all crop types with temporal aggregation
**Method**: Year-over-year production summation with crop-specific trend analysis and statistical validation
**Output**: Multi-year production trends with crop-specific breakdowns and regional variation analysis
**Limitations**: Yield data lag may affect recent years; crop mapping changes over time may impact consistency

#### Company Agricultural Portfolio Analysis
**Question**: What is the total estimated production for agricultural companies with >1000 hectares?
**Data Used**: Field production estimates aggregated by CVR numbers with area and production summation
**Method**: Company-level aggregation with production totals, crop diversity analysis, and regional distribution
**Output**: Company agricultural portfolios with production estimates, crop diversity, and geographic distribution
**Limitations**: CVR-based identification may miss some ownership structures; estimates based on regional yields, not company-specific practices

### Data Access
- **Research Access**: Field production estimates for academic agricultural research and policy analysis
- **Business Access**: Production data for agricultural market intelligence and investment analysis
- **Policy Access**: Field-level production data for agricultural policy development and subsidy calculations
- **Regional Access**: Regional production statistics for administrative planning and resource allocation

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Field Production Estimates (Gold Layer)

**Comprehensive Field-Level Production Dataset**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
field_id | VARCHAR | FVM field identifier | "12345"
block_id | VARCHAR | FVM block identifier | "67890"
cvr_number | VARCHAR | Company registration number | "12345678"
year | INTEGER | Production year | 2023
area_ha | DOUBLE | Field area in hectares | 12.5
crop_type | VARCHAR | Crop type from FVM data | "Vinterhvede"
organic_farming | BOOLEAN | Organic farming status | false
landsdel_code | VARCHAR | DST region code | "DK01"
landsdel_name | VARCHAR | DST region name | "Hovedstaden"
dst_regions | VARCHAR | DST statistical region | "Region Hovedstaden"
yield_estimate_hkg_ha | DOUBLE | Yield estimate (hectograms per hectare) | 85.2
yield_estimation_method | VARCHAR | Yield estimation method | "dst_hst77_regional_perfect"
production_estimate_hkg | DOUBLE | Production estimate (hectograms) | 1065.0
production_unit | VARCHAR | Production unit | "hkg"
created_at | TIMESTAMP | Processing timestamp | "2024-01-15T10:30:00Z"
field_uuid | VARCHAR | Unique field identifier | "abc-123-def-456"
primary_field_id | VARCHAR | Primary field reference | "abc-123-def-456"
```

### Storage Locations
- **Gold Output (Normal)**: `gs://landbrugsdata-raw-data/gold/field_production/latest/data.parquet`
- **Gold Output (Matrix)**: `gs://landbrugsdata-raw-data/gold/field_production_{year}/{timestamp}/data.parquet`

### Processing Infrastructure
- **Platform**: Automated monthly execution as dependent data source with matrix job support
- **Resources**: 8GB RAM allocation, 2-thread processing, 10GB temporary storage
- **Dependencies**: FVM WFS silver data, DST silver data (4 statistical tables)
- **Performance**: ~2.5 hours for complete field production estimation (150 minutes estimated)

### Crop Mapping System
#### DST Field Crop Mapping Table
```python
# Example crop mappings
DST_FIELD_MAPPING = {
    "Vinterhvede": {
        "dst_table": "HST77",
        "dst_category": "Vinterhvede", 
        "match_quality": "perfect",
        "field_count": 317000
    },
    "Vårbyg": {
        "dst_table": "HST77",
        "dst_category": "Vårbyg",
        "match_quality": "perfect", 
        "field_count": 478000
    }
}
```

#### Yield Application Logic
```sql
-- Regional yield application (preferred)
UPDATE year_production_estimates
SET yield_estimate_hkg_ha = hst77.harvest_value,
    yield_estimation_method = 'dst_hst77_regional_perfect',
    production_estimate_hkg = area_ha * hst77.harvest_value
FROM dst_dst_hst77 hst77
WHERE hst77.area_name = year_production_estimates.dst_regions
  AND hst77.time_period = CAST(year_production_estimates.year AS VARCHAR)
  AND LOWER(hst77.crop_name) = LOWER('Vinterhvede')
  AND year_production_estimates.crop_type = 'Vinterhvede'
```

### Resource Optimization Features
- **Memory Management**: Conservative 8GB allocation with emergency monitoring and cleanup
- **Batch Processing**: Single-year processing with 50,000-record spatial join batches
- **Streaming Processing**: Direct GCS-to-DuckDB streaming without memory loading
- **Checkpoint Management**: Frequent checkpoints (256MB) for memory-constrained environments
- **Emergency Fallbacks**: Automatic batch size reduction and emergency cleanup on memory pressure

### Quality Assurance Features
- **Coverage Validation**: Minimum 30% yield coverage requirement with comprehensive warnings
- **Match Quality Tracking**: Detailed tracking of crop-to-DST mapping quality assessment
- **Regional Validation**: Year-by-year and region-by-region coverage analysis and validation
- **Statistical Validation**: Multi-level validation of production estimates against agricultural benchmarks
- **Processing Statistics**: Detailed logging of processing performance and quality metrics

### Matrix Job Support
- **Parallel Processing**: Individual year processing through GitHub Actions matrix workflows
- **Year-Specific Paths**: Separate output paths for matrix jobs with timestamp-based organization
- **Resource Scaling**: Dynamic resource allocation based on year-specific processing requirements
- **Time Management**: GitHub Actions time limit monitoring with 5.5-hour safety margin
- **Fault Tolerance**: Year-level isolation with robust error handling and recovery strategies

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Agricultural Analytics Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Field Production" label
- **Performance Issues**: Contact infrastructure team for resource optimization and performance concerns
- **Crop Mapping Issues**: Contact agricultural data team for crop-to-DST mapping improvements

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when FVM field data or DST yield data structures change
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural production data accessible and trustworthy for research, policy development, and business intelligence.*

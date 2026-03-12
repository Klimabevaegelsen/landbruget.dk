# Worker Safety Workflow

> **Manual Processing**: Agricultural workplace injury and safety data from Danish authorities

---

## What This Workflow Does

The Worker Safety workflow processes agricultural workplace injury data from the Danish Working Environment Authority (Arbejdstilsynet). This workflow transforms raw injury statistics from the EASY (Electronic Accident Statistics System) into structured, analysis-ready data showing workplace safety trends across Danish agricultural companies.

### Why This Data Matters
- **Safety Monitoring**: Track workplace injury trends in Danish agriculture to identify safety risks
- **Regulatory Oversight**: Support Danish Working Environment Authority in monitoring agricultural workplace safety
- **Industry Analysis**: Identify patterns in agricultural workplace injuries by company and injury type
- **Policy Development**: Inform workplace safety policies and intervention strategies
- **Risk Assessment**: Enable risk-based approaches to workplace safety inspections and support

### Key Statistics
- **Data Coverage**: ~190 agricultural companies with workplace injury data (2020-2024)
- **Detailed Analysis**: 34 companies with injury type breakdowns across multiple categories
- **Privacy Protection**: Handles privacy-protected values ("<5" injuries) with appropriate range representations
- **Processing**: Gold layer only (structured analysis of drive pipeline silver data)
- **Temporal Scope**: 2020-2024 with annual injury statistics

---

## Data Sources and Collection

### Official Sources
This workflow processes data from the Danish Working Environment Authority:

| Data Source | Purpose | Coverage | Format |
|-------------|---------|----------|--------|
| **EASY Database** | Electronic Accident Statistics System | Agricultural sector injuries | Structured reports |
| **Main Dataset** | Total injury counts by CVR and year | 190 companies (2020-2024) | Parquet (mv file) |
| **Injury Type Dataset** | Detailed injury type breakdowns | 34 companies (2020-2024) | Parquet (skadeart file) |

### Data Collection Process

#### Drive Data Pipeline Integration
- **Source System**: Danish Working Environment Authority EASY database
- **Collection Method**: Processed through Drive Data Pipeline as silver layer data
- **Data Format**: Two complementary parquet files with company-level injury statistics
- **Update Frequency**: Annual updates reflecting workplace injury reporting cycles
- **Privacy Compliance**: Automatic handling of privacy-protected values ("<5" injuries)

#### Data Structure Overview
- **Main Data (190 companies)**: Total injury counts by CVR number and year
- **Detailed Data (34 companies)**: Injury counts broken down by specific injury types
- **Temporal Coverage**: 2020-2024 with annual granularity
- **Privacy Protection**: Values "<5" replaced with range "1-5" to preserve analytical utility

### Data Privacy and Compliance
- **Privacy Protection**: Automatic handling of Danish privacy regulations for small counts
- **Company Identification**: Uses CVR numbers for regulatory and analytical purposes
- **Anonymization**: Privacy-protected values handled as ranges rather than exact counts
- **Legal Compliance**: Data processing complies with Danish workplace safety reporting requirements
- **Access Controls**: Restricted access for workplace safety analysis and regulatory oversight

---

## Data Processing Steps

### 🥇 Gold Layer: Advanced Data Normalization and Privacy Handling
**What happens**: We transform wide-format injury data into normalized time-series format with proper privacy protection
**Why**: Raw data comes in pivot table format (columns for years) and needs normalization for analytical use, plus privacy compliance

**Core Processing Components**:

#### 1. Dual Dataset Integration
- **Main Dataset Processing**: 190 companies with total injury counts across 2020-2024
- **Detailed Dataset Processing**: 34 companies with injury type breakdowns
- **Smart Deduplication**: Prevents double-counting by excluding main totals where detailed data exists
- **Coverage Optimization**: Maximizes data completeness while avoiding analytical conflicts

#### 2. Data Unpivoting and Normalization
- **Wide-to-Long Transformation**: Converts year columns (2020, 2021, 2022, 2023, 2024) to rows
- **Injury Type Preservation**: Maintains detailed injury type classifications where available
- **CVR Standardization**: Ensures consistent company identification across datasets
- **Temporal Alignment**: Creates proper time-series structure for trend analysis

#### 3. Privacy-Compliant Value Handling
- **Privacy Detection**: Identifies "<5" values indicating privacy-protected small counts
- **Range Replacement**: Converts "<5" to "1-5" preserving analytical utility while maintaining privacy
- **Zero Handling**: Distinguishes between actual zeros and missing/null values
- **Non-Zero Filtering**: Includes only records with actual injuries for meaningful analysis

#### 4. Injury Type Classification
- **Detailed Categories**: Preserves specific injury type classifications for 34 companies
- **Total Aggregation**: Uses "TOTAL" category for companies without detailed breakdowns
- **Consistent Labeling**: Ensures injury type labels are standardized across the dataset
- **Coverage Tracking**: Monitors which companies have detailed vs. total-only data

**Quality Controls**:
- **Duplicate Prevention**: Ensures companies appear only once per year-injury type combination
- **Privacy Compliance**: Verifies all privacy-protected values are properly handled
- **Data Completeness**: Tracks coverage across companies, years, and injury types
- **Consistency Validation**: Ensures data integrity across main and detailed datasets

**Output**: Clean, normalized workplace injury dataset with CVR, year, injury_type, and injury_count columns

---

## Workflow Schedule and Execution

### Manual Processing Schedule
- **Execution Type**: Manual trigger only (not automated)
- **Typical Usage**: When updated workplace injury data becomes available from authorities
- **Processing Duration**: ~10-15 minutes for complete dataset normalization
- **Dependencies**: Requires worker safety silver data from Drive Data Pipeline
- **Data Refresh**: Annual updates aligned with Danish workplace injury reporting cycles

### Processing Performance
- **Data Volume**: ~190 companies × 5 years × injury types = several thousand records
- **Memory Usage**: Moderate (~4-8GB) for data transformation and normalization
- **Storage**: ~10-50MB for processed parquet output depending on injury detail level
- **Processing Speed**: Fast transformation due to structured input data

### Advanced Features
- **Privacy-Aware Processing**: Automatic detection and appropriate handling of privacy-protected values
- **Dual Dataset Integration**: Intelligent merging of total and detailed injury data
- **Flexible Time Range**: Configurable start/end years for analysis periods
- **Quality Reporting**: Comprehensive logging of data coverage and privacy handling statistics

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Good | 190 companies with total data, 34 with detailed injury types |
| **Accuracy** | Excellent | Official Danish Working Environment Authority data from EASY system |
| **Timeliness** | Good | Annual updates reflecting workplace injury reporting cycles |
| **Privacy Compliance** | Excellent | Proper handling of privacy-protected values with range preservation |

### Known Issues and Limitations

#### Data Coverage Limitations
- **Detailed Breakdown**: Only 34 of 190 companies have injury type breakdowns
- **Industry Scope**: Limited to agricultural sector companies reporting to EASY system
- **Reporting Compliance**: Coverage depends on company compliance with injury reporting requirements
- **Small Company Representation**: Privacy protection may limit analysis of smaller agricultural operations

#### Privacy and Statistical Considerations
- **Privacy Ranges**: "<5" values represented as "1-5" ranges affect precise statistical analysis
- **Aggregation Constraints**: Privacy protection may limit detailed trend analysis for some companies
- **Comparative Analysis**: Different levels of detail across companies may complicate sector-wide comparisons
- **Temporal Consistency**: Changes in reporting requirements may affect year-over-year comparisons

#### Methodological Limitations
- **Injury Classification**: Injury type categories may evolve over time affecting longitudinal analysis
- **Reporting Bias**: Companies may have different injury reporting practices affecting comparability
- **Severity Weighting**: Data shows injury counts but not severity or lost-time information
- **Causal Analysis**: Data shows patterns but doesn't establish causation for injury trends

### Recommended Uses
✅ **This data is excellent for**:
- Agricultural workplace safety trend analysis and benchmarking
- Regulatory oversight and compliance monitoring by Danish authorities
- Industry-wide safety performance assessment and policy development
- Risk-based inspection planning and resource allocation
- Research on agricultural workplace safety patterns and interventions

⚠️ **Use with caution for**:
- Precise statistical analysis involving privacy-protected values (use ranges appropriately)
- Company-to-company comparisons without considering reporting differences
- Trend analysis without accounting for potential reporting requirement changes

❌ **Not recommended for**:
- Individual company enforcement actions - Requires additional regulatory investigation
- Detailed injury severity analysis - Data shows counts, not severity or lost time
- Predictive modeling without external risk factors - Limited to reported injury patterns

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Which agricultural companies have the highest workplace injury rates?** - Company ranking analysis with proper privacy consideration
2. **What are the most common injury types in Danish agriculture?** - Injury type analysis for companies with detailed breakdowns
3. **How have agricultural workplace injuries trended over 2020-2024?** - Time series analysis of injury patterns and safety improvements
4. **Which companies might benefit from targeted safety interventions?** - Risk assessment based on injury patterns and trends

### Example Analyses
#### Safety Trend Assessment
**Question**: How have workplace injuries in Danish agriculture changed from 2020 to 2024?
**Data Used**: Normalized injury counts by year across all companies
**Method**: Time series analysis with appropriate handling of privacy-protected values
**Output**: Trend analysis showing overall safety improvements or areas of concern
**Limitations**: Privacy ranges affect precise trend calculations; reporting consistency may vary

#### Injury Type Risk Analysis
**Question**: What are the most common workplace injury types in agricultural companies?
**Data Used**: Detailed injury type data for 34 companies with breakdowns
**Method**: Injury type frequency analysis with privacy-compliant aggregation
**Output**: Risk profile showing most common injury categories requiring prevention focus
**Limitations**: Limited to companies with detailed reporting; may not represent entire sector

#### Company Safety Benchmarking
**Question**: How do individual companies compare to sector averages for workplace safety?
**Data Used**: Company-level injury rates with privacy protection considerations
**Method**: Comparative analysis using appropriate statistical methods for privacy ranges
**Output**: Company safety performance relative to sector benchmarks
**Limitations**: Privacy protection limits precision; different reporting levels affect comparability

### Data Access
- **Regulatory Access**: Full dataset for Danish Working Environment Authority oversight
- **Company Access**: Company-specific safety data for internal safety management
- **Research Access**: Anonymized aggregate data for academic workplace safety research
- **Public Access**: Sector-level safety statistics and trend summaries

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Worker Safety Clean Dataset
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
cvr_number | BIGINT | Danish company registration number | 12345678
year | INTEGER | Injury reporting year | 2023
injury_type | VARCHAR | Injury category or "TOTAL" | "TOTAL" or specific type
injury_count | VARCHAR | Injury count or privacy range | "3" or "1-5"
```

#### Injury Type Categories
- **TOTAL**: Aggregate injury count (for companies without detailed breakdown)
- **Specific Types**: Detailed injury classifications (available for 34 companies)
- **Privacy Values**: "1-5" range for privacy-protected small counts
- **Zero Values**: "0" for years with no reported injuries

#### Data Coverage Summary
- **Total Companies**: 190 agricultural companies
- **Detailed Companies**: 34 companies with injury type breakdowns  
- **Time Period**: 2020-2024 (5 years)
- **Privacy Records**: Automatically handled with range preservation

### Storage Locations
- **Gold Output**: `gs://landbruget-data/gold/worker_safety/{timestamp}/`
- **Clean Dataset**: `worker_safety_clean.parquet`
- **Source Silver**: `gs://landbruget-data/silver/worker safety/{timestamp}/`
- **Input Files**: `worker_safety_2020-2024_mv.parquet`, `worker_safety_2020-2024_skadeart.parquet`

### Processing Infrastructure
- **Platform**: Manual execution via GitHub Actions
- **Resources**: 8GB RAM, 4 threads for data transformation
- **Dependencies**: Worker safety silver data from Drive Data Pipeline
- **Performance**: ~15 minutes for complete dataset normalization
- **Memory Configuration**: Optimized for DuckDB-based data transformation

### Privacy Protection Features
- **Automatic Detection**: Identifies "<5" privacy-protected values
- **Range Preservation**: Converts to "1-5" maintaining analytical utility
- **Compliance Tracking**: Reports count of privacy-protected records
- **Statistical Compatibility**: Enables appropriate statistical analysis of ranges

### Data Integration Details
```sql
-- Example of dual dataset integration logic
WITH detailed_cvrs AS (
    SELECT DISTINCT cvr_number FROM injury_type_data
),
main_unpivoted AS (
    SELECT cvr_number, 'TOTAL' as injury_type, year, injury_count
    FROM main_data m
    LEFT JOIN detailed_cvrs d ON m.cvr_number = d.cvr_number
    WHERE d.cvr_number IS NULL  -- Exclude companies with detailed data
)
```

### Quality Metrics Tracking
- **Total Records**: Complete record count after normalization
- **Unique CVR Numbers**: Company coverage verification
- **Detailed vs Total**: Distribution of detailed vs aggregate data
- **Privacy Records**: Count of privacy-protected values
- **Injury Types**: Unique injury type categories identified

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Agricultural Safety Data Team
- **Response Time**: 2-3 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Worker Safety" label
- **Privacy Concerns**: Contact data protection team for privacy-related questions
- **Access Problems**: Submit access requests via appropriate channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when data sources or privacy requirements change
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural workplace safety data accessible and trustworthy.*

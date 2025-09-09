# Work Permits Workflow

> **Manual Processing**: Agricultural work permit data from Danish visa statistics

---

## What This Workflow Does

The Work Permits workflow processes agricultural work permit data from Danish "Landbrugsvisum" (Agricultural Visa) statistics. This workflow takes structured work permit data extracted from PDF documents by the Drive Data Pipeline and makes it available for analysis, showing patterns of international agricultural labor in Denmark by company, nationality, and year.

### Why This Data Matters
- **Labor Market Analysis**: Track international agricultural labor trends and patterns in Denmark
- **Policy Development**: Support agricultural labor policy decisions and visa program assessment
- **Economic Impact**: Understand the role of international workers in Danish agricultural operations
- **Compliance Monitoring**: Monitor agricultural companies' use of international work permits
- **Research Support**: Enable analysis of agricultural labor migration patterns and economic impacts

### Key Statistics
- **Data Coverage**: Agricultural companies using international work permits (2019-2025)
- **Nationality Tracking**: 23 tracked nationalities including Ukraine, India, Brazil, Vietnam, and others
- **Temporal Scope**: 2019-2025 with annual permit statistics
- **Processing**: Gold layer only (final validation and formatting of Drive Pipeline silver data)
- **Data Source**: Danish agricultural visa statistics from official PDF documents

---

## Data Sources and Collection

### Official Sources
This workflow processes data from Danish agricultural visa statistics:

| Data Source | Purpose | Coverage | Format |
|-------------|---------|----------|--------|
| **Landbrugsvisum PDFs** | Danish Agricultural Visa statistics | Company-level permit data | PDF documents |
| **Drive Pipeline Silver** | Structured permit data | Extracted and cleaned | Parquet files |
| **Nationality Tracking** | 23 country classifications | Major agricultural labor sources | Standardized names |

### Data Collection Process

#### Drive Data Pipeline Integration
- **Source Documents**: Official Danish agricultural visa statistics in PDF format
- **PDF Processing**: Advanced PDF parsing using `pdfplumber` for pivot table extraction
- **Data Extraction**: Company CVR numbers, nationalities, years, and permit counts
- **Silver Processing**: Drive Data Pipeline transforms PDFs to structured parquet data
- **Gold Validation**: Final validation and formatting for analytical use

#### Nationality Classifications
The system tracks 23 major agricultural labor source countries:
- **Eastern Europe**: Ukraine, Poland, Romania, Belarus, Albania, North Macedonia
- **South Asia**: India, Bangladesh, Pakistan
- **Southeast Asia**: Vietnam, Philippines, Thailand
- **Africa**: Uganda, Tanzania, South Africa, Ethiopia, Kenya, Ghana, Morocco
- **Other**: Brazil, China, Kyrgyzstan, Uzbekistan, Turkey

#### Data Structure and Coverage
- **Company Identification**: CVR numbers for Danish agricultural companies
- **Temporal Coverage**: Annual permit statistics from 2019-2025
- **Permit Types**: First-time agricultural work permits (primary focus)
- **Geographic Scope**: Complete national coverage of Danish agricultural visa system

### Data Privacy and Compliance
- **Company Data**: Uses CVR numbers for legitimate agricultural labor analysis
- **Anonymization**: No personal worker information - only aggregate company statistics
- **Legal Compliance**: Data derived from official Danish agricultural visa statistics
- **Access Controls**: Restricted access for labor market analysis and policy research

---

## Data Processing Steps

### 🥇 Gold Layer: Data Validation and Final Formatting
**What happens**: We validate and format work permit data from Drive Pipeline silver layer for analytical use
**Why**: Ensures data quality and consistency for agricultural labor analysis while maintaining proper data structure

**Core Processing Components**:

#### 1. Silver Data Integration
- **Pattern Matching**: Locates work permit parquet files from Drive Data Pipeline silver output
- **Schema Validation**: Ensures consistent data structure (company_id, year, nationality, first_permits_count)
- **Temporal Filtering**: Applies configurable year range filtering (2019-2025 default)
- **Volume Validation**: Applies reasonable limits for permit counts per record (max 1000)

#### 2. Data Quality Validation
- **Completeness Checks**: Validates presence of required fields (company_id, year, nationality)
- **Range Validation**: Ensures permit counts are positive and within reasonable limits
- **Year Validation**: Confirms years fall within expected range (2019-2025)
- **Data Cleaning**: Removes invalid records with missing or malformed data

#### 3. Statistical Analysis and Reporting
- **Coverage Statistics**: Reports total records, unique companies, and nationalities
- **Temporal Analysis**: Tracks permit trends across years
- **Volume Analysis**: Calculates total permits and averages per record
- **Quality Metrics**: Reports data validation results and cleaning actions

#### 4. Output Generation
- **Main Dataset**: Clean work permit records with company, year, nationality, and permit counts
- **Summary Statistics**: Processing metadata and aggregate statistics
- **Quality Reports**: Data validation results and coverage metrics

**Quality Controls**:
- **Data Validation**: Comprehensive checks for missing, invalid, or out-of-range values
- **Statistical Validation**: Reasonable permit count limits and consistency checks
- **Coverage Tracking**: Monitoring of companies, nationalities, and temporal coverage
- **Error Handling**: Graceful handling of missing or corrupted silver data

**Output**: Validated work permit dataset ready for agricultural labor analysis and database insertion

---

## Workflow Schedule and Execution

### Manual Processing Schedule
- **Execution Type**: Manual trigger only (not automated)
- **Typical Usage**: When updated agricultural visa statistics become available
- **Processing Duration**: ~5-10 minutes for data validation and formatting
- **Dependencies**: Requires work permits silver data from Drive Data Pipeline
- **Data Refresh**: As new PDF documents become available from Danish authorities

### Processing Performance
- **Data Volume**: Variable based on agricultural labor demand (hundreds to thousands of records)
- **Memory Usage**: Low (~4GB) for data validation and formatting
- **Storage**: ~5-20MB for processed parquet output depending on permit volume
- **Processing Speed**: Fast validation due to pre-processed silver data

### Advanced Features
- **Flexible Time Range**: Configurable start/end years for analysis periods
- **Data Validation**: Comprehensive quality checks with detailed reporting
- **Statistical Summaries**: Automatic generation of coverage and trend statistics
- **Error Recovery**: Graceful handling of missing or corrupted input data

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Good | Coverage depends on PDF document availability and processing success |
| **Accuracy** | Excellent | Official Danish agricultural visa statistics from government sources |
| **Timeliness** | Good | Updates dependent on official document release schedules |
| **Consistency** | Excellent | Standardized processing with comprehensive validation |

### Known Issues and Limitations

#### Data Source Limitations
- **PDF Dependency**: Data quality depends on successful PDF parsing of official documents
- **Document Availability**: Coverage limited by availability of official statistical reports
- **Reporting Delays**: Updates dependent on government publication schedules
- **Format Changes**: PDF format changes may require transformer updates

#### Processing Constraints
- **Nationality Classification**: Limited to 23 pre-defined country categories
- **Permit Types**: Focuses on first-time permits; may not capture all visa categories
- **Company Coverage**: Only includes companies explicitly listed in official statistics
- **Temporal Gaps**: Missing years or partial coverage if source documents unavailable

#### Analytical Limitations
- **Aggregation Level**: Company-level data doesn't show individual worker details
- **Permit Duration**: Data shows permit issuance, not duration or renewal patterns
- **Seasonal Patterns**: Annual data may not capture seasonal agricultural labor patterns
- **Compliance Status**: Data shows permits issued, not compliance or actual employment

### Recommended Uses
✅ **This data is excellent for**:
- Agricultural labor market trend analysis and policy development
- International agricultural worker flow analysis by nationality and company
- Economic impact assessment of agricultural visa programs
- Research on agricultural labor migration patterns and regional distribution
- Compliance monitoring and oversight of agricultural work permit usage

⚠️ **Use with caution for**:
- Individual company enforcement actions - Requires additional regulatory investigation
- Seasonal labor analysis - Annual data may not capture seasonal patterns
- Worker outcome analysis - No data on employment duration or conditions

❌ **Not recommended for**:
- Individual worker tracking or privacy-sensitive analysis
- Real-time labor market monitoring - Data updated periodically, not continuously
- Detailed compliance assessment - Requires additional regulatory data sources

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Which agricultural companies rely most heavily on international workers?** - Company ranking by permit volume and nationality diversity
2. **What are the trends in international agricultural labor over time?** - Time series analysis of permit patterns by nationality and total volume
3. **Which nationalities are most prominent in Danish agricultural labor?** - Nationality analysis showing major source countries for agricultural workers
4. **How has the agricultural visa program evolved since 2019?** - Policy impact analysis through permit volume and pattern changes

### Example Analyses
#### Agricultural Labor Dependency Analysis
**Question**: Which Danish agricultural companies are most dependent on international workers?
**Data Used**: Company-level permit counts across years and nationalities
**Method**: Ranking analysis by total permits and consistency across years
**Output**: Company profiles showing international labor dependency patterns
**Limitations**: Doesn't account for company size or total workforce; permit issuance vs. actual employment

#### Nationality Flow Analysis
**Question**: How have agricultural worker source countries changed over 2019-2025?
**Data Used**: Permit counts by nationality and year across all companies
**Method**: Time series analysis of nationality composition and trends
**Output**: Migration pattern analysis showing shifts in agricultural labor sources
**Limitations**: Reflects permits issued, not actual worker flows or employment outcomes

#### Policy Impact Assessment
**Question**: How did policy changes affect agricultural visa patterns?
**Data Used**: Year-over-year permit changes by company and nationality
**Method**: Trend analysis with policy timeline correlation
**Output**: Assessment of policy impacts on agricultural labor patterns
**Limitations**: Correlation analysis; other economic factors may influence patterns

### Data Access
- **Policy Access**: Full dataset for agricultural labor policy development and analysis
- **Research Access**: Aggregate data for academic research on agricultural labor migration
- **Company Access**: Company-specific permit data for internal planning and compliance
- **Public Access**: Aggregate statistics on agricultural labor trends and patterns

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Work Permits Dataset
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
company_id | VARCHAR | Danish company CVR number | "12345678"
year | INTEGER | Permit issuance year | 2023
nationality | VARCHAR | Worker nationality/country | "Ukraine"
first_permits_count | INTEGER | Number of first-time permits | 15
source_file | VARCHAR | Source PDF document name | "landbrugsvisum_2023.pdf"
extracted_at | TIMESTAMP | PDF extraction timestamp | "2024-01-15T10:30:00"
created_at | TIMESTAMP | Record creation timestamp | "2024-01-15T10:35:00"
updated_at | TIMESTAMP | Record update timestamp | "2024-01-15T10:35:00"
```

#### Tracked Nationalities (23 countries)
- **Eastern Europe**: Ukraine, Polen, Rumænien, Hviderusland, Albanien, Nordmakedonien
- **South Asia**: Indien, Bangladesh, Pakistan
- **Southeast Asia**: Vietnam, Filippinerne, Thailand
- **Africa**: Uganda, Tanzania, Sydafrika, Etiopien, Kenya, Ghana, Marokko
- **Other**: Brasilien, Kina, Kirgizstan, Usbekistan, Tyrkiet

#### Summary Statistics Schema
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
processing_timestamp | VARCHAR | Processing run timestamp | "20250115_103000"
total_records | BIGINT | Total permit records | 1250
unique_companies | BIGINT | Number of companies | 85
unique_nationalities | BIGINT | Number of nationalities | 18
min_year | INTEGER | Earliest year in data | 2019
max_year | INTEGER | Latest year in data | 2023
total_permits | BIGINT | Sum of all permits | 3420
avg_permits_per_record | DOUBLE | Average permits per record | 2.74
```

### Storage Locations
- **Gold Output**: `gs://landbrugsdata-raw-data/gold/work_permits/{timestamp}/`
- **Main Dataset**: `work_permits.parquet`
- **Summary Statistics**: `summary.parquet`
- **Source Silver**: `gs://landbrugsdata-raw-data/silver/drive_data/{timestamp}/work_permits_*.parquet`

### Processing Infrastructure
- **Platform**: Manual execution via GitHub Actions
- **Resources**: 4GB RAM, 2 threads for data validation
- **Dependencies**: Work permits silver data from Drive Data Pipeline
- **Performance**: ~10 minutes for complete validation and formatting
- **Memory Configuration**: Optimized for DuckDB-based data processing

### Data Validation Rules
- **Company ID**: Must be non-null, non-empty string (CVR format expected)
- **Year**: Must be between 2019-2025 (configurable range)
- **Nationality**: Must be non-null, non-empty string
- **Permit Count**: Must be positive integer ≤ 1000 (configurable limit)
- **Source Tracking**: Maintains PDF source file and extraction metadata

### Drive Pipeline Integration
```python
# Example pattern matching for silver data
work_permits_pattern = f"gs://{bucket}/silver/drive_data/**/work_permits_*.parquet"

# Data loading with validation
CREATE TABLE work_permits AS
SELECT company_id, year, nationality, first_permits_count, ...
FROM read_parquet('{pattern}')
WHERE year BETWEEN {start_year} AND {end_year}
  AND first_permits_count > 0
  AND first_permits_count <= {max_permits}
```

### Quality Assurance Features
- **Comprehensive Validation**: Checks for missing, invalid, and out-of-range values
- **Statistical Limits**: Reasonable bounds on permit counts and years
- **Coverage Reporting**: Detailed statistics on data completeness and quality
- **Error Recovery**: Graceful handling of missing or corrupted silver data

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Agricultural Labor Data Team
- **Response Time**: 2-3 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Work Permits" label
- **PDF Processing Problems**: Contact Drive Data Pipeline team for extraction issues
- **Access Problems**: Submit access requests via appropriate channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when source documents or processing requirements change
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural labor data accessible and trustworthy.*

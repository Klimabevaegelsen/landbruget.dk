# Arbejdstilsynet Inspections (Work Environment Authority) Workflow

> **Monthly Foundation Processing**: Danish workplace safety inspection data with hybrid pipeline architecture

---

## What This Workflow Does

The Arbejdstilsynet Inspections workflow collects and processes workplace safety inspection data from the Danish Work Environment Authority (Arbejdstilsynet). This workflow uses a hybrid architecture combining a standalone pipeline for data collection and transformation with the unified pipeline's gold layer for business analytics. It provides comprehensive workplace safety compliance data for Danish companies across all industries.

### Why This Data Matters
- **Workplace Safety Monitoring**: Critical data for tracking workplace safety compliance across Danish industries
- **Risk Assessment**: Identification of high-risk companies and industries based on inspection patterns
- **Compliance Analysis**: Analysis of workplace safety violations and enforcement actions
- **Industry Benchmarking**: Comparison of safety performance across different industries and company sizes
- **Policy Effectiveness**: Assessment of workplace safety policy implementation and effectiveness
- **Agricultural Safety**: Specific focus on agricultural workplace safety for industry analysis

### Key Statistics
- **Data Coverage**: Comprehensive Danish workplace inspection data with P-number to CVR mapping
- **Processing Scale**: Historical inspection data with 6-month rolling window processing
- **Industry Coverage**: All Danish industries with focus on high-risk sectors including agriculture
- **Integration Architecture**: Hybrid pipeline with standalone bronze/silver and unified gold processing
- **Update Frequency**: Monthly processing ensuring current workplace safety compliance data

---

## Data Sources and Collection

### Official Sources
This workflow processes data from the Danish Work Environment Authority:

| Data Source | Purpose | Coverage | Data Format |
|-------------|---------|----------|-------------|
| **Arbejdstilsynet CSV Export** | Workplace inspection records | National Denmark | CSV via URL |

### Hybrid Architecture Overview

#### Standalone Pipeline (Bronze & Silver)
- **Location**: `backend/pipelines/arbejdstilsynet_inspections/`
- **Purpose**: Data collection and transformation from Arbejdstilsynet CSV source
- **Technology**: Docker containerized with browser automation capabilities
- **Processing**: Independent execution with GCS export for unified pipeline consumption

#### Unified Pipeline (Gold)
- **Location**: `backend/pipelines/unified_pipeline/src/unified_pipeline/gold/arbejdstilsynet_inspections.py`
- **Purpose**: Business analytics and advanced data processing
- **Integration**: Consumes silver data from standalone pipeline
- **Processing**: Monthly foundation processing as part of unified pipeline schedule

### Data Collection Process

#### Standalone Pipeline Processing
- **Data Source**: CSV file export from Arbejdstilsynet via configured URL
- **Authentication**: Environment variable-based URL configuration
- **Date Filtering**: Configurable date range processing (default: 6 months rolling window)
- **Browser Automation**: Docker-based browser automation for data access
- **Export**: Automatic GCS export to `gs://bucket/silver/arbejdstilsynet_inspections/`

#### CVR Enrichment Integration
- **P-number Mapping**: Conversion of Arbejdstilsynet P-numbers to CVR business registration numbers
- **CVR API Integration**: Automated CVR lookup using unified pipeline CVR API client
- **Data Enrichment**: Company information enhancement for business analytics
- **Success Rate Tracking**: Monitoring of CVR mapping success rates for data quality

### Data Privacy and Compliance
- **Workplace Safety Data**: Official inspection data for legitimate safety analysis and compliance monitoring
- **PII Protection**: Automated anonymization of potential personally identifiable information
- **Business Data**: Company identification through CVR numbers for business analysis
- **Regulatory Compliance**: Workplace safety data for regulatory compliance and policy analysis

---

## Data Processing Steps

### 🥉 Bronze Layer: Raw Data Collection (Standalone Pipeline)
**What happens**: We fetch raw workplace inspection data from Arbejdstilsynet CSV source
**Why**: Official workplace safety data requires careful extraction to preserve inspection records and compliance information

**Specific processing**:
- **CSV Data Extraction**: Direct download of workplace inspection data from configured Arbejdstilsynet URL
- **Docker Containerization**: Isolated processing environment with browser automation capabilities
- **Raw Data Preservation**: Exact replica of source data maintained for audit and compliance purposes
- **Metadata Generation**: Comprehensive metadata tracking including fetch timestamps and source URLs
- **Date Range Processing**: Configurable date filtering for targeted data collection

**Quality controls**:
- **Source URL Validation**: Verification of data source accessibility and format consistency
- **Data Completeness**: Assessment of record counts and data structure validation
- **Timestamp Tracking**: Precise tracking of data collection timing for audit purposes
- **Error Recovery**: Robust error handling for network issues and source format changes

**Output**: Raw CSV workplace inspection data with comprehensive metadata

### 🥈 Silver Layer: Advanced Data Transformation (Standalone Pipeline)
**What happens**: We transform raw inspection data into standardized, analysis-ready format with CVR enrichment
**Why**: Raw inspection data requires cleaning, normalization, and business context for analytical use

**Specific transformations**:

#### Comprehensive Data Cleaning
- **Column Standardization**: Consistent naming conventions following unified pipeline standards
- **Deduplication**: Removal of duplicate inspection records based on key identifiers
- **Data Type Conversion**: Proper typing for dates, numeric values, and categorical data
- **Null Value Handling**: Conversion of empty strings to proper null values
- **Danish Character Normalization**: Proper handling of Danish characters (æ, ø, å) for text processing

#### Advanced CVR Integration
- **P-number to CVR Mapping**: Automated conversion using CVR API integration
- **Company Information Enhancement**: Addition of company details through CVR lookup
- **Mapping Success Tracking**: Detailed monitoring of CVR conversion success rates
- **Business Context Addition**: Company size, industry classification, and registration details

#### Privacy and Compliance Processing
- **PII Detection and Anonymization**: Automated detection and protection of personal information
- **Data Quality Scoring**: Assessment of record completeness and reliability
- **Compliance Validation**: Verification of inspection data integrity and consistency
- **Industry Classification**: Standardized industry coding for analytical purposes

#### Efficient Data Storage
- **Parquet Conversion**: Conversion to Parquet format for efficient querying and analysis
- **GCS Export**: Automatic export to Google Cloud Storage for unified pipeline consumption
- **Metadata Preservation**: Retention of processing metadata and data lineage information

**Quality checks**:
- **Data Integrity Validation**: Comprehensive validation of inspection record consistency
- **CVR Mapping Validation**: Verification of P-number to CVR conversion accuracy
- **Date Range Validation**: Confirmation of proper date filtering and temporal consistency
- **Industry Classification Validation**: Verification of industry code assignments and consistency

**Output**: Clean, standardized workplace inspection data with CVR enrichment in Parquet format

### 🥇 Gold Layer: Business Analytics and Advanced Processing (Unified Pipeline)
**What happens**: We transform silver data into business-ready analytics with advanced derived metrics
**Why**: Business users need sophisticated analytics, risk assessment, and compliance monitoring capabilities

**Specific transformations**:

#### Advanced Business Analytics
- **Risk Scoring**: Comprehensive risk assessment based on inspection history and violation severity
- **Company Profiling**: Multi-dimensional company analysis including compliance rates and inspection frequency
- **Industry Benchmarking**: Statistical analysis of safety performance across industries
- **Temporal Analysis**: Time-series analysis of inspection patterns and compliance trends

#### Sophisticated Data Enhancement
- **Danish Language Restoration**: Proper restoration of Danish characters and formatting for business presentation
- **Decision Type Standardization**: Standardized classification of inspection outcomes (Strakspåbud, Påbud, Påtale)
- **Severity Scoring**: Quantitative severity assessment for risk analysis and prioritization
- **Geographic Enhancement**: Postal code extraction and city standardization for regional analysis

#### Advanced Derived Metrics
- **Company Inspection Frequency**: Historical analysis of inspection patterns per company
- **Industry Risk Assessment**: Statistical analysis of industry-wide safety performance
- **Compliance Rate Calculation**: Quantitative assessment of company compliance history
- **Repeat Offender Identification**: Algorithmic identification of companies with multiple severe violations
- **Quality Scoring**: Multi-factor data quality assessment for analytical confidence

#### Business Intelligence Features
- **Executive Dashboards**: Summary statistics and key performance indicators
- **Risk Alerts**: Identification of high-risk companies and emerging safety trends
- **Compliance Reporting**: Standardized reporting for regulatory and business purposes
- **Trend Analysis**: Long-term trend identification and forecasting capabilities

**Quality checks**:
- **Business Rule Validation**: Comprehensive validation of business logic and analytical rules
- **Data Quality Assessment**: Multi-dimensional quality scoring with confidence intervals
- **Statistical Validation**: Verification of derived metrics and analytical calculations
- **Temporal Consistency**: Validation of time-series data and trend calculations

**Output**: Business-ready workplace safety analytics with advanced risk assessment and compliance monitoring

---

## Workflow Schedule and Execution

### Hybrid Architecture Execution
- **Standalone Pipeline**: Independent execution with configurable scheduling
- **Unified Pipeline Gold**: Monthly foundation processing (priority 7, estimated 45 minutes)
- **Data Flow**: Standalone pipeline → GCS silver data → Unified pipeline gold processing
- **Integration**: Seamless data handoff via GCS storage with metadata preservation

### Monthly Foundation Processing (Unified Pipeline)
- **Schedule**: 1st of every month at 1 AM UTC (foundation batch, priority 7)
- **Execution Type**: Automated monthly processing as foundation data source
- **Processing Duration**: ~45 minutes for complete gold layer processing
- **Dependencies**: Requires silver data from standalone pipeline
- **Downstream Impact**: Provides workplace safety context for other analyses

### Standalone Pipeline Execution
- **Execution Environment**: Docker containerized with browser automation
- **Processing Window**: Configurable date range (default: 6-month rolling window)
- **Resource Requirements**: Browser automation capabilities with virtual display
- **Output**: Silver data exported to GCS for unified pipeline consumption

### Advanced Features
- **Hybrid Architecture**: Optimal separation of concerns between data collection and analytics
- **CVR Integration**: Automated business registration number mapping for company analysis
- **Docker Containerization**: Isolated execution environment with browser automation
- **GCS Integration**: Seamless data handoff between pipeline components
- **Configurable Processing**: Flexible date range and parameter configuration

### Resource Management
- **Standalone Pipeline**: Docker container with browser automation and virtual display
- **Unified Pipeline**: Standard unified pipeline resource allocation
- **Memory Optimization**: Efficient processing of large inspection datasets
- **Storage Optimization**: Parquet format for efficient querying and analysis

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Good | Comprehensive Danish workplace inspection data with some CVR mapping limitations |
| **Accuracy** | Excellent | Danish Work Environment Authority official data with rigorous validation |
| **Timeliness** | Good | Monthly processing with configurable date windows for current data |
| **Business Context** | Excellent | Enhanced with CVR mapping and advanced business analytics |

### Known Issues and Limitations

#### Data Source and Collection Constraints
- **CSV Source Dependency**: Processing success depends on Arbejdstilsynet CSV export availability and format consistency
- **URL Configuration**: Requires manual URL configuration for data source access
- **Browser Automation**: Complex Docker setup required for automated data collection
- **Date Range Limitations**: Processing limited to configurable date windows (default 6 months)

#### CVR Mapping and Business Context Limitations
- **P-number Coverage**: CVR mapping success depends on P-number to CVR conversion rates
- **Historical Data**: Some historical records may lack current CVR mapping information
- **Company Changes**: Business registration changes may affect historical data consistency
- **Industry Classification**: Industry codes may change over time affecting trend analysis

#### Processing and Technical Limitations
- **Hybrid Architecture Complexity**: Coordination required between standalone and unified pipeline components
- **Processing Dependencies**: Unified pipeline gold layer depends on successful standalone pipeline execution
- **Resource Requirements**: Browser automation requires specialized Docker configuration
- **Data Format Dependencies**: Processing assumes consistent CSV format from Arbejdstilsynet

### Recommended Uses
✅ **This data is excellent for**:
- Workplace safety compliance analysis and monitoring across Danish industries
- Risk assessment and identification of high-risk companies and industries
- Industry benchmarking and safety performance comparison analysis
- Regulatory compliance reporting and policy effectiveness assessment
- Agricultural workplace safety analysis as part of comprehensive industry studies

⚠️ **Use with caution for**:
- Real-time safety monitoring - Monthly processing with potential data lag
- Individual inspection details - Aggregated data focused on compliance patterns
- Historical trend analysis - CVR mapping limitations may affect historical consistency

❌ **Not recommended for**:
- Daily operational safety management - Monthly updates, not real-time inspection data
- Individual worker safety analysis - Company-level data, not individual worker information
- International safety comparisons without context - Danish-specific workplace safety regulations

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Which Danish companies have the most workplace safety violations?** - Company-level violation analysis with severity scoring
2. **What industries have the highest workplace safety risks?** - Industry-wide risk assessment and benchmarking
3. **How effective are Danish workplace safety enforcement actions?** - Compliance rate analysis and enforcement effectiveness
4. **Which agricultural companies have workplace safety compliance issues?** - Agricultural industry-specific safety analysis

### Example Analyses
#### Industry Risk Assessment and Benchmarking
**Question**: Which industries have the highest workplace safety risks and how do they compare?
**Data Used**: Arbejdstilsynet gold data with industry classification and severity scoring
**Method**: Statistical analysis of violation rates, severity scores, and compliance patterns by industry
**Output**: Industry risk rankings with benchmarking analysis and trend identification
**Limitations**: Industry classifications may change over time, CVR mapping affects historical consistency

#### Company Compliance Monitoring and Risk Scoring
**Question**: Which companies are repeat offenders and pose the highest workplace safety risks?
**Data Used**: Company-level inspection history with severity scoring and compliance rate calculation
**Method**: Multi-factor risk assessment combining violation frequency, severity, and compliance trends
**Output**: Company risk profiles with compliance scores and repeat offender identification
**Limitations**: Analysis limited to companies with CVR mapping, recent inspection data may be incomplete

#### Agricultural Workplace Safety Analysis
**Question**: How does workplace safety compliance in agriculture compare to other industries?
**Data Used**: Agricultural company inspection data with industry benchmarking
**Method**: Comparative analysis of agricultural vs. non-agricultural workplace safety performance
**Output**: Agricultural safety performance analysis with industry-specific recommendations
**Limitations**: Agricultural classification depends on accurate industry coding and CVR mapping

### Data Access
- **Research Access**: Complete workplace safety datasets for academic and scientific research
- **Policy Access**: Workplace safety data for policy development and regulatory analysis
- **Industry Access**: Safety compliance data for industry analysis and benchmarking
- **Regulatory Access**: Official inspection data for compliance monitoring and enforcement

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Workplace Inspection Data (Gold Layer)

**Business-Ready Inspection Records**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
company_id | VARCHAR | Internal company identifier | "P12345678"
cvr_number | INTEGER | CVR business registration number | 12345678
company_name_clean | VARCHAR | Standardized company name | "Dansk Landbrug A/S"
company_address | VARCHAR | Company address | "Hovedgade 123, 1000 København"
postal_code | VARCHAR | Extracted postal code | "1000"
city | VARCHAR | Standardized city name | "København"
industry_clean | VARCHAR | Standardized industry name | "Avl Af Malkekvæg"
industry_formatted | VARCHAR | Danish formatted industry | "Avl af malkekvæg"
date | DATE | Inspection date | "2024-06-15"
year | INTEGER | Inspection year | 2024
month | INTEGER | Inspection month | 6
year_month | VARCHAR | Year-month period | "2024-06"
decision | VARCHAR | Original decision type | "paabud"
decision_type | VARCHAR | Standardized decision type | "Påbud"
severity_score | INTEGER | Severity score (1-3) | 2
work_env_issue | VARCHAR | Work environment issue | "ergonomi"
work_env_issue_formatted | VARCHAR | Formatted work environment issue | "Ergonomi"
complied | INTEGER | Compliance flag (0/1) | 0
case_count | INTEGER | Number of cases in inspection | 3
company_inspection_count | INTEGER | Total inspections for company | 5
industry_avg_severity | DOUBLE | Industry average severity | 1.85
company_compliance_rate | DOUBLE | Company compliance rate (0-1) | 0.60
days_since_last_inspection | INTEGER | Days since previous inspection | 365
is_repeat_offender | BOOLEAN | Repeat offender flag | true
has_quality_flag | BOOLEAN | Data quality flag | false
data_quality_score | DOUBLE | Data quality score (0-1) | 0.95
```

### Storage Locations
#### Standalone Pipeline
- **Bronze**: `backend/pipelines/arbejdstilsynet_inspections/data/bronze/{timestamp}/data.csv`
- **Silver**: `backend/pipelines/arbejdstilsynet_inspections/data/silver/{timestamp}/processed_data.parquet`
- **GCS Export**: `gs://landbruget-data/silver/arbejdstilsynet_inspections/{timestamp}/workplace_inspections.parquet`

#### Unified Pipeline
- **Gold**: `gs://landbruget-data/gold/arbejdstilsynet_inspections/{timestamp}/workplace_inspections_gold.parquet`

### Processing Infrastructure
#### Standalone Pipeline
- **Platform**: Docker containerized with browser automation
- **Resources**: Virtual display (xvfb), Chrome browser, Python environment
- **Dependencies**: Arbejdstilsynet CSV source access via configured URL
- **Performance**: Configurable date range processing (default 6 months)

#### Unified Pipeline Gold Layer
- **Platform**: Automated monthly execution as foundation data source
- **Resources**: Standard unified pipeline resource allocation
- **Dependencies**: Silver data from standalone pipeline via GCS
- **Performance**: ~45 minutes for complete gold processing (45 minutes estimated)

### Hybrid Architecture Configuration
```bash
# Standalone Pipeline Environment Variables
SOURCE_CSV_URL=https://example.com/arbejdstilsynet-data.csv
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
GCS_BUCKET=landbruget-data

# Processing Parameters
--start-date 2024-01-01
--end-date 2024-06-30
--stage all
--gcs-bucket landbruget-data
```

### CVR Integration Features
- **P-number Mapping**: Automated conversion using unified pipeline CVR API client
- **Success Rate Monitoring**: Detailed tracking of CVR mapping success rates
- **Company Enhancement**: Addition of company information through CVR lookup
- **Business Context**: Industry classification and company size information

### Data Quality Features
- **PII Protection**: Automated detection and anonymization of personal information
- **Data Quality Scoring**: Multi-factor assessment of record completeness and reliability
- **Danish Language Support**: Proper handling and restoration of Danish characters
- **Business Rule Validation**: Comprehensive validation of business logic and analytical rules

### Docker Configuration
```dockerfile
# Browser automation support
RUN apt-get update && apt-get install -y xvfb
ENV DISPLAY=:99

# Chrome browser configuration
--no-sandbox
--disable-dev-shm-usage
--disable-gpu
--remote-debugging-port=9222
```

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Workplace Safety Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Arbejdstilsynet" label
- **Standalone Pipeline Issues**: Contact system administrators for Docker and browser automation issues
- **CVR Mapping Issues**: Contact CVR integration team for business registration mapping problems

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when Arbejdstilsynet data format changes or CVR integration updates occur
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make workplace safety data accessible and trustworthy.*

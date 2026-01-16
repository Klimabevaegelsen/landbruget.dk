# DST (Danish Statistics) Workflow

> **Monthly Foundation Processing**: Official Danish agricultural statistics from Danmarks Statistik

---

## What This Workflow Does

The DST (Danish Statistics) workflow collects and processes official Danish agricultural statistics from Danmarks Statistik API. This workflow fetches comprehensive agricultural production data across four key statistical tables, providing authoritative agricultural production statistics that serve as foundation data for field-level production estimates and agricultural analysis.

### Why This Data Matters
- **Production Estimates**: Official yield and production statistics essential for field-level agricultural modeling
- **Policy Development**: Authoritative agricultural statistics for policy decisions and economic analysis
- **Market Analysis**: Production data for agricultural market analysis and forecasting
- **Research Foundation**: Official statistics for academic and scientific agricultural research
- **EU Reporting**: Danish agricultural statistics for European Union reporting requirements

### Key Statistics
- **Data Coverage**: National Danish agricultural statistics with regional breakdowns
- **Statistical Tables**: 4 core agricultural tables (harvest, horticulture, seeds, straw)
- **Temporal Scope**: Historical and current agricultural production data
- **Processing**: Complete Bronze and Silver layer processing with JSONSTAT format handling
- **Integration**: Foundation data for field production estimates and agricultural analysis

---

## Data Sources and Collection

### Official Sources
This workflow collects data from Danmarks Statistik (Danish Statistics):

| Table ID | Purpose | Coverage | Data Type |
|----------|---------|----------|-----------|
| **HST77** | Harvest Results | Grain crops (wheat, barley, rye, oats) | Production yields |
| **GARTN1** | Horticulture | Horticultural crops and vegetables | Production volumes |
| **FRO** | Seeds | Seed production and distribution | Seed statistics |
| **HALM1** | Straw | Straw production and utilization | Straw usage |

### Data Collection Process

#### Danmarks Statistik API Integration
- **API Endpoint**: `https://api.statbank.dk/v1` (official DST API)
- **Data Format**: JSONSTAT format with multidimensional statistical data
- **Authentication**: Public API with rate limiting and retry logic
- **Request Method**: POST requests with JSON payloads for data retrieval
- **Language**: Danish language responses with Danish statistical categories

#### Statistical Data Structure
- **JSONSTAT Format**: Structured statistical data with dimensions and values
- **Multidimensional Data**: Area codes, crop codes, time periods, and measure types
- **Comprehensive Coverage**: National and regional agricultural statistics
- **Historical Data**: Multi-year time series for trend analysis
- **Metadata Integration**: Table information and statistical metadata

#### Advanced API Processing
- **Wildcard Queries**: Uses "*" wildcards to fetch all available data dimensions
- **Retry Logic**: Exponential backoff retry strategy for robust API communication
- **Error Handling**: Comprehensive error handling for API timeouts and failures
- **Batch Processing**: Sequential processing of multiple statistical tables

### Data Privacy and Compliance
- **Public Statistics**: Official government statistics, no privacy concerns
- **Open Data**: Publicly available Danish agricultural statistics
- **API Compliance**: Follows Danmarks Statistik API usage guidelines
- **Data Licensing**: Complies with Danish government open data policies

---

## Data Processing Steps

### 🥉 Bronze Layer: Official Statistical Data Collection
**What happens**: We fetch raw agricultural statistics from Danmarks Statistik API in JSONSTAT format
**Why**: Official statistics require careful API handling and comprehensive data collection to ensure complete statistical coverage

**Specific processing**:
- **API Communication**: POST requests to DST API with JSON payloads
- **Table Processing**: Sequential fetching of HST77, GARTN1, FRO, and HALM1 tables
- **Metadata Collection**: Fetches both data and table information for each statistical table
- **Retry Strategy**: Exponential backoff retry logic for robust API communication
- **Data Persistence**: Raw JSONSTAT responses stored with processing metadata

**Quality controls**:
- **API Response Validation**: Verify JSONSTAT structure and data presence
- **Table Completeness**: Ensure all configured tables are successfully fetched
- **Metadata Integration**: Store table information and processing metadata
- **Error Recovery**: Graceful handling of API failures and partial data

**Output**: Raw JSONSTAT statistical data for all four agricultural tables

### 🥈 Silver Layer: Advanced Statistical Data Transformation
**What happens**: We transform raw JSONSTAT data into structured, analysis-ready datasets using Ibis and DuckDB
**Why**: JSONSTAT format requires specialized parsing and dimensional analysis to extract meaningful agricultural statistics

**Specific transformations**:

#### JSONSTAT Processing
- **Dimension Extraction**: Parse multidimensional statistical structure (areas, crops, time, measures)
- **Value Mapping**: Convert statistical indices to meaningful labels and codes
- **Data Flattening**: Transform multidimensional data into flat, queryable records
- **Type Conversion**: Proper data type handling for statistical values and categories

#### Table-Specific Processing
- **HST77 (Harvest Results)**: Area codes, crop codes, measure types, harvest values
- **GARTN1 (Horticulture)**: Regional data, horticultural crops, production measures
- **FRO (Seeds)**: Crop codes, seed production measures, temporal data
- **HALM1 (Straw)**: Area codes, crop codes, usage types, straw production values

#### Advanced Data Engineering
- **Ibis Integration**: Uses Ibis framework for performant, database-agnostic transformations
- **DuckDB Processing**: Leverages DuckDB for efficient statistical data processing
- **Schema Standardization**: Consistent column naming and data types across all tables
- **Null Handling**: Proper handling of missing statistical values

**Quality checks**:
- **Record Validation**: Ensure successful transformation of all statistical records
- **Dimension Integrity**: Verify proper mapping of statistical dimensions to columns
- **Value Ranges**: Validate statistical values are within reasonable ranges
- **Completeness Assessment**: Monitor record counts through transformation pipeline

**Output**: Structured agricultural statistics ready for production estimation and analysis

### 🥇 Gold Layer: Integration with Field Production Estimates
**What happens**: DST statistics are integrated with field-level data for production estimates
**Why**: Official statistics provide authoritative yield data for field-level agricultural modeling

**Integration applications**:
- **Field Production Estimates**: DST yields applied to field-level crop data
- **Crop Mapping**: Comprehensive mapping between field crops and DST statistical categories
- **Production Modeling**: Statistical foundation for agricultural production estimates
- **Yield Validation**: Official statistics for validating field-level production calculations

**Output**: Foundation statistical data integrated into comprehensive agricultural analysis

---

## Workflow Schedule and Execution

### Monthly Foundation Processing
- **Schedule**: 1st of every month at 1 AM UTC (foundation batch)
- **Execution Type**: Automated monthly execution as foundation workflow
- **Processing Duration**: ~60 minutes for complete statistical data collection
- **Dependencies**: None (independent foundation data source)
- **Downstream Impact**: Enables field production estimates and agricultural analysis

### Processing Performance
- **Data Volume**: Thousands of statistical records across four tables
- **Memory Usage**: Moderate (~4-8GB) for JSONSTAT processing and transformation
- **API Calls**: Sequential API requests with retry logic and rate limiting
- **Storage**: ~50-200MB for processed statistical datasets
- **Network**: Sustained API communication with Danmarks Statistik services

### Advanced Features
- **JSONSTAT Expertise**: Specialized handling of multidimensional statistical data format
- **Ibis Integration**: Modern data processing framework for performant transformations
- **Comprehensive Mapping**: Detailed crop mapping for field-level integration
- **Statistical Validation**: Quality checks specific to agricultural statistics

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Complete official Danish agricultural statistics from authoritative source |
| **Accuracy** | Excellent | Danmarks Statistik official data with rigorous statistical methodology |
| **Timeliness** | Good | Monthly updates as new statistical data becomes available |
| **Consistency** | Excellent | Standardized statistical methodology and processing |

### Known Issues and Limitations

#### API and Data Constraints
- **API Rate Limits**: Sequential processing required to respect Danmarks Statistik API limits
- **JSONSTAT Complexity**: Multidimensional format requires specialized parsing logic
- **Network Dependency**: Processing success depends on stable API connectivity
- **Update Frequency**: Limited by Danmarks Statistik publication schedule

#### Statistical Coverage Limitations
- **Table Scope**: Limited to 4 core agricultural tables (HST77, GARTN1, FRO, HALM1)
- **Regional Granularity**: Statistical areas may not align perfectly with field-level data
- **Crop Categories**: DST categories may not match all field-level crop classifications
- **Temporal Alignment**: Statistical years may not align with agricultural seasons

#### Processing Considerations
- **JSONSTAT Parsing**: Complex multidimensional data requires careful dimension handling
- **Memory Requirements**: Large statistical datasets require adequate processing resources
- **Integration Complexity**: Mapping between statistical categories and field data requires maintenance
- **Error Propagation**: Statistical errors can impact downstream production estimates

### Recommended Uses
✅ **This data is excellent for**:
- Field-level agricultural production estimates using official yield statistics
- Agricultural policy analysis and economic modeling with authoritative data
- Market analysis and forecasting based on official production statistics
- Academic research requiring official Danish agricultural statistics
- EU reporting and international agricultural comparisons

⚠️ **Use with caution for**:
- Real-time agricultural monitoring - Monthly updates with publication delays
- Field-level precision - Statistical areas may not match individual field boundaries
- Non-Danish applications - Data specific to Danish agricultural classification systems

❌ **Not recommended for**:
- Individual farm analysis - Aggregated statistical data, not farm-specific
- Daily operational decisions - Monthly statistical updates, not operational data
- International comparisons without context - Danish-specific statistical methodology

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **What are the official Danish agricultural production yields by crop type?** - HST77 harvest statistics with regional and temporal breakdowns
2. **How do horticultural production patterns vary across Danish regions?** - GARTN1 data analysis showing regional horticultural trends
3. **What are the seed production and distribution patterns in Denmark?** - FRO statistics showing seed industry data
4. **How is agricultural straw utilized across different applications?** - HALM1 analysis of straw production and usage patterns

### Example Analyses
#### Field Production Estimation
**Question**: What yields should be applied to field-level crop data for production estimates?
**Data Used**: HST77 harvest statistics matched to field crop classifications
**Method**: Crop mapping between field data and DST statistical categories with yield application
**Output**: Field-level production estimates based on official statistical yields
**Limitations**: Statistical areas may not perfectly match field-level conditions; requires crop mapping maintenance

#### Agricultural Market Analysis
**Question**: How have Danish agricultural production patterns changed over time?
**Data Used**: Multi-year DST statistics across all four tables (HST77, GARTN1, FRO, HALM1)
**Method**: Time series analysis of production trends by crop type and region
**Output**: Market trend analysis showing production evolution and regional patterns
**Limitations**: Statistical methodology changes may affect long-term comparisons

#### Policy Impact Assessment
**Question**: How do agricultural policies affect different crop production sectors?
**Data Used**: DST statistics correlated with policy implementation timelines
**Method**: Before/after analysis of production statistics relative to policy changes
**Output**: Policy impact assessment showing statistical evidence of agricultural changes
**Limitations**: Correlation analysis; external factors may influence production beyond policy

### Data Access
- **Research Access**: Full statistical datasets for academic and scientific research
- **Policy Access**: Official statistics for agricultural policy development and analysis
- **Industry Access**: Production statistics for agricultural market analysis
- **Public Access**: Aggregate statistics and trend summaries

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### DST Statistical Tables (Silver Layer)

**HST77 (Harvest Results)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
table_id | VARCHAR | Statistical table identifier | "HST77"
area_code | VARCHAR | Statistical area code | "000"
area_name | VARCHAR | Area name in Danish | "Hele landet"
crop_code | VARCHAR | Crop classification code | "010"
crop_name | VARCHAR | Crop name in Danish | "Vinterhvede"
measure_code | VARCHAR | Measure type code | "HOEST"
measure_name | VARCHAR | Measure name in Danish | "Høstudbytte"
time_period | VARCHAR | Time period code | "2023"
time_label | VARCHAR | Time period label | "2023"
harvest_value | DOUBLE | Statistical value | 7.8
processing_time | VARCHAR | Processing timestamp | "2025-01-15T10:30:00"
```

**GARTN1 (Horticulture)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
table_id | VARCHAR | Statistical table identifier | "GARTN1"
area_code | VARCHAR | Statistical area code | "101"
area_name | VARCHAR | Area name in Danish | "København og Frederiksberg"
measure_code | VARCHAR | Measure type code | "AREAL"
measure_name | VARCHAR | Measure name in Danish | "Areal"
crop_code | VARCHAR | Horticultural crop code | "1010"
crop_name | VARCHAR | Crop name in Danish | "Tomater"
time_period | VARCHAR | Time period code | "2023"
time_label | VARCHAR | Time period label | "2023"
horticulture_value | DOUBLE | Statistical value | 125.5
processing_time | VARCHAR | Processing timestamp | "2025-01-15T10:30:00"
```

**FRO (Seeds)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
table_id | VARCHAR | Statistical table identifier | "FRO"
crop_code | VARCHAR | Seed crop code | "010"
crop_name | VARCHAR | Crop name in Danish | "Vinterhvede"
measure_code | VARCHAR | Measure type code | "PROD"
measure_name | VARCHAR | Measure name in Danish | "Produktion"
time_period | VARCHAR | Time period code | "2023"
time_label | VARCHAR | Time period label | "2023"
seed_value | DOUBLE | Statistical value | 1250.0
processing_time | VARCHAR | Processing timestamp | "2025-01-15T10:30:00"
```

**HALM1 (Straw)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
table_id | VARCHAR | Statistical table identifier | "HALM1"
area_code | VARCHAR | Statistical area code | "000"
area_name | VARCHAR | Area name in Danish | "Hele landet"
crop_code | VARCHAR | Crop code | "010"
crop_name | VARCHAR | Crop name in Danish | "Vinterhvede"
usage_code | VARCHAR | Straw usage code | "ENERGY"
usage_name | VARCHAR | Usage name in Danish | "Energiformål"
unit_code | VARCHAR | Unit code | "TONS"
unit_name | VARCHAR | Unit name in Danish | "Tons"
time_period | VARCHAR | Time period code | "2023"
time_label | VARCHAR | Time period label | "2023"
straw_value | DOUBLE | Statistical value | 850000.0
processing_time | VARCHAR | Processing timestamp | "2025-01-15T10:30:00"
```

### Storage Locations
- **Bronze**: `gs://landbrugsdata-raw-data/bronze/dst/{timestamp}/{table_id}_data.json`
- **Bronze Metadata**: `gs://landbrugsdata-raw-data/bronze/dst/{timestamp}/{table_id}_metadata.json`
- **Bronze Table Info**: `gs://landbrugsdata-raw-data/bronze/dst/{timestamp}/{table_id}_tableinfo.json`
- **Silver**: `gs://landbrugsdata-raw-data/silver/{table_id}_processed/{timestamp}/data.parquet`

### Processing Infrastructure
- **Platform**: Automated monthly execution via GitHub Actions (foundation batch)
- **Resources**: 8GB RAM, optimized for JSONSTAT processing
- **Dependencies**: Danmarks Statistik API access
- **Performance**: ~60 minutes for complete statistical data collection
- **Framework Integration**: Ibis + DuckDB for performant statistical transformations

### API Integration Details
- **Base URL**: `https://api.statbank.dk/v1`
- **Endpoints**: `/tableinfo` (metadata), `/data` (statistical data)
- **Request Method**: POST with JSON payloads
- **Data Format**: JSONSTAT multidimensional statistical format
- **Language**: Danish (`da`) for statistical category names
- **Rate Limiting**: Sequential processing with exponential backoff retry

### JSONSTAT Processing Features
- **Multidimensional Data**: Handles complex statistical dimensions (area, crop, time, measure)
- **Index Mapping**: Converts statistical indices to meaningful labels and codes
- **Dimension Extraction**: Parses nested dimension structures from JSONSTAT format
- **Value Processing**: Handles statistical values with proper null handling

### Crop Mapping System
- **HST77 Crops**: Grains (wheat, barley, rye, oats) with regional yield data
- **GARTN1 Crops**: Horticultural crops and vegetables with production volumes
- **FRO Crops**: Seed production statistics across major crop types
- **HALM1 Usage**: Straw utilization patterns (energy, feed, bedding)
- **Field Integration**: Comprehensive mapping to field-level crop classifications

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Agricultural Statistics Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "DST" label
- **API Problems**: Contact system administrators for Danmarks Statistik API issues
- **Statistical Questions**: Direct statistical methodology questions to DST support

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when DST API changes or statistical methodology evolves
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural statistical data accessible and trustworthy.*

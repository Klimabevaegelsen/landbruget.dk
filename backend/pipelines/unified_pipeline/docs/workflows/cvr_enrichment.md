# CVR Enrichment Workflow

> **Weekly Data Processing**: Company information enrichment and geocoding for Danish agricultural businesses

---

## What This Workflow Does

The CVR Enrichment workflow processes and enriches company data from the Danish Business Authority's Central Business Register (CVR). This system ensures we have up-to-date, accurate information about agricultural companies, their addresses, and business relationships.

### Why This Data Matters
- **Business Intelligence**: Tracks ownership and structure of agricultural enterprises
- **Regulatory Compliance**: Links companies to their regulatory obligations and permits
- **Economic Analysis**: Enables analysis of agricultural business trends and consolidation
- **Data Quality**: Provides authoritative company information for linking with other datasets

### Key Statistics
- **Data Volume**: ~500,000+ Danish companies processed
- **Coverage**: All registered Danish businesses with agricultural activities
- **Update Frequency**: Weekly (every Monday at 2 AM UTC)
- **Historical Data**: Company records and changes over time

---

## Data Sources and Collection

### Official Sources
This workflow collects data from these Danish government sources:

| Source | Agency | Purpose | Data Type |
|--------|--------|---------|-----------|
| CVR Register | Danish Business Authority | Company registrations and details | REST API |
| DAWA Address Service | Danish Agency for Data Supply | Address geocoding and validation | REST API |
| Financial Documents | Danish Business Authority | Company financial reports | REST API |

### How We Collect the Data

#### CVR Company Data
- **Collection Method**: REST API calls to CVR register
- **Frequency**: Weekly updates to capture new registrations and changes
- **Format**: JSON responses from official API
- **Quality Controls**: Validation against official schemas, duplicate detection

#### Address Geocoding
- **Collection Method**: DAWA API for address standardization and coordinates
- **Frequency**: Real-time geocoding during processing
- **Format**: Structured address components with coordinates
- **Quality Controls**: Address validation, coordinate accuracy checks

### Data Privacy and Compliance
- **Personal Data**: Only processes publicly available business information
- **Anonymization**: Personal details of business owners are filtered out
- **Legal Compliance**: GDPR compliant, uses only public business registry data
- **Access Restrictions**: Business information only, no personal data included

---

## Data Processing Steps

### 🥉 Bronze Layer: Raw Data Collection
**What happens**: We collect raw company data from CVR register and related sources
**Why**: Preserves original data for audit trails and reprocessing
**Output**: Raw JSON files with company registrations, addresses, and financial data

**No Changes Made**: Data stored exactly as received from CVR register

### 🥈 Silver Layer: Data Cleaning and Standardization
**What happens**: We clean and standardize company information
**Why**: Raw CVR data has inconsistencies and needs normalization

**Specific transformations**:
- **Address standardization**: Convert addresses to consistent format using DAWA
- **Company name normalization**: Clean and standardize business names
- **Industry classification**: Standardize NACE industry codes
- **Data validation**: Check for completeness and accuracy

**Quality checks**:
- **Completeness**: Verify required fields are present
- **Accuracy**: Validate addresses and coordinates
- **Consistency**: Check data against previous versions

**Output**: Standardized Parquet files with clean company data

### 🥇 Gold Layer: Enriched Business Intelligence
**What happens**: We enrich company data with additional business intelligence
**Why**: Creates analysis-ready datasets with enhanced value

**Value-added features**:
- **Geocoded addresses**: Precise coordinates for all company locations
- **Business relationships**: Parent-subsidiary relationships and ownership structures
- **Financial indicators**: Key financial metrics from annual reports
- **Agricultural classification**: Identification of agricultural vs non-agricultural businesses
- **Historical tracking**: Changes in company status and ownership over time

**Output**: Analysis-ready datasets for business intelligence and regulatory analysis

---

## Workflow Schedule and Execution

### Weekly Schedule
- **Execution Time**: Every Monday at 2 AM UTC
- **Processing Duration**: ~180 minutes (3 hours)
- **Dependencies**: None (runs independently)

### Processing Stages
1. **P-Number Collection** (30 min): Gather production unit numbers from various sources
2. **Company Fetching** (60 min): Retrieve detailed company information from CVR
3. **Address Geocoding** (45 min): Geocode and validate all company addresses  
4. **Financial Documents** (30 min): Collect and process financial reports
5. **Data Consolidation** (15 min): Merge all data sources into final datasets

### Resource Requirements
- **Memory**: 8-16GB RAM for large company datasets
- **Storage**: ~10GB temporary space for processing
- **Network**: High-bandwidth for API calls to CVR and DAWA services

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Good | ~95% of companies have complete basic information |
| **Accuracy** | Excellent | Official government source, high accuracy |
| **Timeliness** | Good | Weekly updates capture most changes within 7 days |
| **Consistency** | Good | Standardized processing ensures consistent format |

### Known Issues and Limitations

#### Data Gaps
- **Historical gaps**: Some older companies may have incomplete historical data
- **Timing delays**: New registrations may take 1-7 days to appear in CVR

#### Quality Issues
- **Address accuracy**: Rural addresses may have lower geocoding accuracy
- **Business classification**: Some companies may be misclassified regarding agricultural activities

#### Methodological Limitations
- **Scope limitation**: Only covers officially registered businesses
- **Update frequency**: Weekly updates mean some very recent changes may be missing

### Recommended Uses
✅ **This data is good for**:
- Business intelligence and market analysis
- Regulatory compliance tracking
- Ownership structure analysis
- Geographic distribution of agricultural businesses

⚠️ **Use with caution for**:
- Real-time business status - Weekly updates may miss very recent changes
- Small/informal operations - May not capture all agricultural activities

❌ **Not recommended for**:
- Personal information about business owners - This data is filtered out
- Immediate regulatory actions - Use real-time CVR queries for urgent matters

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Who owns agricultural land in a specific area?** - Use geocoded company addresses with ownership data
2. **How has agricultural business consolidation changed over time?** - Analyze historical ownership patterns
3. **Which companies are subject to specific regulations?** - Filter by industry codes and business activities

### Example Analyses
#### Agricultural Business Concentration
**Question**: How concentrated is agricultural land ownership in Denmark?
**Data Used**: Company ownership data, subsidiary relationships, agricultural classification
**Method**: Analyze parent-subsidiary structures and land ownership patterns
**Limitations**: Only captures officially registered ownership structures

#### Regional Business Distribution
**Question**: Where are agricultural businesses concentrated geographically?
**Data Used**: Geocoded company addresses, industry classifications
**Method**: Spatial analysis of business locations by municipality/region
**Limitations**: Based on registered addresses, not operational locations

### Data Access
- **Public Access**: Aggregated statistics and anonymized insights
- **API Access**: Available through landbruget.dk API for registered users
- **Download Options**: Parquet files for research use
- **Integration**: Links with field ownership, regulatory compliance, and financial data

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Companies Dataset
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
cvr_number | INTEGER | Official CVR number | 12345678
company_name | TEXT | Official company name | "Andersen Landbrug ApS"
address_full | TEXT | Complete standardized address | "Hovedgade 123, 4000 Roskilde"
coordinates_lat | FLOAT | Latitude coordinate | 55.641944
coordinates_lon | FLOAT | Longitude coordinate | 12.080833
industry_code | TEXT | NACE industry classification | "01.110"
status | TEXT | Company status | "ACTIVE"
registration_date | DATE | Date of registration | 2015-03-15
```

### Storage Locations
- **Bronze**: `gs://landbruget-data/bronze/cvr_enrichment/{timestamp}/`
- **Silver**: `gs://landbruget-data/silver/cvr_enrichment/{timestamp}/`
- **Gold**: `gs://landbruget-data/gold/cvr_enrichment/{timestamp}/`

### Processing Infrastructure
- **Platform**: GitHub Actions on Ubuntu runners
- **Resources**: 16GB RAM, 4 CPU cores
- **Dependencies**: CVR API access, DAWA API, Google Cloud Storage

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Data Engineering Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "CVR Enrichment" label
- **Access Problems**: Contact technical support team
- **Feature Requests**: Submit enhancement requests via project channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed monthly
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural data accessible and trustworthy.*

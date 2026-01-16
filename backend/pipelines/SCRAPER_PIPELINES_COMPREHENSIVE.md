# Danish Agricultural Regulatory Scraper Pipelines

> **Automated Data Collection**: Three specialized web scraping pipelines for Danish agricultural and environmental regulatory compliance data

---

## Overview

The Danish Agricultural Regulatory Scraper Pipelines consist of three sophisticated web scraping systems that automatically collect, process, and standardize regulatory compliance data from Danish government authorities. These pipelines use advanced web scraping techniques, including form automation, token-based authentication, asynchronous processing, and containerized execution to gather critical regulatory data for agricultural compliance monitoring, environmental oversight, and policy analysis.

### Why These Scrapers Matter
- **Regulatory Transparency**: Automated collection of regulatory data that would otherwise require manual monitoring
- **Compliance Monitoring**: Real-time tracking of agricultural and environmental compliance violations and inspections
- **Policy Research**: Comprehensive datasets for analyzing regulatory effectiveness and industry compliance patterns
- **Agricultural Intelligence**: Integration of regulatory data with agricultural operations for risk assessment and compliance planning
- **Public Accountability**: Transparent access to government regulatory enforcement data

---

## Pipeline Architecture Overview

All three scraper pipelines follow a consistent medallion architecture with specialized processing for different data sources:

| Pipeline | Target Authority | Data Source | Collection Method | Update Frequency |
|----------|------------------|-------------|-------------------|------------------|
| **BMD Scraper** | Miljøstyrelsen (Environmental Protection Agency) | Bekæmpelsesmiddel Database | Excel download automation | Monthly (1st at 2 AM UTC) |
| **DMA Scraper** | Miljøstyrelsen (Environmental Protection Agency) | Environmental Authority Database | Async web scraping + PDF collection | Monthly via GitHub Actions |
| **Arbejdstilsynet Scraper** | Arbejdstilsynet (Work Environment Authority) | Workplace Inspection Database | CSV data processing | Configurable date range |

---

## BMD Scraper Pipeline: Pesticide Database Collection

### What This Pipeline Does
The BMD (Bekæmpelsesmiddel Database) Scraper automates the collection of comprehensive pesticide registration and authorization data from the Danish Environmental Protection Agency. Using sophisticated form automation and token-based authentication, it downloads complete Excel datasets containing pesticide product information, active ingredients, authorization status, and regulatory compliance data.

### Technical Implementation

#### Advanced Web Scraping Architecture
- **Token-Based Authentication**: Automated extraction of `__RequestVerificationToken` from export dialog forms
- **Form Automation**: Programmatic completion of complex search forms with multiple parameters
- **Document Generation**: Automated triggering of server-side Excel document generation
- **Download Management**: Robust file download with integrity verification and retry logic
- **Session Management**: Persistent HTTP sessions with proper cookie handling

#### Data Processing Workflow
```python
# BMD Scraping Process
1. GET /External/Entry/ExportDialog → Extract verification token
2. POST /External/Entry/GenerateDocument → Trigger Excel generation  
3. GET /External/Entry/DownloadDocument → Download generated Excel file
4. Verify file integrity and save with metadata
5. Transform to Parquet format with data quality validation
```

#### Bronze Layer: Raw Data Collection
**Data Source**: `https://bmd.mst.dk` (Danish Pesticide Database)
**Output Format**: Raw Excel files with comprehensive metadata
**Processing Details**:
- **Form Parameters**: 20+ search parameters including ProductName, RegNo, CASNumber, AuthorizationHolder
- **Token Extraction**: BeautifulSoup parsing of HTML forms to extract CSRF tokens
- **Document Generation**: Server-side Excel generation with polling for completion
- **File Validation**: Checksum verification and metadata extraction
- **Error Handling**: Comprehensive error recovery with detailed logging

#### Silver Layer: Data Standardization
**Transformation Process**:
- **Column Standardization**: Lowercase naming with underscore separation
- **Data Type Casting**: Proper INTEGER, FLOAT, TEXT, and DATE type assignments
- **Date Parsing**: Standardized date format conversion for temporal analysis
- **Status Normalization**: Standardization of authorization and approval status fields
- **Quality Validation**: Comprehensive data quality checks and issue reporting

### Key Data Products
- **Pesticide Registrations**: Complete database of approved pesticide products
- **Active Ingredients**: Detailed chemical composition and CAS number mappings
- **Authorization Status**: Current approval status and regulatory compliance information
- **Usage Classifications**: Permitted uses and application restrictions
- **Regulatory Timeline**: Historical authorization and modification dates

---

## DMA Scraper Pipeline: Environmental Authority Company Data

### What This Pipeline Does
The DMA (Miljøaktør Database) Scraper performs comprehensive data collection from the Danish Environmental Authority's company database. Using asynchronous web scraping techniques, it collects detailed company information, environmental permits, inspection records, enforcement actions, and regulatory compliance data for agricultural and industrial facilities.

### Technical Implementation

#### Asynchronous Web Scraping Architecture
- **Concurrent Processing**: Async/await pattern with configurable concurrency limits (20 concurrent requests)
- **Session Management**: Persistent aiohttp sessions with connection pooling
- **Rate Limiting**: Intelligent request throttling to prevent server overload
- **Error Recovery**: Comprehensive retry logic with exponential backoff
- **Memory Management**: Streaming processing for large datasets

#### Multi-Stage Data Collection
```python
# DMA Scraping Process
1. Company Discovery → POST /soeg/page with livestock activity filters
2. Detail Scraping → Async collection of company-specific data
3. Section Processing → Extraction of Grunddata, Adresse, Aktiviteter, etc.
4. Table Scraping → Collection of Tilsyn, Håndhævelser, Afgørelser tables
5. PDF Collection → Download of regulatory documents and reports
6. CVR Integration → Extraction and validation of company registration numbers
```

#### Bronze Layer: Multi-Source Data Collection
**Data Source**: `https://dma.mst.dk` (Environmental Authority Database)
**Collection Strategy**:
- **Company Search**: POST requests with livestock-specific activity codes (VL20000112, VL20000430, etc.)
- **Detail Extraction**: Async scraping of company detail pages with BeautifulSoup parsing
- **Document Collection**: Automated PDF download for regulatory documents
- **Table Processing**: Extraction of structured data from inspection and enforcement tables
- **Date Filtering**: Configurable date range filtering based on Tilsynsdato (inspection dates)

#### Silver Layer: Data Integration and Standardization
**Processing Components**:
- **Company Data Harmonization**: Standardization of company information across multiple data sources
- **Inspection Record Processing**: Normalization of inspection dates, findings, and compliance status
- **Enforcement Action Tracking**: Structured processing of regulatory enforcement actions
- **Document Metadata**: Comprehensive metadata extraction from collected PDF documents
- **CVR Number Validation**: Integration with Danish company registration system

### Key Data Products
- **Company Profiles**: Comprehensive environmental compliance profiles for agricultural companies
- **Inspection Records**: Historical inspection data with findings and compliance status
- **Enforcement Actions**: Regulatory enforcement actions and penalties
- **Environmental Permits**: Permit status and compliance requirements
- **Regulatory Documents**: Complete collection of regulatory correspondence and reports

---

## Arbejdstilsynet Scraper Pipeline: Workplace Safety Inspections

### What This Pipeline Does
The Arbejdstilsynet (Work Environment Authority) Scraper processes workplace safety inspection data from the Danish Work Environment Authority. Using containerized processing with Docker, it collects, filters, and standardizes workplace inspection records, safety violations, and compliance data specifically focused on agricultural workplaces and safety incidents.

### Technical Implementation

#### Containerized Processing Architecture
- **Docker Integration**: Full containerization with xvfb for virtual display support
- **Environment Isolation**: Isolated processing environment with proper dependency management
- **Volume Mounting**: Persistent data storage with host filesystem integration
- **Resource Management**: Configurable memory and CPU limits for scalable processing

#### Date-Range Processing System
```python
# Arbejdstilsynet Processing Workflow
1. CSV Data Ingestion → Download from configurable SOURCE_CSV_URL
2. Date Range Filtering → Filter based on --start-date and --end-date parameters
3. Data Cleaning → Column renaming, deduplication, and normalization
4. Privacy Protection → PII detection and anonymization
5. Format Conversion → Parquet export for efficient querying
6. GCS Integration → Optional cloud storage upload
```

#### Bronze Layer: Raw Data Ingestion
**Data Source**: Configurable CSV URL (via SOURCE_CSV_URL environment variable)
**Processing Details**:
- **CSV Download**: Direct download of raw inspection data from government servers
- **Metadata Capture**: Comprehensive metadata including source URL, fetch timestamp, and data lineage
- **File Integrity**: Checksum validation and record count verification
- **Timestamp Management**: Timestamped directory structure for historical data tracking

#### Silver Layer: Data Cleaning and Standardization
**Transformation Process**:
- **Column Standardization**: Consistent naming conventions following project standards
- **Data Deduplication**: Removal of duplicate inspection records
- **Value Normalization**: Standardization of Danish special characters (æ, ø, å)
- **Type Conversion**: Appropriate data type casting for analysis
- **Privacy Protection**: Automated PII detection and anonymization
- **Quality Validation**: Comprehensive data quality checks and validation reports

### Key Data Products
- **Workplace Inspections**: Complete records of workplace safety inspections in agricultural settings
- **Safety Violations**: Detailed violation records with severity and compliance requirements
- **Compliance Tracking**: Historical compliance trends and improvement patterns
- **Industry Analysis**: Sector-specific safety performance metrics
- **Regulatory Enforcement**: Enforcement actions and penalty information

---

## Shared Technical Infrastructure

### Common Architecture Patterns

#### Medallion Architecture Implementation
All three scrapers implement consistent medallion architecture:
- **Bronze Layer**: Raw data preservation with comprehensive metadata
- **Silver Layer**: Cleaned, standardized data optimized for analysis
- **Gold Layer**: Analytical datasets (implemented via Unified Pipeline integration)

#### Advanced Error Handling and Monitoring
- **Retry Logic**: Exponential backoff with configurable retry limits
- **Circuit Breakers**: Automatic failure detection and recovery mechanisms  
- **Comprehensive Logging**: Detailed execution logs with performance metrics
- **Health Monitoring**: Real-time monitoring of scraping success rates and data quality

#### Google Cloud Storage Integration
- **Optimized GCS Access**: Integration with unified GCS access patterns
- **Automatic Uploads**: Production-mode automatic upload to cloud storage
- **Metadata Preservation**: Complete metadata preservation in cloud storage
- **Data Lineage**: Full traceability from source to processed data

### Pipeline Metadata and Data Tracing
```python
# Integrated Pipeline Metadata System
- Source Attribution: Complete source URL and authority information
- Processing Timestamps: Detailed execution timing and duration tracking
- Data Quality Metrics: Comprehensive quality assessment and validation
- CVR Integration: Automatic extraction and validation of company registration numbers
- Cross-Pipeline Integration: Seamless integration with other agricultural data pipelines
```

### Deployment and Orchestration

#### GitHub Actions Integration
- **Automated Scheduling**: Monthly execution schedules with configurable triggers
- **Environment Management**: Production and development environment support
- **Secret Management**: Secure handling of credentials and API keys
- **Artifact Management**: Automated artifact collection and storage

#### Docker Containerization
- **Consistent Environments**: Identical execution environments across development and production
- **Dependency Management**: Isolated dependency management with uv package manager
- **Resource Optimization**: Configurable resource limits and optimization
- **Virtual Display Support**: xvfb integration for browser automation in headless environments

#### Configuration Management
```bash
# Environment Configuration
SOURCE_CSV_URL=https://government.dk/data/inspections.csv
GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
GCS_BUCKET=landbrugsdata-raw-data
LOG_LEVEL=INFO
MAX_CONCURRENT_REQUESTS=20
RETRY_ATTEMPTS=3
```

---

## Data Quality and Validation

### Comprehensive Quality Assurance
- **Data Integrity**: Checksum validation and file integrity verification
- **Completeness Checks**: Validation of required fields and data completeness
- **Format Consistency**: Standardization of data formats and value representations
- **Temporal Validation**: Date range validation and temporal consistency checks
- **Cross-Reference Validation**: Validation against external data sources (CVR registry)

### Privacy and Compliance
- **PII Protection**: Automated detection and anonymization of personally identifiable information
- **GDPR Compliance**: Privacy-compliant data processing and storage
- **Data Retention**: Configurable data retention policies
- **Access Controls**: Secure access controls for sensitive regulatory data

### Performance Monitoring
- **Processing Metrics**: Detailed performance metrics and execution timing
- **Success Rate Tracking**: Monitoring of scraping success rates and failure patterns
- **Resource Usage**: Memory and CPU usage monitoring with optimization recommendations
- **Data Volume Tracking**: Monitoring of data collection volumes and growth patterns

---

## Integration and Downstream Usage

### Cross-Pipeline Integration
- **CVR Number Extraction**: Automatic extraction and sharing of company registration numbers
- **Unified Pipeline Integration**: Seamless integration with agricultural field and farm data
- **Regulatory Compliance Analysis**: Integration with pesticide application and compliance monitoring
- **Risk Assessment**: Integration with building proximity and exposure risk analysis

### Analytical Applications
- **Regulatory Compliance Monitoring**: Real-time monitoring of agricultural regulatory compliance
- **Policy Impact Assessment**: Analysis of regulatory policy effectiveness
- **Industry Benchmarking**: Comparative analysis of compliance across agricultural sectors
- **Risk Profiling**: Development of risk profiles based on regulatory history

### Data Products and APIs
- **Standardized Datasets**: High-quality, standardized datasets for research and analysis
- **API Integration**: RESTful API access to processed regulatory data
- **Real-time Updates**: Near real-time updates of regulatory changes and compliance status
- **Historical Analysis**: Complete historical datasets for trend analysis and research

These three scraper pipelines form a comprehensive regulatory data collection infrastructure, providing essential insights into Danish agricultural and environmental regulatory compliance through sophisticated web scraping, automated processing, and advanced data integration techniques.

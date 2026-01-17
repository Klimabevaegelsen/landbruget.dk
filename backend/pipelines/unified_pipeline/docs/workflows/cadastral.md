# Cadastral (Danish Property Boundaries) Workflow

> **Monthly Foundation Processing**: Official Danish property boundaries from Datafordeler WFS service

---

## What This Workflow Does

The Cadastral workflow collects and processes official Danish property boundary data (cadastral parcels) from the Danish Agency for Data Supply and Infrastructure's Datafordeler WFS service. This workflow provides the foundational property boundary dataset essential for spatial analysis, property-agricultural field relationships, and land use studies across Denmark.

### Why This Data Matters
- **Official Property Boundaries**: Authoritative property parcel boundaries for all Danish real estate
- **Spatial Analysis Foundation**: Essential spatial data for property-based analysis and modeling
- **Agricultural Integration**: Property boundaries enable linking agricultural fields to property ownership
- **Legal Compliance**: Official cadastral data for regulatory and compliance purposes
- **Land Use Planning**: Foundation data for land use analysis and urban planning studies

### Key Statistics
- **Data Coverage**: Complete national Danish property cadastral data (~2.8 million parcels)
- **Processing Scale**: High-volume spatial data processing (~16GB memory requirements)
- **Data Quality**: Official government data with comprehensive attribute validation
- **Integration Role**: Foundation for property-agricultural field merge and spatial analysis
- **Update Frequency**: Monthly updates ensuring current property boundary information

---

## Data Sources and Collection

### Official Sources
This workflow collects data from the Danish Datafordeler WFS service:

| Data Layer | Purpose | Coverage | Feature Count |
|------------|---------|----------|---------------|
| **SamletFastEjendom_Gaeldende** | Current property parcels | National Denmark | ~2.8M parcels |

### Data Collection Process

#### Datafordeler WFS Integration
- **Service URL**: `https://wfs.datafordeler.dk/MATRIKLEN2/MatGaeldendeOgForeloebigWFS/1.0.0/WFS`
- **Protocol**: WFS 2.0.0 with authenticated access using Datafordeler credentials
- **Data Layer**: `mat:SamletFastEjendom_Gaeldende` (current property parcels)
- **Coordinate System**: EPSG:25832 (Danish UTM Zone 32N) native format
- **Authentication**: Username/password authentication via environment variables

#### Advanced Processing Architecture
- **Batch Processing**: 10,000 features per request with concurrent processing (5 workers)
- **Rate Limiting**: Configurable requests per second (default 2/sec) to respect service limits
- **Memory Optimization**: High-memory processing (16GB) for large spatial datasets
- **Error Recovery**: Comprehensive retry logic and graceful error handling
- **Progress Monitoring**: Detailed logging of processing progress and feature validation

#### Comprehensive Attribute Collection
- **Property Identification**: BFE numbers, local IDs, and namespace information
- **Administrative Data**: Business events, processes, and authority information
- **Temporal Information**: Registration dates and effective dates for property changes
- **Property Characteristics**: Worker housing, common lots, owner apartments, separated roads
- **Agricultural Notation**: Special agricultural land use classifications and notes

### Data Privacy and Compliance
- **Public Property Data**: Official cadastral information available for legitimate analysis purposes
- **Authenticated Access**: Secure access via Datafordeler credentials for authorized use
- **Property Boundaries**: Spatial boundaries for analytical and regulatory purposes
- **Agricultural Integration**: Property data for agricultural business and land use analysis

---

## Data Processing Steps

### 🥉 Bronze Layer: Comprehensive WFS Data Collection
**What happens**: We fetch raw property boundary data from Datafordeler WFS service with full attribute collection
**Why**: Official property boundaries require comprehensive collection to ensure complete spatial and administrative coverage

**Specific processing**:
- **Authenticated WFS Access**: Secure connection to Datafordeler using environment-based credentials
- **Batch Processing**: Efficient processing of ~2.8M property parcels in 10,000-feature chunks
- **Concurrent Processing**: 5 concurrent workers with rate limiting to optimize throughput
- **Feature Count Validation**: Pre-flight total count retrieval for processing planning
- **Progressive Data Collection**: Sequential batch processing with progress monitoring

**Advanced spatial processing**:
- **GML Geometry Parsing**: Direct parsing of GML MultiSurface geometries to WKT format
- **3D Coordinate Handling**: Proper handling of 3D coordinates (extracting X,Y, ignoring Z)
- **Polygon Validation**: Automatic polygon closure validation and repair
- **MultiPolygon Support**: Intelligent handling of single and multi-polygon geometries
- **Coordinate Precision**: High-precision coordinate handling for accurate property boundaries

**Comprehensive attribute extraction**:
- **Property Identifiers**: BFE numbers, local IDs, namespace information
- **Administrative Metadata**: Business events, processes, case IDs, authority information
- **Temporal Data**: Registration and effective dates with proper timezone handling
- **Property Classifications**: Worker housing, common lots, owner apartments flags
- **Agricultural Information**: Agricultural notation and land use classifications

**Quality controls**:
- **Feature Validation**: Comprehensive validation of BFE numbers and geometry presence
- **Parsing Success Monitoring**: Detailed tracking of feature parsing success rates
- **Error Recovery**: Graceful handling of malformed features and parsing errors
- **Progress Tracking**: Real-time monitoring of processing progress with batch statistics

**Output**: Raw property boundary features with complete administrative and spatial information

### 🥈 Silver Layer: Advanced Spatial Data Transformation and Validation
**What happens**: We transform raw property data into standardized, analysis-ready spatial datasets with comprehensive processing
**Why**: Raw WFS data requires coordinate transformation, attribute standardization, and spatial validation for analytical use

**Specific transformations**:

#### High-Performance Data Processing
- **DuckDB Bulk Operations**: Ultra-fast bulk insert processing using DuckDB's VALUES clause optimization
- **Batch Processing**: Large batch sizes (10,000+ features) for maximum performance
- **Memory-Optimized Processing**: Efficient handling of large spatial datasets with 16GB memory allocation
- **Fallback Processing**: Multiple processing strategies (bulk VALUES, executemany, small batches) for robustness

#### Comprehensive Spatial Processing
- **Coordinate Transformation**: Convert from EPSG:25832 (Danish UTM) to EPSG:4326 (WGS84)
- **Geometry Validation**: Comprehensive spatial validation and repair using DuckDB-spatial
- **WKT Processing**: Conversion from raw WKT strings to proper DuckDB geometry objects
- **Spatial Quality Assurance**: Validation of geometry integrity and coordinate accuracy

#### Advanced Attribute Processing
- **Data Type Conversion**: Proper typing for all numeric, date, boolean, and text fields
- **Temporal Processing**: ISO timestamp parsing with timezone handling for registration dates
- **Boolean Standardization**: Consistent boolean conversion for property characteristic flags
- **Null Handling**: Appropriate handling of missing values and empty fields
- **Validation Rules**: Comprehensive validation of required fields (BFE number, geometry)

#### Dissolved Dataset Creation
- **Spatial Aggregation**: Creation of dissolved dataset by merging adjacent properties with similar attributes
- **Attribute Grouping**: Grouping by business event, process, authority, and property characteristics
- **Geometry Union**: Advanced spatial union operations using `ST_Union_Agg` for merged boundaries
- **Statistical Aggregation**: Count, minimum, and maximum statistics for merged property groups
- **Analysis Optimization**: Simplified dataset for regional and aggregate analysis purposes

**Quality checks**:
- **Processing Statistics**: Detailed tracking of valid vs. skipped features during transformation
- **Geometry Integrity**: Validation of all spatial geometries and coordinate transformations
- **Attribute Completeness**: Assessment of data completeness across all property fields
- **Performance Monitoring**: Real-time monitoring of bulk processing performance and memory usage

**Output**: Standardized property boundary datasets (individual and dissolved) with validated geometries and consistent attributes

### 🥇 Gold Layer: Integration with Property-Agricultural Analysis
**What happens**: Cadastral property data is integrated into comprehensive property-agricultural analysis workflows
**Why**: Property boundaries are essential for linking agricultural operations to property ownership and land use analysis

**Integration applications**:
- **Property-Cadastral Merge**: Direct integration with property ownership data for comprehensive land analysis
- **Agricultural Field Linkage**: Spatial relationships between property boundaries and agricultural field boundaries
- **Land Use Analysis**: Property-based land use classification and change analysis
- **Ownership Analysis**: Integration of property boundaries with agricultural business ownership data

**Output**: Foundation spatial data enabling comprehensive property-based agricultural and land use analysis

---

## Workflow Schedule and Execution

### Monthly Foundation Processing
- **Schedule**: 1st of every month at 1 AM UTC (foundation batch, priority 3)
- **Execution Type**: Automated monthly processing as foundation data source
- **Processing Duration**: ~2 hours for complete data collection (estimated 120 minutes)
- **Dependencies**: None (independent foundation data source)
- **Downstream Dependencies**: Required by `property_cadastral_merge` workflow

### Processing Performance
- **Data Volume**: ~2.8 million property parcels, multi-GB spatial dataset
- **Memory Usage**: High (~16GB) for large spatial dataset processing and bulk operations
- **Network**: Sustained WFS communication with Datafordeler service (rate-limited)
- **Storage**: Multi-GB storage requirements for complete property boundary data
- **Concurrency**: 5 concurrent workers with 2 requests/second rate limiting

### Advanced Features
- **Authenticated Access**: Secure Datafordeler WFS access with credential management
- **Rate Limiting**: Configurable request throttling to respect service capacity
- **Bulk Processing**: Optimized DuckDB bulk operations for high-performance data processing
- **Memory Management**: High-memory processing with efficient batch handling
- **Dissolved Dataset**: Additional aggregated dataset for regional analysis

### Resource Management
- **High Memory Processing**: Dedicated high-memory instances for large spatial datasets
- **Disk Space Management**: Automatic cleanup and efficient storage management
- **Network Optimization**: Rate-limited concurrent processing for optimal throughput
- **Error Handling**: Comprehensive error recovery and processing continuation

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Complete official Danish property cadastral data from authoritative government source |
| **Accuracy** | Excellent | Danish Agency for Data Supply official data with rigorous spatial validation |
| **Timeliness** | Good | Monthly updates as new property registrations and changes become available |
| **Spatial Precision** | Excellent | High-precision property boundaries with comprehensive geometry validation |

### Known Issues and Limitations

#### Service and Infrastructure Constraints
- **WFS Service Dependency**: Processing success depends on Datafordeler WFS service availability
- **Authentication Requirements**: Requires valid Datafordeler credentials for data access
- **Rate Limiting**: Service rate limits require careful request throttling (2 requests/second)
- **Large Dataset Processing**: Memory-intensive processing requiring substantial computational resources

#### Data Processing and Technical Limitations
- **Processing Duration**: Complete processing requires ~2 hours for all property parcels
- **Memory Requirements**: High memory requirements (16GB) for bulk spatial processing
- **Coordinate Transformation**: Minor precision changes during CRS conversion (UTM to WGS84)
- **Dissolved Dataset Complexity**: Spatial union operations can be computationally intensive

#### Integration and Usage Considerations
- **Property-Field Relationships**: Spatial relationships between properties and agricultural fields require careful analysis
- **Temporal Consistency**: Property boundary changes may not align temporally with agricultural field updates
- **Administrative Complexity**: Complex administrative attributes require domain knowledge for interpretation
- **Scale Considerations**: National-scale processing requires robust infrastructure and error handling

### Recommended Uses
✅ **This data is excellent for**:
- Property-based spatial analysis requiring official Danish property boundaries
- Agricultural land use analysis linking fields to property ownership
- Regional land use planning and development analysis using official property data
- Legal and regulatory analysis requiring authoritative property boundary information
- Research requiring comprehensive Danish property cadastral information

⚠️ **Use with caution for**:
- Real-time property analysis - Monthly updates with processing delays
- Historical property analysis - Limited to current property boundaries
- Cross-border analysis - Data specific to Danish cadastral system and classifications

❌ **Not recommended for**:
- Individual property management - Aggregated data, not property-specific operational information
- Daily property transactions - Monthly updates, not real-time transaction data
- International property comparisons without context - Danish-specific cadastral system and legal framework

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Where are the official boundaries of all Danish properties?** - Complete property parcel boundaries for spatial analysis
2. **How do agricultural fields relate to property ownership?** - Spatial relationships between field boundaries and property parcels
3. **What are the characteristics of properties in specific regions?** - Property classification, ownership types, and administrative information
4. **Which properties have agricultural land use designations?** - Agricultural notation and land use classification analysis

### Example Analyses
#### Property-Agricultural Field Relationship Analysis
**Question**: How do agricultural field boundaries align with property ownership boundaries?
**Data Used**: Cadastral property boundaries spatially joined with FVM agricultural field boundaries
**Method**: Spatial intersection analysis to identify field-property relationships and ownership patterns
**Output**: Property-field relationship mapping showing agricultural operations relative to property ownership
**Limitations**: Temporal misalignment between property updates and agricultural field boundary updates

#### Regional Land Use Analysis
**Question**: What is the distribution of property types and agricultural land use across Danish regions?
**Data Used**: Cadastral property data with agricultural notation and dissolved regional boundaries
**Method**: Spatial aggregation by administrative regions with property classification analysis
**Output**: Regional land use distribution maps and statistical analysis of property characteristics
**Limitations**: Property classifications may not capture all land use nuances

#### Agricultural Property Ownership Analysis
**Question**: What is the scale and distribution of agricultural property ownership in Denmark?
**Data Used**: Cadastral property boundaries with agricultural notation and area calculations
**Method**: Area-based analysis of agricultural properties with ownership pattern identification
**Output**: Agricultural property size distribution and regional ownership concentration analysis
**Limitations**: Property boundaries may not align perfectly with operational agricultural units

### Data Access
- **Research Access**: Complete property boundary datasets for academic and scientific land use research
- **Policy Access**: Official property data for land use policy development and analysis
- **Industry Access**: Property boundary data for agricultural and real estate industry analysis
- **Regulatory Access**: Official cadastral data for legal compliance and regulatory verification

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Property Boundary Data (Silver Layer)

**Individual Property Parcels**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
bfe_number | BIGINT | Official property identifier | 1234567890
business_event | VARCHAR | Administrative business event | "Opdeling"
business_process | VARCHAR | Administrative process type | "Matrikulær sag"
latest_case_id | VARCHAR | Latest case identifier | "2024-123456"
id_local | VARCHAR | Local identifier | "abc123"
id_namespace | VARCHAR | Namespace identifier | "https://data.gov.dk"
registration_from | TIMESTAMP | Registration start date | "2024-01-15T10:30:00"
effect_from | TIMESTAMP | Effective start date | "2024-01-15T10:30:00"
authority | VARCHAR | Responsible authority | "Geodatastyrelsen"
is_worker_housing | BOOLEAN | Worker housing flag | false
is_common_lot | BOOLEAN | Common lot flag | false
has_owner_apartments | BOOLEAN | Owner apartments flag | true
is_separated_road | BOOLEAN | Separated road flag | false
agricultural_notation | VARCHAR | Agricultural classification | "Landbrugsejendom"
geometry | GEOMETRY | Property boundary polygon | POLYGON((...)
created_at | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

**Dissolved Property Data**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
business_event | VARCHAR | Administrative business event | "Opdeling"
business_process | VARCHAR | Administrative process type | "Matrikulær sag"
authority | VARCHAR | Responsible authority | "Geodatastyrelsen"
is_worker_housing | BOOLEAN | Worker housing flag | false
is_common_lot | BOOLEAN | Common lot flag | false
has_owner_apartments | BOOLEAN | Owner apartments flag | true
agricultural_notation | VARCHAR | Agricultural classification | "Landbrugsejendom"
parcel_count | BIGINT | Number of merged parcels | 15
min_bfe_number | BIGINT | Minimum BFE number | 1234567890
max_bfe_number | BIGINT | Maximum BFE number | 1234567905
geometry | GEOMETRY | Dissolved boundary polygon | MULTIPOLYGON((...)
created_at | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

### Storage Locations
- **Bronze**: `gs://landbrugsdata-raw-data/bronze/cadastral/{timestamp}/data.json`
- **Silver**: `gs://landbrugsdata-raw-data/silver/cadastral/{timestamp}/data.parquet`
- **Silver Dissolved**: `gs://landbrugsdata-raw-data/silver/cadastral_dissolved/{timestamp}/data.parquet`

### Processing Infrastructure
- **Platform**: Automated monthly execution as foundation data source
- **Resources**: 16GB RAM, high-memory instances for large spatial datasets
- **Dependencies**: Datafordeler WFS service access with valid credentials
- **Performance**: ~2 hours for complete processing (120 minutes estimated)

### WFS Service Details
- **Service URL**: `https://wfs.datafordeler.dk/MATRIKLEN2/MatGaeldendeOgForeloebigWFS/1.0.0/WFS`
- **Protocol**: WFS 2.0.0 with username/password authentication
- **Layer**: `mat:SamletFastEjendom_Gaeldende` (current property parcels)
- **Coordinate System**: EPSG:25832 (source) → EPSG:4326 (target)
- **Batch Size**: 10,000 features per request
- **Rate Limiting**: 2 requests per second (configurable)

### Authentication Configuration
```bash
# Required environment variables
DATAFORDELER_USERNAME=your_username
DATAFORDELER_PASSWORD=your_password
# Alternative variable names also supported
WFS_USERNAME=your_username  
WFS_PASSWORD=your_password
```

### Processing Performance Features
- **Bulk Insert Optimization**: DuckDB VALUES clause for maximum performance
- **Memory Management**: Efficient handling of large spatial datasets
- **Concurrent Processing**: 5 concurrent workers with rate limiting
- **Fallback Strategies**: Multiple processing approaches for robustness
- **Progress Monitoring**: Real-time processing statistics and performance metrics

### Spatial Processing Features
- **Coordinate Transformation**: EPSG:25832 → EPSG:4326 using DuckDB-spatial
- **Geometry Validation**: Comprehensive validation and repair of spatial geometries
- **GML Parsing**: Direct GML MultiSurface to WKT conversion without external libraries
- **Polygon Validation**: Automatic polygon closure and coordinate validation
- **Spatial Union**: Advanced dissolved dataset creation using ST_Union_Agg

### Quality Assurance Features
- **Feature Count Validation**: Pre-flight checks for data availability and processing planning
- **Parsing Success Monitoring**: Detailed tracking of feature parsing success rates
- **Geometry Integrity**: Validation of spatial data quality and coordinate accuracy
- **Attribute Completeness**: Assessment of property attribute completeness and validity
- **Performance Tracking**: Memory usage and processing performance monitoring

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Spatial Data Infrastructure Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Cadastral" label
- **WFS Service Problems**: Contact system administrators for Datafordeler service issues
- **Authentication Issues**: Contact credentials administrator for access problems

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when Datafordeler service changes or data schema updates occur
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make property spatial data accessible and trustworthy.*

# DAGI (Danish Administrative Geographic Division) Workflow

> **Monthly Foundation Processing**: Danish administrative boundaries from DAWA API

---

## What This Workflow Does

The DAGI (Danish Administrative Geographic Division) workflow collects and processes official Danish administrative boundary data from the Danish Agency for Data Supply and Infrastructure's DAWA API. This workflow provides essential administrative spatial datasets including municipalities, regions, postal codes, and landsdele (parts of country) that serve as the foundation for administrative analysis and spatial aggregation across Denmark.

### Why This Data Matters
- **Administrative Boundaries**: Official administrative division boundaries for all levels of Danish government
- **Spatial Aggregation Foundation**: Essential spatial data for administrative-level analysis and reporting
- **Geographic Context**: Administrative context for all Danish spatial data and analysis
- **Policy Analysis**: Administrative boundaries for government policy analysis and implementation
- **Statistical Reporting**: Foundation data for administrative-level statistical reporting and analysis

### Key Statistics
- **Data Coverage**: Complete national Danish administrative boundaries (4 administrative levels)
- **Processing Scale**: National coverage with full administrative hierarchy
- **Administrative Levels**: Municipalities, regions, postal codes, and landsdele
- **Integration Role**: Foundation data for administrative spatial analysis and aggregation
- **Update Frequency**: Monthly updates ensuring current administrative boundary information

---

## Data Sources and Collection

### Official Sources
This workflow collects data from the Danish DAWA API (Data Supply and Infrastructure):

| Administrative Level | Purpose | Coverage | Feature Count |
|---------------------|---------|----------|---------------|
| **Kommuner** | Municipalities | National Denmark | 98 municipalities |
| **Regioner** | Regions | National Denmark | 5 regions |
| **Landsdele** | Parts of country | National Denmark | 11 landsdele |
| **Postnumre** | Postal codes | National Denmark | ~1,200 postal codes |

### Data Collection Process

#### DAWA API Integration
- **Service URL**: `https://api.dataforsyningen.dk` (official Danish data supply API)
- **Protocol**: REST API with GeoJSON format responses
- **Authentication**: No authentication required (public API)
- **Data Format**: GeoJSON with complete administrative boundaries and attributes
- **Coordinate System**: WGS84 (EPSG:4326) native format from API

#### Advanced Processing Architecture
- **Concurrent Processing**: 5 concurrent requests for efficient data collection
- **Multi-Layer Collection**: Simultaneous processing of all 4 administrative levels
- **Retry Logic**: Exponential backoff retry strategy (3 attempts) for robust API communication
- **Error Recovery**: Graceful handling of API issues and processing continuation
- **Progress Monitoring**: Detailed logging of processing progress for each administrative level

#### Comprehensive Administrative Data Collection
- **Complete Hierarchy**: Full administrative hierarchy from postal codes to regions
- **Official Codes**: Administrative codes, NUTS codes, and region codes
- **Danish Names**: Official Danish names for all administrative divisions
- **Spatial Boundaries**: Precise geometric boundaries for all administrative areas
- **Metadata Integration**: Processing timestamps and data lineage information

### Administrative Level Details
| Level | Danish Name | Code Field | Additional Attributes |
|-------|-------------|------------|----------------------|
| Municipalities | Kommuner | kode | navn, regionskode |
| Regions | Regioner | kode | navn |
| Landsdele | Landsdele | nuts3 | navn |
| Postal Codes | Postnumre | nr | navn |

### Data Privacy and Compliance
- **Public Administrative Data**: Official administrative boundaries available for legitimate analysis
- **Government API**: Authorized access to official Danish administrative data
- **Spatial Boundaries**: Administrative boundaries for analytical and policy purposes
- **Statistical Integration**: Administrative data for statistical reporting and analysis

---

## Data Processing Steps

### 🥉 Bronze Layer: Comprehensive API Data Collection
**What happens**: We fetch raw administrative boundary data from DAWA API across all administrative levels
**Why**: Administrative boundaries require comprehensive collection to ensure complete spatial and hierarchical coverage

**Specific processing**:
- **DAWA API Integration**: Direct connection to official Danish data supply API
- **Multi-Level Collection**: Concurrent fetching of 4 administrative levels (municipalities, regions, landsdele, postal codes)
- **GeoJSON Processing**: Complete GeoJSON feature collection with spatial boundaries and attributes
- **Concurrent Processing**: 5 concurrent requests for optimal API throughput
- **Error Recovery**: Robust error handling allowing processing to continue if individual layers fail

**API endpoint processing**:
- **Municipalities**: `/kommuner?format=geojson` - Complete municipal boundaries with region codes
- **Regions**: `/regioner?format=geojson` - Regional boundaries covering all of Denmark
- **Landsdele**: `/landsdele?format=geojson` - NUTS3 statistical regions for European reporting
- **Postal Codes**: `/postnumre?format=geojson` - Postal code boundaries for address analysis

**Quality controls**:
- **API Response Validation**: Comprehensive validation of GeoJSON structure and content
- **Feature Count Monitoring**: Tracking of expected vs. actual feature counts for completeness
- **Error Recovery**: Graceful handling of API failures and processing continuation
- **Data Completeness**: Verification of essential attributes for each administrative level

**Output**: Raw GeoJSON data for all Danish administrative levels with complete spatial and attribute information

### 🥈 Silver Layer: Advanced Spatial Data Transformation and Standardization
**What happens**: We transform raw GeoJSON data into standardized, analysis-ready spatial datasets with consistent structure
**Why**: Raw API data requires standardization, validation, and spatial processing for analytical use

**Specific transformations**:

#### Advanced GeoJSON Processing
- **DuckDB-Spatial Integration**: Native GeoJSON processing using DuckDB-spatial functions
- **Feature Extraction**: Complete feature extraction with properties and geometries
- **Spatial Validation**: Comprehensive geometry validation using `ST_IsValid`
- **Coordinate Processing**: Direct processing of GeoJSON geometries without external libraries

#### Comprehensive Spatial Processing
- **Geometry Validation**: Comprehensive spatial validation and repair using DuckDB-spatial
- **Area Calculations**: Precise area calculations in square meters using `ST_Area`
- **Centroid Calculations**: Administrative center point calculations using `ST_Centroid`
- **WKT Conversion**: Conversion to Well-Known Text format for downstream compatibility
- **Coordinate System Validation**: Ensures consistent WGS84 coordinate system across all layers

#### Administrative Data Standardization
- **Column Mapping**: Standardized English column names from Danish originals
- **Data Type Conversion**: Proper typing for all administrative codes and names
- **Hierarchical Relationships**: Preservation of administrative hierarchy (municipality → region)
- **Code Standardization**: Consistent handling of administrative codes, NUTS codes, and postal codes
- **Metadata Enrichment**: Addition of processing timestamps and layer type information

#### Multi-Layer Processing
- **Layer-Specific Processing**: Customized processing for each administrative level's requirements
- **Required Column Validation**: Verification of essential columns for each administrative type
- **Dynamic Column Handling**: Flexible handling of varying attribute structures across layers
- **Unified Output Schema**: Consistent output structure across all administrative levels

**Quality checks**:
- **Geometry Integrity**: Validation of all spatial geometries and coordinate accuracy
- **Attribute Completeness**: Assessment of data completeness for all administrative levels
- **Administrative Hierarchy**: Validation of hierarchical relationships between levels
- **Spatial Consistency**: Verification of spatial boundary consistency and coverage

**Output**: Standardized administrative boundary datasets with validated geometries and consistent attributes across all levels

### 🥇 Gold Layer: Integration with Administrative Analysis
**What happens**: DAGI administrative boundary data serves as foundation for administrative spatial analysis
**Why**: Administrative boundaries are essential for spatial aggregation, policy analysis, and statistical reporting

**Integration applications**:
- **Spatial Aggregation**: Administrative boundaries for aggregating field-level and regional data
- **Policy Analysis**: Administrative context for government policy implementation and analysis
- **Statistical Reporting**: Administrative units for statistical data collection and reporting
- **Geographic Context**: Administrative context for all Danish spatial analysis and visualization

**Output**: Foundation administrative data enabling comprehensive administrative-level analysis and reporting

---

## Workflow Schedule and Execution

### Monthly Foundation Processing
- **Schedule**: 1st of every month at 1 AM UTC (foundation batch, priority 5)
- **Execution Type**: Automated monthly processing as foundation data source
- **Processing Duration**: ~1 hour for complete data collection (estimated 60 minutes)
- **Dependencies**: None (independent foundation data source)
- **Downstream Dependencies**: Foundation data for administrative analysis and spatial aggregation

### Processing Performance
- **Data Volume**: Complete Danish administrative boundaries (~1,300 administrative units)
- **Memory Usage**: Moderate memory requirements for GeoJSON processing and spatial operations
- **Network**: API communication with official DAWA service
- **Storage**: Moderate storage requirements for administrative boundary spatial data
- **Concurrency**: 5 concurrent API requests for optimal throughput

### Advanced Features
- **API Integration**: Direct integration with official Danish data supply infrastructure
- **Multi-Level Processing**: Simultaneous processing of 4 administrative levels
- **DuckDB-Spatial Processing**: Advanced spatial processing using DuckDB-spatial functions
- **Column Standardization**: Intelligent mapping from Danish to English standardized column names
- **Error Recovery**: Robust error handling allowing partial success when individual layers fail

### Resource Management
- **Moderate Memory Usage**: Efficient GeoJSON processing and spatial operations
- **API Optimization**: Concurrent processing with appropriate timeout handling
- **Error Handling**: Comprehensive error recovery and processing continuation
- **Storage Efficiency**: Optimized storage of standardized administrative datasets

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Complete national Danish administrative boundaries from authoritative government source |
| **Accuracy** | Excellent | Danish Agency for Data Supply official data with rigorous spatial validation |
| **Timeliness** | Good | Monthly updates as administrative changes and boundary updates are implemented |
| **Spatial Precision** | Excellent | High-precision administrative boundaries with comprehensive geometry validation |

### Known Issues and Limitations

#### Service and Infrastructure Constraints
- **API Service Dependency**: Processing success depends on DAWA API service availability
- **Network Requirements**: Sustained API communication for multi-level data collection
- **Processing Duration**: Complete processing requires ~1 hour for all administrative levels
- **API Rate Limits**: Service may have undocumented rate limits requiring careful request management

#### Data Processing and Technical Limitations
- **Column Standardization**: Danish column names mapped to English may lose linguistic nuance
- **Administrative Changes**: Boundary changes may not be immediately reflected in monthly updates
- **Spatial Precision**: Minor precision variations in boundary definitions across administrative levels
- **Hierarchical Complexity**: Complex administrative relationships require careful interpretation

#### Integration and Usage Considerations
- **Administrative Stability**: Administrative boundaries change infrequently but updates are important
- **Scale Appropriateness**: Different administrative levels appropriate for different analysis scales
- **Temporal Consistency**: Administrative changes may affect temporal analysis consistency
- **International Context**: Danish-specific administrative system may not align with international standards

### Recommended Uses
✅ **This data is excellent for**:
- Administrative-level spatial analysis and aggregation across Denmark
- Government policy analysis requiring official administrative boundaries
- Statistical reporting and analysis using official administrative units
- Geographic context for Danish spatial data visualization and analysis
- Research requiring authoritative Danish administrative boundary information

⚠️ **Use with caution for**:
- Historical administrative analysis - Monthly updates may not capture all historical changes
- Cross-border analysis - Data specific to Danish administrative system
- Fine-scale analysis - Administrative boundaries may be too coarse for detailed local analysis

❌ **Not recommended for**:
- Individual address analysis - Use postal code data for address-level analysis
- Real-time administrative status - Monthly updates, not real-time administrative changes
- International administrative comparisons without context - Danish-specific administrative classifications

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **What are the official boundaries of Danish municipalities and regions?** - Complete administrative boundary data for all levels
2. **How do postal codes relate to municipal and regional boundaries?** - Spatial relationships between administrative hierarchy levels
3. **Which administrative unit contains a specific location in Denmark?** - Point-in-polygon analysis for administrative classification
4. **What is the area and geographic center of Danish administrative units?** - Area calculations and centroid analysis for administrative planning

### Example Analyses
#### Administrative Spatial Aggregation
**Question**: How do agricultural statistics aggregate across Danish administrative levels?
**Data Used**: DAGI administrative boundaries spatially joined with agricultural field and production data
**Method**: Spatial aggregation of field-level data by municipality, region, and landsdele boundaries
**Output**: Administrative-level agricultural statistics with proper spatial aggregation and hierarchy
**Limitations**: Administrative boundaries may not align perfectly with agricultural operational boundaries

#### Policy Implementation Analysis
**Question**: What is the geographic coverage and distribution of government policy implementation?
**Data Used**: DAGI administrative boundaries with policy implementation status data
**Method**: Administrative-level analysis of policy coverage and implementation status by municipality and region
**Output**: Policy implementation maps and statistics by administrative level with geographic context
**Limitations**: Administrative reporting may not reflect actual on-ground policy implementation

#### Regional Statistical Analysis
**Question**: How do demographic and economic indicators vary across Danish regions and landsdele?
**Data Used**: DAGI administrative boundaries with statistical data from Danmarks Statistik
**Method**: Statistical analysis and visualization of indicators aggregated by administrative boundaries
**Output**: Regional statistical analysis with proper administrative context and geographic visualization
**Limitations**: Administrative boundaries may not reflect functional economic or social regions

### Data Access
- **Research Access**: Complete administrative boundary datasets for academic and scientific research
- **Policy Access**: Official administrative data for government policy development and analysis
- **Industry Access**: Administrative boundary data for business analysis and market research
- **Public Access**: Administrative context data for public information and civic applications

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Administrative Boundary Data (Silver Layer)

**Municipalities (Kommuner)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
code | VARCHAR | Municipal code | "0101"
name | VARCHAR | Municipal name | "København"
region_code | VARCHAR | Region code | "1084"
geometry | GEOMETRY | Municipal boundary | POLYGON((...))
is_valid_geometry | BOOLEAN | Geometry validity flag | true
area_m2 | DOUBLE | Area in square meters | 86200000.0
centroid_x | DOUBLE | Centroid longitude | 12.5683
centroid_y | DOUBLE | Centroid latitude | 55.6761
geometry_wkt | VARCHAR | Well-Known Text geometry | "POLYGON((...))"
layer_type | VARCHAR | Administrative layer type | "kommuner"
processed_at | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

**Regions (Regioner)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
code | VARCHAR | Region code | "1084"
name | VARCHAR | Region name | "Hovedstaden"
geometry | GEOMETRY | Regional boundary | POLYGON((...))
is_valid_geometry | BOOLEAN | Geometry validity flag | true
area_m2 | DOUBLE | Area in square meters | 2561000000.0
centroid_x | DOUBLE | Centroid longitude | 12.3456
centroid_y | DOUBLE | Centroid latitude | 55.7890
geometry_wkt | VARCHAR | Well-Known Text geometry | "POLYGON((...))"
layer_type | VARCHAR | Administrative layer type | "regioner"
processed_at | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

**Postal Codes (Postnumre)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
code | VARCHAR | Postal code | "1000"
name | VARCHAR | Postal area name | "København K"
geometry | GEOMETRY | Postal area boundary | POLYGON((...))
is_valid_geometry | BOOLEAN | Geometry validity flag | true
area_m2 | DOUBLE | Area in square meters | 5200000.0
centroid_x | DOUBLE | Centroid longitude | 12.5789
centroid_y | DOUBLE | Centroid latitude | 55.6823
geometry_wkt | VARCHAR | Well-Known Text geometry | "POLYGON((...))"
layer_type | VARCHAR | Administrative layer type | "postnumre"
processed_at | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

**Landsdele (Parts of Country)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
code | VARCHAR | NUTS3 code | "DK011"
name | VARCHAR | Landsdel name | "København"
geometry | GEOMETRY | Landsdel boundary | POLYGON((...))
is_valid_geometry | BOOLEAN | Geometry validity flag | true
area_m2 | DOUBLE | Area in square meters | 2561000000.0
centroid_x | DOUBLE | Centroid longitude | 12.3456
centroid_y | DOUBLE | Centroid latitude | 55.7890
geometry_wkt | VARCHAR | Well-Known Text geometry | "POLYGON((...))"
layer_type | VARCHAR | Administrative layer type | "landsdele"
processed_at | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

### Storage Locations
- **Bronze**: `gs://landbruget-data/bronze/dagi_{layer}/{timestamp}/dagi_{layer}.json`
- **Silver**: `gs://landbruget-data/silver/dagi_{layer}/{timestamp}/data.parquet`
- **Layer Examples**:
  - `dagi_kommuner`, `dagi_regioner`
  - `dagi_landsdele`, `dagi_postnumre`

### Processing Infrastructure
- **Platform**: Automated monthly execution as foundation data source
- **Resources**: Moderate memory requirements for GeoJSON and spatial processing
- **Dependencies**: DAWA API service access (no authentication required)
- **Performance**: ~1 hour for complete processing (60 minutes estimated)

### DAWA API Details
- **Service URL**: `https://api.dataforsyningen.dk`
- **Protocol**: REST API with GeoJSON format responses
- **Authentication**: Not required (public API)
- **Endpoints**:
  - `/kommuner?format=geojson` - Municipalities
  - `/regioner?format=geojson` - Regions  
  - `/landsdele?format=geojson` - Landsdele
  - `/postnumre?format=geojson` - Postal codes
- **Coordinate System**: WGS84 (EPSG:4326) native format
- **Timeout**: 120 seconds per request
- **Concurrency**: 5 concurrent requests

### Column Mapping Configuration
```python
column_mapping = {
    "kode": "code",        # Administrative codes
    "navn": "name",        # Danish names  
    "nr": "code",          # Postal code numbers
    "nuts3": "code",       # NUTS3 statistical codes
    "regionskode": "region_code"  # Regional hierarchy
}

required_columns = {
    "kommuner": ["kode", "navn", "regionskode"],
    "regioner": ["kode", "navn"], 
    "landsdele": ["nuts3", "navn"],
    "postnumre": ["nr", "navn"]
}
```

### Spatial Processing Features
- **GeoJSON Processing**: Native DuckDB-spatial GeoJSON processing with `ST_GeomFromGeoJSON`
- **Geometry Validation**: Comprehensive validation using `ST_IsValid` and geometry repair
- **Area Calculations**: Precise area calculations in square meters using `ST_Area`
- **Centroid Calculations**: Administrative center calculations using `ST_Centroid`
- **Coordinate System Handling**: Consistent WGS84 processing with transformation support

### Quality Assurance Features
- **API Response Validation**: Comprehensive validation of GeoJSON structure and feature content
- **Feature Count Verification**: Validation of expected administrative unit counts per layer
- **Geometry Integrity Checks**: Spatial validation and coordinate accuracy verification
- **Attribute Completeness**: Assessment of required administrative attributes for each layer
- **Processing Continuity**: Error recovery allowing partial success when individual layers fail

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Administrative Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "DAGI" label
- **API Service Problems**: Contact system administrators for DAWA API service issues
- **Column Mapping Issues**: Contact data team for administrative attribute questions

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when DAWA API changes or administrative boundaries are updated
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make administrative geographic data accessible and trustworthy.*

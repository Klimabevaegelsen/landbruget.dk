# BNBO (Well Protection Areas) Workflow

> **Monthly Foundation Processing**: Danish well-near protection areas status from Environmental Portal WFS service

---

## What This Workflow Does

The BNBO (Boringsnære Beskyttelsesområder - Well-Near Protection Areas) workflow collects and processes municipal status data for well protection areas from the Danish Environmental Portal WFS service. This workflow provides essential environmental protection data showing the status of pesticide management requirements around Danish wells and water sources.

### Why This Data Matters
- **Environmental Protection**: Critical data for protecting Danish groundwater and drinking water sources
- **Pesticide Regulation**: Status tracking for pesticide use restrictions near wells and water sources
- **Municipal Compliance**: Municipal-level status reporting for well protection area management
- **Agricultural Planning**: Environmental constraints data for agricultural field planning and operations
- **Policy Implementation**: Tracking implementation status of environmental protection measures

### Key Statistics
- **Data Coverage**: Complete national Danish well protection area status data
- **Processing Scale**: Municipal-level well protection status across Denmark
- **Status Categories**: Simplified categorization (Action Required, Completed, Unknown)
- **Integration Role**: Foundation data for field area analysis and environmental constraint modeling
- **Update Frequency**: Monthly updates ensuring current environmental protection status

---

## Data Sources and Collection

### Official Sources
This workflow collects data from the Danish Environmental Portal WFS service:

| Data Layer | Purpose | Coverage | Status Categories |
|------------|---------|----------|-------------------|
| **dai:status_bnbo** | Municipal well protection status | National Denmark | 6 detailed statuses |

### Data Collection Process

#### Environmental Portal WFS Integration
- **Service URL**: `https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs`
- **Protocol**: WFS 2.0.0 with standard HTTP access (no authentication required)
- **Data Layer**: `dai:status_bnbo` (well-near protection area status)
- **Coordinate System**: EPSG:25832 (Danish UTM Zone 32N) native format
- **Batch Processing**: 100 features per request with concurrent processing (3 workers)

#### Advanced Processing Architecture
- **Concurrent Processing**: 3 concurrent workers for efficient data collection
- **Retry Logic**: Exponential backoff retry strategy (5 attempts) for robust WFS communication
- **SSL Configuration**: Custom SSL handling for government service compatibility
- **Progress Monitoring**: Detailed logging of processing progress and feature validation
- **Memory Optimization**: Efficient XML processing and geometry handling

#### Comprehensive Status Collection
- **Detailed Status Tracking**: Six specific municipal status categories for well protection
- **Spatial Boundaries**: Precise geometric boundaries for well protection areas
- **Municipal Information**: Administrative data linking protection areas to municipalities
- **Area Calculations**: Precise area calculations in hectares using DuckDB-spatial
- **Status Simplification**: Mapping of detailed statuses to simplified categories

### Status Categories and Mapping
| Original Status | Simplified Category | Description |
|----------------|---------------------|-------------|
| "Frivillig aftale tilbudt (UDGÅET)" | Action Required | Voluntary agreement offered (expired) |
| "Gennemgået, indsats nødvendig" | Action Required | Reviewed, action necessary |
| "Ikke gennemgået (default værdi)" | Action Required | Not reviewed (default value) |
| "Gennemgået, indsats ikke nødvendig" | Completed | Reviewed, no action necessary |
| "Indsats gennemført" | Completed | Action completed |
| "Ingen erhvervsmæssig anvendelse af pesticider" | Completed | No commercial pesticide use |

### Data Privacy and Compliance
- **Environmental Data**: Public environmental protection information for legitimate analysis
- **Municipal Status**: Administrative status data for environmental compliance monitoring
- **Well Protection**: Spatial boundaries for groundwater protection area analysis
- **Agricultural Integration**: Environmental constraint data for agricultural planning

---

## Data Processing Steps

### 🥉 Bronze Layer: Comprehensive WFS Data Collection
**What happens**: We fetch raw well protection area status data from Environmental Portal WFS service
**Why**: Environmental protection data requires comprehensive collection to ensure complete coverage of well protection areas

**Specific processing**:
- **WFS Service Integration**: Direct connection to Environmental Portal without authentication requirements
- **Concurrent Data Collection**: 3 concurrent workers fetching 100 features per batch for optimal throughput
- **Comprehensive XML Processing**: Complete XML feature collection with proper namespace handling
- **Retry Strategy**: Exponential backoff retry logic (5 attempts) for robust service communication
- **Feature Count Validation**: Pre-flight total count retrieval and validation

**XML processing features**:
- **GML Geometry Parsing**: Direct parsing of GML MultiSurface geometries from XML responses
- **Namespace Detection**: Dynamic namespace extraction from XML documents
- **Feature Extraction**: Complete feature extraction with all attributes and spatial data
- **Error Recovery**: Graceful handling of malformed XML and parsing errors
- **Progress Tracking**: Real-time monitoring of data collection progress

**Quality controls**:
- **Response Validation**: Comprehensive validation of WFS responses and XML structure
- **Feature Count Monitoring**: Tracking of total vs. returned features for completeness
- **Error Handling**: Robust error recovery and processing continuation
- **SSL Compatibility**: Custom SSL configuration for government service access

**Output**: Raw XML responses containing complete well protection area status data

### 🥈 Silver Layer: Advanced Spatial Data Transformation and Status Processing
**What happens**: We transform raw XML data into standardized, analysis-ready spatial datasets with status categorization
**Why**: Raw XML data requires coordinate transformation, status simplification, and spatial validation for analytical use

**Specific transformations**:

#### Advanced XML Processing
- **Dynamic Namespace Handling**: Automatic detection and handling of XML namespaces
- **GML Geometry Conversion**: Direct conversion from GML MultiSurface to WKT format without external libraries
- **Multi-Polygon Support**: Intelligent handling of single and multi-polygon geometries
- **Coordinate Processing**: Proper handling of coordinate pairs with validation

#### Comprehensive Spatial Processing
- **Coordinate Transformation**: Convert from EPSG:25832 (Danish UTM) to EPSG:4326 (WGS84)
- **Geometry Validation**: Comprehensive spatial validation and repair using DuckDB-spatial
- **Area Calculations**: Precise area calculations in hectares using DuckDB ST_Area function
- **WKT Processing**: Direct WKT creation from coordinate data for optimal performance

#### Status Categorization and Simplification
- **Status Mapping**: Conversion of 6 detailed status categories to 3 simplified categories
- **Action Required Classification**: Identification of areas requiring environmental action
- **Completed Status Tracking**: Areas where environmental protection measures are complete
- **Unknown Status Handling**: Proper handling of unrecognized status values

#### Advanced Dissolved Dataset Creation
- **Status-Based Aggregation**: Spatial aggregation by simplified status categories
- **Overlap Resolution**: Sophisticated handling of overlapping protection areas
- **Priority-Based Dissolving**: Action Required areas take priority over Completed areas
- **Spatial Union Operations**: Advanced spatial union using DuckDB `ST_Union_Agg` and `ST_Difference`

**Quality checks**:
- **XML Parsing Validation**: Comprehensive validation of XML structure and content
- **Geometry Integrity**: Validation of all spatial geometries and coordinate transformations
- **Status Consistency**: Validation of status category mapping and completeness
- **Dissolved Geometry Validation**: Verification of spatial union operations and overlap resolution

**Output**: Standardized well protection area datasets (individual and dissolved) with validated geometries and simplified status categories

### 🥇 Gold Layer: Integration with Field Area Analysis
**What happens**: BNBO protection area data is integrated into comprehensive field area analysis workflows
**Why**: Well protection areas are essential environmental constraints for agricultural field analysis and planning

**Integration applications**:
- **Field Area Analysis**: Direct integration into field area analysis for environmental constraint assessment
- **Environmental Filtering**: Pre-filtering of agricultural fields based on well protection area constraints
- **Spatial Constraint Modeling**: Environmental protection areas as spatial constraints for agricultural analysis
- **Compliance Analysis**: Integration with agricultural field data for environmental compliance assessment

**Output**: Environmental constraint data enabling comprehensive field-level agricultural analysis with environmental protection considerations

---

## Workflow Schedule and Execution

### Monthly Foundation Processing
- **Schedule**: 1st of every month at 1 AM UTC (foundation batch, priority 4)
- **Execution Type**: Automated monthly processing as foundation data source
- **Processing Duration**: ~1.5 hours for complete data collection (estimated 90 minutes)
- **Dependencies**: None (independent foundation data source)
- **Downstream Dependencies**: Required by `field_area_analysis` workflow for environmental constraint analysis

### Processing Performance
- **Data Volume**: Municipal-level well protection area data across Denmark
- **Memory Usage**: Moderate memory requirements for XML processing and spatial operations
- **Network**: Sustained WFS communication with Environmental Portal service
- **Storage**: Moderate storage requirements for well protection area spatial data
- **Concurrency**: 3 concurrent workers with 100 features per batch for optimal throughput

### Advanced Features
- **SSL Compatibility**: Custom SSL configuration for government service access
- **Retry Logic**: Exponential backoff retry strategy for robust service communication
- **Dynamic XML Processing**: Flexible XML parsing with automatic namespace detection
- **Dissolved Dataset Creation**: Advanced spatial aggregation with overlap resolution
- **Status Simplification**: Intelligent mapping from detailed to simplified status categories

### Resource Management
- **Moderate Memory Usage**: Efficient XML processing and spatial operations
- **Network Optimization**: Concurrent processing with appropriate batch sizing
- **Error Handling**: Comprehensive error recovery and processing continuation
- **Storage Efficiency**: Optimized storage of both individual and dissolved datasets

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Complete national Danish well protection area status from authoritative environmental source |
| **Accuracy** | Excellent | Danish Environmental Portal official data with rigorous spatial validation |
| **Timeliness** | Good | Monthly updates as municipal status changes and environmental assessments are completed |
| **Spatial Precision** | Good | Municipal-level precision with comprehensive geometry validation |

### Known Issues and Limitations

#### Service and Infrastructure Constraints
- **WFS Service Dependency**: Processing success depends on Environmental Portal WFS service availability
- **SSL Configuration Requirements**: Government service requires custom SSL handling
- **Batch Processing Constraints**: Service limits require careful batch sizing (100 features)
- **XML Processing Complexity**: Complex GML geometry parsing requires robust error handling

#### Data Processing and Technical Limitations
- **Status Simplification**: Detailed status information reduced to 3 simplified categories
- **Overlap Resolution**: Complex spatial overlap handling may affect area calculations
- **Coordinate Transformation**: Minor precision changes during CRS conversion
- **Municipal-Level Granularity**: Data granularity limited to municipal reporting level

#### Integration and Usage Considerations
- **Environmental Constraints**: Protection areas represent constraints, not operational guidance
- **Status Temporal Lag**: Municipal status updates may lag behind actual environmental conditions
- **Spatial Boundary Precision**: Municipal boundaries may not reflect precise well locations
- **Field-Level Application**: Municipal data requires careful interpretation for field-level analysis

### Recommended Uses
✅ **This data is excellent for**:
- Environmental constraint analysis for agricultural field planning
- Regional environmental protection status assessment and monitoring
- Agricultural compliance analysis with environmental protection requirements
- Policy analysis requiring well protection area status and coverage information
- Research requiring Danish environmental protection area spatial data

⚠️ **Use with caution for**:
- Individual well protection analysis - Municipal-level data, not well-specific
- Real-time environmental status - Monthly updates with processing delays
- Precise field-level constraints - Municipal boundaries may not reflect actual protection zones

❌ **Not recommended for**:
- Individual well management - Aggregated municipal data, not well-specific information
- Daily operational decisions - Monthly updates, not real-time environmental status
- International environmental comparisons without context - Danish-specific environmental protection system

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Which areas of Denmark require action for well protection?** - Action Required status areas showing where environmental measures are needed
2. **Where have well protection measures been completed?** - Completed status areas showing successful environmental protection implementation
3. **How do well protection areas affect agricultural field planning?** - Spatial constraints for agricultural operations near wells and water sources
4. **What is the status of environmental protection implementation across Danish municipalities?** - Municipal-level status tracking for policy assessment

### Example Analyses
#### Environmental Constraint Assessment for Agricultural Fields
**Question**: Which agricultural fields are affected by well protection area requirements?
**Data Used**: BNBO dissolved status data spatially intersected with FVM agricultural field boundaries
**Method**: Spatial intersection analysis to identify fields within or adjacent to protection areas
**Output**: Agricultural field constraint mapping showing environmental protection requirements by field
**Limitations**: Municipal-level protection areas may not reflect precise field-level constraints

#### Regional Environmental Protection Status Analysis
**Question**: What is the distribution of well protection implementation status across Danish regions?
**Data Used**: BNBO status data aggregated by administrative regions and status categories
**Method**: Spatial aggregation and statistical analysis of protection area status by region
**Output**: Regional environmental protection status maps and implementation progress statistics
**Limitations**: Municipal reporting may not reflect actual on-ground environmental conditions

#### Agricultural Compliance Risk Assessment
**Question**: Which agricultural operations face the highest environmental compliance requirements?
**Data Used**: BNBO Action Required areas intersected with agricultural field and pesticide application data
**Method**: Risk assessment based on proximity to protection areas requiring action
**Output**: Agricultural compliance risk mapping and prioritization for environmental measures
**Limitations**: Municipal status data requires field-level verification for operational decisions

### Data Access
- **Research Access**: Complete well protection area datasets for academic environmental research
- **Policy Access**: Environmental protection status data for policy development and monitoring
- **Industry Access**: Environmental constraint data for agricultural industry planning and compliance
- **Regulatory Access**: Official environmental protection data for regulatory compliance verification

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Well Protection Area Status Data (Silver Layer)

**Individual Protection Areas**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
geometry | VARCHAR | Protection area boundary (WKT) | "MULTIPOLYGON(((...)))"
area_ha | DOUBLE | Area in hectares | 125.75
status_bnbo | VARCHAR | Original detailed status | "Gennemgået, indsats nødvendig"
status_category | VARCHAR | Simplified status category | "Action Required"
municipality_code | VARCHAR | Municipality identifier | "0101"
municipality_name | VARCHAR | Municipality name | "København"
created_at | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
geometry_spatial | GEOMETRY | DuckDB spatial geometry object | GEOMETRY object
```

**Dissolved Protection Areas**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
status_category | VARCHAR | Simplified status category | "Action Required"
geometry | GEOMETRY | Dissolved boundary geometry | MULTIPOLYGON geometry
dissolved_at | TIMESTAMP | Dissolution timestamp | "2025-01-15T10:30:00"
```

### Storage Locations
- **Bronze**: `gs://landbrugsdata-raw-data/bronze/bnbo_status/{timestamp}/data.parquet`
- **Silver**: `gs://landbrugsdata-raw-data/silver/bnbo_status/{timestamp}/data.parquet`
- **Silver Dissolved**: `gs://landbrugsdata-raw-data/silver/bnbo_status_dissolved/{timestamp}/data.parquet`

### Processing Infrastructure
- **Platform**: Automated monthly execution as foundation data source
- **Resources**: Moderate memory requirements for XML and spatial processing
- **Dependencies**: Environmental Portal WFS service access
- **Performance**: ~1.5 hours for complete processing (90 minutes estimated)

### WFS Service Details
- **Service URL**: `https://arealeditering-dist-geo.miljoeportal.dk/geoserver/wfs`
- **Protocol**: WFS 2.0.0 with standard HTTP access
- **Layer**: `dai:status_bnbo` (well-near protection area status)
- **Coordinate System**: EPSG:25832 (source) → EPSG:4326 (target)
- **Batch Size**: 100 features per request
- **Concurrency**: 3 concurrent workers

### Status Mapping Configuration
```python
status_mapping = {
    "Frivillig aftale tilbudt (UDGÅET)": "Action Required",
    "Gennemgået, indsats nødvendig": "Action Required", 
    "Ikke gennemgået (default værdi)": "Action Required",
    "Gennemgået, indsats ikke nødvendig": "Completed",
    "Indsats gennemført": "Completed",
    "Ingen erhvervsmæssig anvendelse af pesticider": "Completed"
}
```

### XML Processing Features
- **Dynamic Namespace Detection**: Automatic extraction of XML namespaces from documents
- **GML Geometry Parsing**: Direct conversion from GML MultiSurface to WKT format
- **Multi-Polygon Handling**: Intelligent processing of single and multi-polygon geometries
- **Coordinate Validation**: Validation and processing of coordinate pairs

### Spatial Processing Features
- **Coordinate Transformation**: EPSG:25832 → EPSG:4326 using DuckDB-spatial
- **Geometry Validation**: Comprehensive validation and repair of spatial geometries
- **Area Calculations**: Precise hectare calculations using DuckDB ST_Area function
- **Spatial Union Operations**: Advanced dissolved dataset creation using ST_Union_Agg and ST_Difference

### Quality Assurance Features
- **XML Structure Validation**: Comprehensive validation of WFS XML responses
- **Feature Count Verification**: Validation of expected vs. actual feature counts
- **Geometry Integrity Checks**: Spatial validation and coordinate accuracy verification
- **Status Mapping Validation**: Verification of status category mapping completeness

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Environmental Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "BNBO" label
- **WFS Service Problems**: Contact system administrators for Environmental Portal service issues
- **Status Mapping Issues**: Contact environmental data team for status category questions

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when Environmental Portal service changes or status categories are updated
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make environmental protection data accessible and trustworthy.*

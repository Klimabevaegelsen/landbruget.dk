# Water Projects Workflow

> **Monthly Foundation Processing**: Danish water management and environmental projects from multiple WFS and ArcGIS services

---

## What This Workflow Does

The Water Projects workflow collects and processes comprehensive Danish water management and environmental project data from multiple Danish environmental services. This workflow aggregates data from WFS (Web Feature Service) and ArcGIS REST API services to provide a unified dataset of water projects, hydrology initiatives, wetland restoration, climate adaptation projects, and nutrient reduction programs across Denmark.

### Why This Data Matters
- **Environmental Management**: Critical data for tracking water quality improvement and environmental restoration projects
- **Climate Adaptation**: Climate change adaptation projects including low-lying area management and flood prevention
- **Nutrient Reduction**: Phosphorus and nitrogen reduction projects for water quality improvement
- **Wetland Restoration**: Wetland and watercourse restoration initiatives for biodiversity and water management
- **Agricultural Impact**: Environmental projects affecting agricultural land use and field management
- **Policy Implementation**: Tracking implementation of Danish environmental and water management policies

### Key Statistics
- **Data Coverage**: Comprehensive national Danish water and environmental project data (15 distinct layer types)
- **Processing Scale**: Multi-source data integration from WFS and ArcGIS services
- **Project Types**: Hydrology, restoration, nutrient reduction, climate adaptation, and private wetland projects
- **Integration Role**: Foundation data for field area analysis and environmental constraint modeling
- **Update Frequency**: Monthly updates ensuring current environmental project status

---

## Data Sources and Collection

### Official Sources
This workflow collects data from multiple Danish environmental services:

| Service Type | Source | Layers | Project Types |
|--------------|--------|---------|---------------|
| **WFS Services** | FVM Geoserver | 12 layers | Hydrology, restoration, nutrient reduction |
| **WFS Services** | Environmental Portal | 2 layers | Climate adaptation projects |
| **ArcGIS REST API** | NST GIS Server | 1 layer | Public climate low-lying area projects |

### Comprehensive Layer Collection

#### Natura 2000 Projects (N2000_projekter)
- **Hydrologi_E**: Established hydrology projects under Natura 2000 program
- **Hydrologi_F**: Completed hydrology projects under Natura 2000 program

#### Other Projects (Ovrige_projekter)
- **Vandloebsrestaurering_E**: Established watercourse restoration projects
- **Vandloebsrestaurering_F**: Completed watercourse restoration projects

#### Water Projects (Vandprojekter)
- **Fosfor_E_samlet**: Established phosphorus reduction projects (aggregated)
- **Fosfor_F_samlet**: Completed phosphorus reduction projects (aggregated)
- **Kvaelstof_E_samlet**: Established nitrogen reduction projects (aggregated)
- **Kvaelstof_F_samlet**: Completed nitrogen reduction projects (aggregated)
- **Lavbund_E_samlet**: Established low-lying area projects (aggregated)
- **Lavbund_F_samlet**: Completed low-lying area projects (aggregated)
- **Private_vaadomraader**: Private wetland area projects
- **Restaurering_af_aadale_2024**: Valley restoration projects (2024)

#### Climate Adaptation Projects (vandprojekter)
- **kla_projektforslag**: Climate adaptation project proposals
- **kla_projektomraader**: Climate adaptation project areas

#### Public Climate Projects (ArcGIS)
- **Klima_lavbund_demarkation___offentlige_projekter**: Public climate low-lying area delineation projects

### Data Collection Process

#### Multi-Service Integration
- **WFS Services**: 
  - FVM Geoserver: `https://geodata.fvm.dk/geoserver/wfs`
  - Environmental Portal: `https://wfs2-miljoegis.mim.dk/vandprojekter/wfs`
- **ArcGIS REST API**: NST GIS Server: `https://gis.nst.dk/server/rest/services/`
- **Protocol**: WFS 2.0.0 for WFS services, REST API for ArcGIS services
- **Coordinate System**: EPSG:25832 (Danish UTM Zone 32N) for all services

#### Advanced Processing Architecture
- **Multi-Service Processing**: Intelligent handling of different service types (WFS vs ArcGIS)
- **Concurrent Processing**: 3 concurrent workers for efficient data collection across services
- **Batch Processing**: 100 features per WFS request with parallel chunk processing
- **Retry Logic**: Exponential backoff retry strategy (5 attempts) for robust service communication
- **Unicode Handling**: Comprehensive Unicode decoding error handling for various data encodings

#### Comprehensive Project Data Collection
- **Project Metadata**: Project names, contact information, budgets, and administrative details
- **Temporal Information**: Start dates, end dates, project years, and status tracking
- **Spatial Boundaries**: Precise geometric boundaries for all project areas
- **Project Classification**: Project types, status categories, and implementation phases
- **Area Calculations**: Precise area calculations in hectares using DuckDB-spatial

### Data Privacy and Compliance
- **Public Environmental Data**: Environmental project information for legitimate analysis and transparency
- **Multi-Agency Access**: Authorized access to multiple Danish environmental agency data sources
- **Project Transparency**: Public environmental project data for policy analysis and monitoring
- **Agricultural Integration**: Environmental project data for agricultural planning and compliance analysis

---

## Data Processing Steps

### 🥉 Bronze Layer: Multi-Service Data Collection
**What happens**: We fetch raw environmental project data from multiple WFS and ArcGIS services across 15 distinct layer types
**Why**: Environmental projects require comprehensive collection from multiple agencies to ensure complete coverage of Danish environmental initiatives

**Specific processing**:
- **Multi-Service Integration**: Seamless integration of WFS and ArcGIS REST API services
- **Service-Specific Processing**: Intelligent routing to appropriate fetch methods based on service type
- **Concurrent Multi-Layer Collection**: 3 concurrent workers processing 15 different project layer types
- **Comprehensive WFS Processing**: Full WFS 2.0.0 integration with pagination and feature count validation
- **ArcGIS REST Integration**: Complete ArcGIS FeatureServer integration with JSON response processing

**Advanced WFS processing**:
- **Parallel Chunk Processing**: Concurrent fetching of WFS data chunks with automatic pagination
- **Feature Count Optimization**: Pre-flight feature count retrieval for efficient processing planning
- **XML Processing**: Comprehensive XML parsing with namespace handling and error recovery
- **Unicode Error Handling**: Robust Unicode decoding with fallback strategies for various encodings

**ArcGIS REST processing**:
- **Layer ID Extraction**: Intelligent parsing of layer identifiers from complex layer names
- **JSON Response Handling**: Complete JSON feature collection with geometry and attribute processing
- **Geometry Precision**: High-precision geometry retrieval with configurable precision settings
- **Timestamp Processing**: Proper handling of ArcGIS timestamp formats and conversions

**Quality controls**:
- **Service Response Validation**: Comprehensive validation of both WFS XML and ArcGIS JSON responses
- **Feature Count Monitoring**: Tracking of expected vs. actual feature counts for completeness
- **Error Recovery**: Graceful handling of service failures allowing processing to continue
- **Data Completeness**: Verification of essential project attributes across all service types

**Output**: Raw XML and JSON responses containing complete environmental project data from all services

### 🥈 Silver Layer: Advanced Multi-Format Data Transformation
**What happens**: We transform raw XML and JSON data into standardized, analysis-ready spatial datasets with comprehensive processing
**Why**: Raw multi-service data requires format-specific processing, standardization, and spatial validation for analytical use

**Specific transformations**:

#### Advanced Multi-Format Processing
- **XML Processing**: Sophisticated GML geometry parsing with namespace detection and feature extraction
- **JSON Processing**: ArcGIS JSON feature processing with ring-based polygon geometry handling
- **Service Type Detection**: Intelligent processing routing based on service type configuration
- **Format Standardization**: Unified output schema regardless of input format (XML vs JSON)

#### Comprehensive Spatial Processing
- **GML Geometry Conversion**: Direct conversion from GML MultiSurface to WKT format without external libraries
- **ArcGIS Geometry Processing**: Ring-based polygon processing with exterior and interior ring handling
- **Coordinate System Processing**: Consistent EPSG:25832 coordinate processing across all services
- **Area Calculations**: Precise area calculations in hectares using DuckDB `ST_Area` function
- **Geometry Validation**: Comprehensive WKT validation with parentheses balancing and format checking

#### Advanced Project Data Processing
- **Attribute Standardization**: Intelligent attribute mapping and type conversion across service types
- **Temporal Data Processing**: Comprehensive date and timestamp processing using DuckDB date functions
- **Numeric Data Conversion**: Robust numeric conversion for areas, budgets, and year values
- **Project Classification**: Standardized project status and type classification across services
- **Metadata Enrichment**: Addition of layer source information and processing timestamps

#### Sophisticated Dissolved Dataset Creation
- **Spatial Union Operations**: Advanced spatial aggregation using DuckDB `ST_Union_Agg` for overlapping projects
- **Geometry Validation**: Pre-dissolution geometry validation and repair using `ST_IsValid`
- **Performance Optimization**: Batch processing of geometry conversions with graceful error handling
- **Success Rate Tracking**: Detailed monitoring of geometry conversion success rates and failure analysis

**Quality checks**:
- **Multi-Format Validation**: Format-specific validation for both XML and JSON data sources
- **Geometry Integrity**: Comprehensive spatial geometry validation and coordinate accuracy verification
- **Attribute Completeness**: Assessment of project attribute completeness across all service types
- **Processing Statistics**: Detailed tracking of successful vs. failed geometry conversions

**Output**: Standardized environmental project datasets (individual and dissolved) with validated geometries and consistent attributes

### 🥇 Gold Layer: Integration with Field Area Analysis
**What happens**: Water project data is integrated into comprehensive field area analysis workflows
**Why**: Environmental projects represent spatial constraints and opportunities for agricultural field analysis

**Integration applications**:
- **Field Area Analysis**: Direct integration into field area analysis for environmental project assessment
- **Environmental Constraints**: Water projects as spatial constraints for agricultural operations
- **Project Impact Analysis**: Assessment of environmental project impacts on agricultural land use
- **Spatial Filtering**: Pre-filtering of agricultural fields based on environmental project proximity

**Output**: Environmental project data enabling comprehensive field-level agricultural analysis with environmental project considerations

---

## Workflow Schedule and Execution

### Monthly Foundation Processing
- **Schedule**: 1st of every month at 1 AM UTC (foundation batch, priority 6)
- **Execution Type**: Automated monthly processing as foundation data source
- **Processing Duration**: ~1.25 hours for complete data collection (estimated 75 minutes)
- **Dependencies**: None (independent foundation data source)
- **Downstream Dependencies**: Required by `field_area_analysis` workflow for environmental project analysis

### Processing Performance
- **Data Volume**: Multi-service environmental project data across 15 layer types
- **Memory Usage**: Moderate to high memory requirements for multi-format processing and spatial operations
- **Network**: Sustained communication with multiple environmental services (WFS and ArcGIS)
- **Storage**: Moderate storage requirements for environmental project spatial data
- **Concurrency**: 3 concurrent workers with service-appropriate batch sizing

### Advanced Features
- **Multi-Service Integration**: Seamless integration of WFS and ArcGIS REST API services
- **Service Type Detection**: Intelligent processing routing based on configured service types
- **Unicode Error Handling**: Robust Unicode decoding with fallback strategies
- **Batch Geometry Processing**: Efficient batch processing of geometry conversions with error recovery
- **Dissolved Dataset Creation**: Advanced spatial aggregation with comprehensive validation

### Resource Management
- **Moderate to High Memory Usage**: Efficient multi-format processing and spatial operations
- **Multi-Service Optimization**: Concurrent processing optimized for different service types
- **Error Handling**: Comprehensive error recovery allowing partial success across services
- **Storage Efficiency**: Optimized storage of both individual and dissolved project datasets

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Comprehensive Danish environmental project data from multiple authoritative sources |
| **Accuracy** | Excellent | Multiple Danish environmental agencies' official data with rigorous spatial validation |
| **Timeliness** | Good | Monthly updates as new environmental projects are initiated and completed |
| **Spatial Precision** | Excellent | High-precision project boundaries with comprehensive geometry validation |

### Known Issues and Limitations

#### Service and Infrastructure Constraints
- **Multi-Service Dependency**: Processing success depends on availability of multiple environmental services
- **Service Type Complexity**: Different service types (WFS vs ArcGIS) require specialized processing approaches
- **Network Requirements**: Sustained communication with multiple services for comprehensive data collection
- **Processing Duration**: Complete processing requires ~1.25 hours for all project types and services

#### Data Processing and Technical Limitations
- **Format Complexity**: Multi-format processing (XML vs JSON) adds complexity to data transformation
- **Geometry Conversion Challenges**: Complex geometry formats may result in some conversion failures
- **Service-Specific Attributes**: Different services provide different attribute sets requiring intelligent mapping
- **Unicode Encoding Issues**: Various services may have different encoding standards requiring error handling

#### Integration and Usage Considerations
- **Project Temporal Overlap**: Environmental projects may have overlapping temporal and spatial coverage
- **Service Availability Variation**: Different services may have varying availability and performance characteristics
- **Attribute Standardization Complexity**: Standardizing attributes across multiple agencies requires careful mapping
- **Dissolved Dataset Complexity**: Spatial union operations can be computationally intensive for large datasets

### Recommended Uses
✅ **This data is excellent for**:
- Environmental project analysis and monitoring across Denmark
- Agricultural field analysis requiring environmental project context
- Policy analysis of Danish environmental and water management initiatives
- Spatial analysis of environmental project coverage and distribution
- Research requiring comprehensive Danish environmental project information

⚠️ **Use with caution for**:
- Real-time project status - Monthly updates with processing delays
- Individual project management - Aggregated data, not project-specific operational information
- Cross-border analysis - Data specific to Danish environmental programs and classifications

❌ **Not recommended for**:
- Daily operational project management - Monthly updates, not real-time project status
- Individual project financial analysis - Aggregated data, not detailed financial information
- International environmental project comparisons without context - Danish-specific environmental programs

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Where are Danish environmental and water management projects located?** - Complete spatial coverage of environmental projects across Denmark
2. **What types of environmental projects affect agricultural areas?** - Project type analysis and spatial relationships with agricultural land
3. **How do environmental projects impact agricultural field planning?** - Spatial constraints and opportunities for agricultural operations
4. **What is the status and coverage of Danish water quality improvement projects?** - Project status tracking and spatial distribution analysis

### Example Analyses
#### Environmental Project Impact on Agricultural Fields
**Question**: Which agricultural fields are affected by environmental and water management projects?
**Data Used**: Water projects dissolved data spatially intersected with FVM agricultural field boundaries
**Method**: Spatial intersection analysis to identify fields within or adjacent to environmental project areas
**Output**: Agricultural field impact mapping showing environmental project influences by field and project type
**Limitations**: Project boundaries may not reflect precise field-level impacts, temporal project phases not captured

#### Regional Environmental Project Distribution Analysis
**Question**: How are environmental projects distributed across Danish regions and what types are most common?
**Data Used**: Water projects data aggregated by administrative regions and project types
**Method**: Spatial aggregation and statistical analysis of project distribution by region and type
**Output**: Regional environmental project coverage maps and type distribution statistics
**Limitations**: Project boundaries may span multiple administrative regions, project completion status may vary

#### Climate Adaptation Project Coverage Assessment
**Question**: What is the coverage and focus of Danish climate adaptation projects?
**Data Used**: Climate adaptation project layers (kla_projektforslag, kla_projektomraader, climate low-lying area projects)
**Method**: Spatial analysis of climate adaptation project coverage with focus area identification
**Output**: Climate adaptation project coverage maps and priority area analysis
**Limitations**: Project proposals may not reflect actual implementation, temporal project phases not synchronized

### Data Access
- **Research Access**: Complete environmental project datasets for academic and scientific environmental research
- **Policy Access**: Environmental project data for policy development and implementation monitoring
- **Industry Access**: Environmental project data for agricultural and environmental industry planning
- **Public Access**: Environmental project information for public transparency and civic engagement

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Environmental Project Data (Silver Layer)

**Individual Environmental Projects**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
layer_name | VARCHAR | Source layer identifier | "Vandprojekter:Fosfor_E_samlet"
geometry | TEXT | Project boundary (WKT) | "MULTIPOLYGON(((...)))"
area_ha | DOUBLE | Project area in hectares | 245.75
projektnavn | VARCHAR | Project name | "Fosfor reduktion Vejle Å"
enhedskontakt | VARCHAR | Contact unit/person | "Vejle Kommune"
startdato | DATE | Project start date | "2023-01-15"
slutdato | DATE | Project end date | "2025-12-31"
startaar | INTEGER | Start year | 2023
slutaar | INTEGER | End year | 2025
tilsagnsaa | INTEGER | Grant year | 2023
status | VARCHAR | Project status | "Etableret"
budget | DOUBLE | Project budget | 1500000.0
object_id | INTEGER | Object identifier | 12345
global_id | VARCHAR | Global unique identifier | "{ABC-123-DEF}"
geometry_spatial | GEOMETRY | DuckDB spatial geometry object | GEOMETRY object
```

**Dissolved Environmental Projects**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
project_id | VARCHAR | Dissolved project identifier | "water_project_dissolved"
geometry | GEOMETRY | Dissolved boundary geometry | MULTIPOLYGON geometry
feature_count | INTEGER | Number of merged projects | 156
dissolved_at | TIMESTAMP | Dissolution timestamp | "2025-01-15T10:30:00"
```

### Storage Locations
- **Bronze**: `gs://landbruget-data/bronze/water_projects/{timestamp}/data.parquet`
- **Silver**: `gs://landbruget-data/silver/water_projects/{timestamp}/data.parquet`
- **Silver Dissolved**: `gs://landbruget-data/silver/water_projects_dissolved/{timestamp}/data.parquet`

### Processing Infrastructure
- **Platform**: Automated monthly execution as foundation data source
- **Resources**: Moderate to high memory requirements for multi-format and spatial processing
- **Dependencies**: Multiple environmental service access (WFS and ArcGIS)
- **Performance**: ~1.25 hours for complete processing (75 minutes estimated)

### Service Details

#### WFS Services
- **FVM Geoserver**: `https://geodata.fvm.dk/geoserver/wfs`
- **Environmental Portal**: `https://wfs2-miljoegis.mim.dk/vandprojekter/wfs`
- **Protocol**: WFS 2.0.0 with GetFeature requests
- **Coordinate System**: EPSG:25832 (Danish UTM Zone 32N)
- **Batch Size**: 100 features per request
- **Timeout**: 300 seconds per request

#### ArcGIS REST API Service
- **NST GIS Server**: `https://gis.nst.dk/server/rest/services/`
- **Protocol**: REST API with JSON responses
- **Query Parameters**: `f=json`, `where=1=1`, `outFields=*`, `returnGeometry=true`
- **Coordinate System**: EPSG:25832 output spatial reference
- **Geometry Precision**: 6 decimal places

### Layer Configuration
```python
layers = [
    # Natura 2000 Projects
    "N2000_projekter:Hydrologi_E",
    "N2000_projekter:Hydrologi_F",
    
    # Other Projects  
    "Ovrige_projekter:Vandloebsrestaurering_E",
    "Ovrige_projekter:Vandloebsrestaurering_F",
    
    # Water Projects
    "Vandprojekter:Fosfor_E_samlet",
    "Vandprojekter:Fosfor_F_samlet", 
    "Vandprojekter:Kvaelstof_E_samlet",
    "Vandprojekter:Kvaelstof_F_samlet",
    "Vandprojekter:Lavbund_E_samlet",
    "Vandprojekter:Lavbund_F_samlet",
    "Vandprojekter:Private_vaadomraader",
    "Vandprojekter:Restaurering_af_aadale_2024",
    
    # Climate Adaptation Projects
    "vandprojekter:kla_projektforslag", 
    "vandprojekter:kla_projektomraader",
    
    # Public Climate Projects (ArcGIS)
    "Klima_lavbund_demarkation___offentlige_projekter:0"
]

service_types = {
    "Klima_lavbund_demarkation___offentlige_projekter:0": "arcgis"
}
```

### Spatial Processing Features
- **Multi-Format Geometry Processing**: GML MultiSurface and ArcGIS ring-based polygon processing
- **Coordinate System Handling**: Consistent EPSG:25832 processing across all services
- **Area Calculations**: Precise hectare calculations using DuckDB `ST_Area` function
- **Geometry Validation**: Comprehensive WKT validation and spatial geometry verification
- **Spatial Union Operations**: Advanced dissolved dataset creation using `ST_Union_Agg`

### Quality Assurance Features
- **Multi-Service Validation**: Service-specific validation for WFS XML and ArcGIS JSON responses
- **Geometry Conversion Monitoring**: Detailed tracking of geometry conversion success rates
- **Unicode Error Handling**: Robust Unicode decoding with fallback strategies
- **Batch Processing Statistics**: Performance monitoring for batch geometry processing
- **Service Availability Monitoring**: Error recovery allowing partial success across services

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Environmental Projects Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Water Projects" label
- **Service Problems**: Contact system administrators for environmental service issues
- **Multi-Service Integration Issues**: Contact data team for service-specific problems

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when environmental services change or new project types are added
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make environmental project data accessible and trustworthy.*

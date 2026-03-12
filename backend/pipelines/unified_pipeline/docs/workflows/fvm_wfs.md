# FVM WFS (Agricultural Field Data) Workflow

> **Monthly Foundation Processing**: Comprehensive Danish agricultural field data from FVM WFS service

---

## What This Workflow Does

The FVM WFS (Danish Agricultural Agency Web Feature Service) workflow collects and processes comprehensive agricultural field data from the Danish Agricultural Agency's official WFS service. This workflow fetches seven different types of agricultural spatial data across multiple years, providing the foundational field boundary and usage data essential for all agricultural analysis in Denmark.

### Why This Data Matters
- **Field Boundaries**: Official field block boundaries (Markblokke) defining agricultural land parcels across Denmark
- **Agricultural Usage**: Field marker data (Marker) showing crop types, organic status, and land use patterns
- **Environmental Programs**: Organic areas, subsidies, and environmental compliance data
- **Spatial Analysis Foundation**: Core spatial data enabling field-level agricultural analysis and modeling
- **Regulatory Compliance**: Official data for agricultural subsidy verification and compliance monitoring

### Key Statistics
- **Data Coverage**: Complete national Danish agricultural field data (2005-2026)
- **Layer Types**: 7 distinct agricultural data layers with varying temporal coverage
- **Processing Scale**: 400K+ features per layer, ~1GB+ of spatial data
- **Matrix Processing**: Dedicated matrix workflow for parallel processing of layer-year combinations
- **Integration**: Foundation data for field production estimates, spatial analysis, and agricultural modeling

---

## Data Sources and Collection

### Official Sources
This workflow collects data from the Danish Agricultural Agency WFS service:

| Layer Type | Purpose | Years Available | Feature Count |
|------------|---------|-----------------|---------------|
| **Markblokke** | Field block boundaries | 2005-2026 (22 years) | ~400K per year |
| **Marker** | Field usage/crop data | 2008-2025 (18 years) | ~400K per year |
| **Smaabiotoper** | Small biotopes | 2023-2025 (3 years) | Variable |
| **Organic Areas** | Organic farming areas | 2012-2024 (13 years) | ~50K per year |
| **Organic Subsidies** | Organic area subsidies | 2019-2024 (6 years) | ~30K per year |
| **Grassland Subsidies** | Grassland management subsidies | 2019-2024 (6 years) | ~20K per year |
| **Environmental Subsidies** | Environmental subsidies | 2019-2023 (5 years) | ~15K per year |

### Data Collection Process

#### WFS Service Integration
- **Service URL**: `https://geodata.fvm.dk/geoserver/wfs` (official Danish Agricultural Agency)
- **Protocol**: WFS 2.0.0 with GetFeature requests for complete dataset downloads
- **Format**: GeoJSON with full spatial geometries and attribute data
- **Coordinate System**: EPSG:25832 (Danish UTM Zone 32N) native format
- **Performance**: Optimized for full dataset downloads (~1 minute per layer, 6K+ features/sec)

#### Advanced Matrix Processing Architecture
- **Dedicated Matrix Workflow**: Separate GitHub Actions workflow for parallel processing
- **Layer-Year Matrix**: Individual processing jobs for each layer-year combination
- **Parallel Execution**: Multiple concurrent matrix jobs for efficient processing
- **Resource Management**: Memory-intensive processing with dedicated resource allocation
- **Scalable Architecture**: Handles 67+ layer-year combinations efficiently

#### Comprehensive Temporal Coverage
- **Historical Data**: Complete field boundary history from 2005
- **Usage Evolution**: Agricultural land use changes from 2008
- **Subsidy Programs**: Environmental and organic farming program data from 2012/2019
- **Future Planning**: Forward-looking data through 2026 for planning purposes

### Data Privacy and Compliance
- **Agricultural Data**: Field-level agricultural information for legitimate agricultural analysis
- **Spatial Boundaries**: Official field boundaries for regulatory and analytical purposes
- **Subsidy Information**: Agricultural subsidy data for compliance and policy analysis
- **CVR Integration**: Company identification for agricultural business analysis

---

## Data Processing Steps

### 🥉 Bronze Layer: Comprehensive WFS Data Collection
**What happens**: We fetch raw agricultural spatial data from FVM WFS service across all available layers and years
**Why**: Official agricultural data requires comprehensive collection to ensure complete spatial and temporal coverage

**Specific processing**:
- **Multi-Layer Collection**: Sequential processing of 7 different agricultural data layers
- **Full Dataset Downloads**: Optimized for complete dataset retrieval (no chunking/pagination)
- **Year-by-Year Processing**: Individual collection for each available year per layer type
- **Feature Count Validation**: Pre-flight checks to verify data availability
- **Retry Logic**: Exponential backoff retry strategy for robust WFS communication

**Layer-specific processing**:
- **Markblokke**: Field block boundaries with geometric and administrative attributes
- **Marker**: Field usage data with crop codes, organic status, and management information
- **Smaabiotoper**: Small biotope areas with ecological classification
- **Organic Areas**: Certified organic farming area boundaries
- **Subsidies**: Various agricultural subsidy program boundaries and classifications

**Quality controls**:
- **WFS Response Validation**: Verify GeoJSON structure and feature presence
- **Feature Count Monitoring**: Track feature counts for each layer-year combination
- **Temporal Completeness**: Ensure coverage across all available years
- **Error Recovery**: Graceful handling of WFS service issues and timeouts

**Output**: Raw GeoJSON agricultural data organized by layer type and year

### 🥈 Silver Layer: Advanced Spatial Data Transformation
**What happens**: We transform raw WFS data into standardized, analysis-ready spatial datasets with comprehensive processing
**Why**: Raw WFS data requires coordinate transformation, attribute standardization, and spatial validation for analytical use

**Specific transformations**:

#### Spatial Processing
- **Coordinate Transformation**: Convert from EPSG:25832 (Danish UTM) to EPSG:4326 (WGS84)
- **Geometry Validation**: Comprehensive spatial validation and repair using DuckDB-spatial
- **Area Calculations**: Precise area calculations in hectares for all field polygons
- **Spatial Quality Checks**: Validation of geometry integrity and coordinate accuracy

#### Attribute Standardization
- **Column Mapping**: Standardized column names across all layer types and years
- **Data Type Conversion**: Proper typing for numeric, date, and categorical fields
- **Code Standardization**: Consistent crop codes, status codes, and classification systems
- **Null Handling**: Appropriate handling of missing values and empty fields

#### Field UUID Generation
- **Deterministic UUIDs**: UUID5 generation based on field geometry for consistent identification
- **Cross-Year Linking**: Enables tracking of field changes across years
- **Namespace Management**: Consistent UUID namespace for field identification
- **Spatial Deduplication**: Identifies and handles duplicate or overlapping fields

#### CVR Collection Integration
- **Company Extraction**: Automatic extraction of CVR numbers from field data
- **CVR Validation**: Validation and standardization of company registration numbers
- **Agricultural Business Mapping**: Links fields to agricultural businesses for analysis

**Quality checks**:
- **Geometry Integrity**: Validation of all spatial geometries and coordinate transformations
- **Attribute Completeness**: Assessment of data completeness across all fields
- **Temporal Consistency**: Validation of year-over-year data consistency
- **UUID Uniqueness**: Verification of field UUID generation and uniqueness

**Output**: Standardized agricultural spatial datasets with validated geometries and consistent attributes

### 🥇 Gold Layer: Integration with Agricultural Analysis
**What happens**: FVM field data is integrated into comprehensive agricultural analysis workflows
**Why**: Field boundaries and usage data are essential for all agricultural modeling and analysis

**Integration applications**:
- **Field Production Estimates**: Field boundaries combined with crop data for production modeling
- **Spatial Analysis**: Foundation for proximity analysis, environmental assessment, and land use studies
- **Pesticide Disaggregation**: Field boundaries for disaggregating company-level pesticide data
- **Compliance Monitoring**: Field data for agricultural subsidy and environmental compliance analysis

**Output**: Foundation spatial data enabling comprehensive field-level agricultural analysis

---

## Workflow Schedule and Execution

### Monthly Foundation Processing with Matrix Execution
- **Schedule**: 1st of every month at 1 AM UTC (foundation batch)
- **Execution Type**: Automated monthly with dedicated matrix workflow trigger
- **Processing Duration**: ~4 hours for complete data collection (estimated 240 minutes)
- **Dependencies**: None (independent foundation data source)
- **Matrix Trigger**: Automatically triggers dedicated FVM WFS matrix workflow for parallel processing

### Matrix Processing Architecture
- **Dedicated Workflow**: `fvm_wfs_matrix_download.yml` for parallel layer-year processing
- **Matrix Dimensions**: Layer type × Year combinations (67+ total combinations)
- **Parallel Execution**: Multiple concurrent jobs for efficient processing
- **Resource Optimization**: Memory-intensive processing with dedicated resource allocation
- **Scalable Design**: Handles addition of new years and layer types efficiently

### Processing Performance
- **Data Volume**: 400K+ features per major layer, multi-GB spatial datasets
- **Memory Usage**: High (~16GB) for large spatial datasets and geometry processing
- **Network**: Sustained WFS communication with Danish Agricultural Agency
- **Storage**: Multi-GB storage requirements for complete agricultural spatial data
- **Concurrency**: Limited concurrent requests (2) to respect WFS service capacity

### Advanced Features
- **CLI Filtering**: Support for processing specific layer types and years via CLI parameters
- **Full Dataset Optimization**: Optimized for complete dataset downloads without chunking
- **SSL Configuration**: Custom SSL handling for robust WFS service communication
- **Resource Monitoring**: Built-in memory and disk usage monitoring for large datasets

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Complete official Danish agricultural field data from authoritative source |
| **Accuracy** | Excellent | Danish Agricultural Agency official data with rigorous spatial validation |
| **Timeliness** | Good | Monthly updates as new agricultural data becomes available |
| **Spatial Precision** | Excellent | High-precision field boundaries with comprehensive geometry validation |

### Known Issues and Limitations

#### Service and Infrastructure Constraints
- **WFS Service Dependency**: Processing success depends on Danish Agricultural Agency WFS availability
- **Large Dataset Processing**: Memory-intensive processing requiring substantial computational resources
- **Network Requirements**: Sustained high-bandwidth communication for large spatial datasets
- **Processing Duration**: Complete processing requires several hours for all layer-year combinations

#### Data Coverage and Temporal Limitations
- **Layer-Specific Years**: Different layers have different temporal coverage (2005-2026 range)
- **Historical Gaps**: Some layers have limited historical coverage (e.g., subsidies from 2019)
- **Future Data**: Forward-looking data (2025-2026) may be provisional or planning-based
- **Layer Dependencies**: Some analysis requires multiple layers with overlapping temporal coverage

#### Processing and Integration Challenges
- **Matrix Complexity**: 67+ layer-year combinations require sophisticated orchestration
- **Resource Requirements**: High memory and storage requirements for complete processing
- **Coordinate Transformation**: Minor precision changes during CRS conversion
- **Integration Complexity**: Complex data structure requires careful integration with downstream workflows

### Recommended Uses
✅ **This data is excellent for**:
- Field-level agricultural analysis requiring official Danish field boundaries
- Spatial analysis of agricultural land use patterns and changes over time
- Agricultural subsidy and compliance analysis with official field data
- Environmental impact assessment using official agricultural spatial data
- Research requiring authoritative Danish agricultural field information

⚠️ **Use with caution for**:
- Real-time agricultural monitoring - Monthly updates with processing delays
- Historical analysis before 2005 - Limited temporal coverage for some layers
- Cross-border analysis - Data specific to Danish agricultural system and classifications

❌ **Not recommended for**:
- Individual field management - Aggregated data, not field-specific operational information
- Daily operational decisions - Monthly updates, not real-time operational data
- International comparisons without context - Danish-specific agricultural classifications and systems

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Where are the boundaries of all Danish agricultural fields?** - Markblokke data providing official field block boundaries
2. **How has agricultural land use changed over time in Denmark?** - Marker data analysis showing crop rotation and land use evolution
3. **Which areas are certified organic and receive organic subsidies?** - Organic areas and subsidies data for organic farming analysis
4. **Where are environmental subsidy programs being implemented?** - Environmental and grassland subsidies showing conservation program locations

### Example Analyses
#### Agricultural Land Use Change Analysis
**Question**: How have crop patterns changed across Danish agricultural regions from 2008-2025?
**Data Used**: Marker field usage data across all available years with crop classifications
**Method**: Temporal analysis of crop code changes by region with spatial visualization
**Output**: Land use change maps showing agricultural transition patterns and regional trends
**Limitations**: Crop classification changes over time may affect long-term comparisons

#### Field-Level Production Estimation
**Question**: What is the total agricultural production potential of Danish fields?
**Data Used**: Markblokke field boundaries combined with Marker crop data and DST yield statistics
**Method**: Spatial join of field boundaries with crop data and application of official yield statistics
**Output**: Field-level production estimates aggregated by region, crop type, and farm size
**Limitations**: Yield estimates based on statistical averages, not field-specific conditions

#### Environmental Program Effectiveness
**Question**: Where are environmental subsidies most concentrated and what is their spatial coverage?
**Data Used**: Environmental, grassland, and organic subsidies data with spatial analysis
**Method**: Spatial analysis of subsidy program distribution and coverage relative to total agricultural area
**Output**: Environmental program coverage maps and effectiveness analysis by region
**Limitations**: Subsidy data availability limited to 2019-2024 for most programs

### Data Access
- **Research Access**: Complete spatial datasets for academic and scientific agricultural research
- **Policy Access**: Official field data for agricultural policy development and analysis
- **Industry Access**: Field boundary and usage data for agricultural industry analysis
- **Regulatory Access**: Official data for agricultural compliance and subsidy verification

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Agricultural Field Data (Silver Layer)

**Markblokke (Field Boundaries)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
field_uuid | VARCHAR | Deterministic field identifier | "123e4567-e89b-12d3-a456-426614174000"
geometry | GEOMETRY | Field boundary polygon | POLYGON((...)
area_ha | DOUBLE | Field area in hectares | 12.45
block_id | VARCHAR | Official block identifier | "12345"
field_id | VARCHAR | Official field identifier | "67890"
cvr_number | VARCHAR | Company registration number | "12345678"
municipality_code | VARCHAR | Municipality code | "101"
year | INTEGER | Data year | 2024
processing_time | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

**Marker (Field Usage)**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
field_uuid | VARCHAR | Deterministic field identifier | "123e4567-e89b-12d3-a456-426614174000"
geometry | GEOMETRY | Field usage polygon | POLYGON((...)
area_ha | DOUBLE | Field area in hectares | 12.45
crop_code | VARCHAR | Crop classification code | "110"
crop_name | VARCHAR | Crop name in Danish | "Vinterhvede"
organic_status | VARCHAR | Organic certification status | "Økologisk"
cvr_number | VARCHAR | Company registration number | "12345678"
block_id | VARCHAR | Block identifier | "12345"
field_id | VARCHAR | Field identifier | "67890"
year | INTEGER | Data year | 2024
processing_time | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

**Subsidy Data Layers**
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
field_uuid | VARCHAR | Deterministic field identifier | "123e4567-e89b-12d3-a456-426614174000"
geometry | GEOMETRY | Subsidy area polygon | POLYGON((...)
area_ha | DOUBLE | Subsidy area in hectares | 8.75
subsidy_type | VARCHAR | Type of subsidy program | "Organic Area Subsidy"
program_code | VARCHAR | Program classification code | "ORG_2024"
cvr_number | VARCHAR | Company registration number | "12345678"
year | INTEGER | Subsidy year | 2024
amount | DOUBLE | Subsidy amount (if available) | 15000.0
processing_time | TIMESTAMP | Processing timestamp | "2025-01-15T10:30:00"
```

### Storage Locations
- **Bronze**: `gs://landbruget-data/bronze/{dataset}_{year}/{timestamp}/data.parquet`
- **Silver**: `gs://landbruget-data/silver/{dataset}_{year}/{timestamp}/data.parquet`
- **Layer Examples**:
  - `fvm_markblokke_2024`, `fvm_marker_2024`
  - `fvm_organic_areas_2024`, `fvm_organic_subsidies_2024`

### Processing Infrastructure
- **Platform**: Automated monthly execution with dedicated matrix workflow
- **Resources**: 16GB RAM, high-memory instances for large spatial datasets
- **Dependencies**: Danish Agricultural Agency WFS service access
- **Performance**: ~4 hours for complete processing (240 minutes estimated)
- **Matrix Architecture**: Parallel processing of 67+ layer-year combinations

### WFS Service Details
- **Service URL**: `https://geodata.fvm.dk/geoserver/wfs`
- **Protocol**: WFS 2.0.0 with GetFeature requests
- **Output Format**: GeoJSON (application/json)
- **Coordinate System**: EPSG:25832 (source) → EPSG:4326 (target)
- **Timeout Configuration**: 15 minutes for large dataset downloads
- **SSL Configuration**: Custom SSL context for robust service communication

### Matrix Processing Configuration
```yaml
# Example matrix configuration
layer_types: "markblokke,marker,organic_areas"  # Specific layers
layer_types: "all"                              # All available layers
years: "2024,2023,2022"                        # Specific years  
years: "all"                                   # All available years
stage: "all"                                   # Bronze and silver processing
```

### Spatial Processing Features
- **Coordinate Transformation**: EPSG:25832 → EPSG:4326 using DuckDB-spatial
- **Geometry Validation**: Comprehensive validation and repair of spatial geometries
- **Area Calculations**: Precise hectare calculations for all field polygons
- **UUID Generation**: Deterministic UUID5 based on field geometry
- **CVR Collection**: Automatic extraction and validation of company registration numbers

### Quality Assurance Features
- **Feature Count Validation**: Pre-flight checks for data availability
- **Geometry Integrity**: Validation of spatial data quality and coordinate accuracy
- **Temporal Consistency**: Year-over-year data consistency validation
- **Attribute Completeness**: Assessment of field attribute completeness
- **Resource Monitoring**: Memory and disk usage monitoring for large datasets

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Agricultural Spatial Data Team
- **Response Time**: 1-2 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "FVM WFS" label
- **WFS Service Problems**: Contact system administrators for Danish Agricultural Agency service issues
- **Matrix Processing Issues**: Report matrix workflow problems via technical support channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when WFS service changes or new layers become available
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural spatial data accessible and trustworthy.*
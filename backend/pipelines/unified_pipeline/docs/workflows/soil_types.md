# Soil Types Workflow

> **Manual Processing**: Danish soil classification data from the Environmental Portal for agricultural analysis

---

## What This Workflow Does

The Soil Types workflow collects comprehensive soil classification data from the Danish Environmental Portal. This data provides detailed information about soil characteristics across Denmark, which is essential for understanding agricultural potential, environmental impact assessment, and land use planning.

### Why This Data Matters
- **Agricultural Planning**: Soil types determine what crops can be grown where and expected yields
- **Environmental Analysis**: Soil characteristics affect water retention, nutrient cycling, and pollution transport
- **Land Use Assessment**: Critical for evaluating agricultural land value and development potential
- **Research Support**: Provides foundation data for agricultural and environmental research
- **Policy Development**: Supports agricultural policy and environmental protection decisions

### Key Statistics
- **Data Volume**: ~13,520 soil type polygons covering Denmark
- **Coverage**: Complete national coverage of Danish soil classifications
- **Update Frequency**: Manual execution (on-demand basis)
- **Data Source**: Danish Environmental Portal (Miljøportalen)
- **Processing**: Bronze and Silver layers only (used by Field Area Analysis gold layer)

---

## Data Sources and Collection

### Official Sources
This workflow collects data from the Danish Environmental Portal:

| Data Source | Agency | Purpose | Data Type |
|-------------|--------|---------|-----------|
| Soil Types Layer | Danish Environmental Portal (Miljøportalen) | Official soil classification polygons | WFS (Web Feature Service) |

### How We Collect the Data

#### WFS Service Access
- **Collection Method**: WFS GetFeature requests to Environmental Portal geoserver
- **Endpoint**: `https://arld-extgeo.miljoeportal.dk/geoserver/wfs`
- **Layer**: `landbrugsdrift:DJF_FGJOR` (Jordbundstyper - Soil Types)
- **Format**: GeoJSON format for efficient processing
- **Quality Controls**: HTTP status validation, feature count verification

#### Data Processing Architecture
- **Bronze Layer**: Prepares WFS request metadata and parameters
- **Silver Layer**: Fetches actual data via HTTP, validates geometries with DuckDB-spatial
- **Processing**: Batch processing (1000 features at a time) to manage memory usage
- **Validation**: Geometry validation, coordinate system transformation, quality checks

### Data Privacy and Compliance
- **Personal Data**: None - purely geographic and soil classification data
- **Anonymization**: Not applicable - no personal information involved
- **Legal Compliance**: Public environmental data from official Danish sources
- **Access Restrictions**: No restrictions on soil classification data

---

## Data Processing Steps

### 🥉 Bronze Layer: WFS Request Preparation
**What happens**: We prepare the WFS request parameters and metadata for data collection
**Why**: Separates request configuration from actual data fetching for better error handling
**Output**: WFS request metadata including URL, layer name, and coordinate system

**No Data Fetching**: Bronze layer only prepares request parameters - actual data fetching happens in silver layer

### 🥈 Silver Layer: Data Collection and Standardization
**What happens**: We fetch soil data from WFS service and clean it using DuckDB-spatial
**Why**: Raw WFS data needs geometric validation and attribute standardization

**Specific transformations**:
- **HTTP Data Fetching**: Direct HTTP requests to WFS service for GeoJSON data
- **Batch Processing**: Process features in 1000-feature batches to manage memory
- **Geometry Validation**: Use DuckDB-spatial to validate and fix polygon geometries
- **Coordinate Transformation**: Convert from source CRS to WGS84 (EPSG:4326)
- **Attribute Standardization**: Map Danish field names to standardized English names

**Quality checks**:
- **Feature Count**: Verify expected number of features (~13,520)
- **Geometry Validity**: Check polygon validity and fix invalid geometries
- **Coordinate Bounds**: Validate coordinates are within Denmark bounds
- **Attribute Completeness**: Check for missing soil codes and descriptions

**Output**: Clean GeoParquet files with validated soil type polygons and attributes

### 🥇 Gold Layer: Integration with Field Analysis
**What happens**: Soil types data is used by the Field Area Analysis workflow
**Why**: Soil information is critical for agricultural field analysis and environmental assessment

**Integration**:
- **Field-Soil Matching**: Links agricultural fields to their underlying soil types
- **Spatial Analysis**: Used in complex spatial joins with field boundaries
- **Environmental Assessment**: Supports analysis of agricultural environmental impact

**Output**: Integrated field-soil datasets for comprehensive agricultural analysis

---

## Workflow Schedule and Execution

### Manual Execution Schedule
- **Execution Type**: Manual trigger only (not automated)
- **Typical Usage**: On-demand when updated soil data is needed
- **Processing Duration**: ~15-30 minutes for complete workflow
- **Dependencies**: None (independent data source)

### Processing Performance
- **Data Volume**: ~13,520 soil polygons across Denmark
- **Memory Usage**: Moderate (~2-4GB) due to batch processing
- **Network**: Single HTTP request to WFS service
- **Storage**: ~50MB for processed GeoParquet output

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Official government source, complete national coverage |
| **Accuracy** | Excellent | Authoritative soil classification from environmental experts |
| **Timeliness** | Moderate | Updated periodically by Environmental Portal |
| **Consistency** | Good | Standardized processing ensures consistent format |

### Known Issues and Limitations

#### Data Characteristics
- **Static Nature**: Soil classifications change slowly, updates are infrequent
- **Classification System**: Based on Danish soil classification standards
- **Scale Dependency**: Soil boundaries may not align perfectly with field boundaries

#### Quality Issues
- **Geometric Complexity**: Some soil polygons may have complex shapes or topology issues
- **Attribute Completeness**: Some features may have missing soil descriptions
- **Coordinate Accuracy**: Precision depends on original survey methods

#### Methodological Limitations
- **Scale Resolution**: Soil mapping resolution may not capture micro-variations
- **Temporal Snapshot**: Represents soil conditions at time of survey
- **Classification Boundaries**: Sharp boundaries may not reflect gradual soil transitions

### Recommended Uses
✅ **This data is good for**:
- Agricultural land capability assessment
- Environmental impact analysis of farming practices
- Land use planning and zoning decisions
- Research on soil-crop relationships
- Integration with agricultural field data

⚠️ **Use with caution for**:
- Precision agriculture applications - May lack field-level detail
- Real-time soil condition assessment - Data may not reflect current conditions

❌ **Not recommended for**:
- Site-specific soil analysis - Requires detailed soil sampling
- Dynamic soil property monitoring - This is classification data, not monitoring

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **What soil types are found in a specific agricultural region?** - Filter by geographic area and soil codes
2. **Which areas have the best soil for specific crops?** - Match soil characteristics to crop requirements
3. **How do soil types correlate with agricultural productivity?** - Integrate with field production data

### Example Analyses
#### Agricultural Suitability Assessment
**Question**: Which areas of Denmark have the best soils for cereal production?
**Data Used**: Soil type classifications, soil characteristics, agricultural field boundaries
**Method**: Filter soil types by agricultural suitability criteria and map to field locations
**Limitations**: Suitability depends on specific crop requirements and management practices

#### Environmental Impact Analysis
**Question**: How do different soil types affect nutrient runoff from agricultural fields?
**Data Used**: Soil types, field boundaries, agricultural practices
**Method**: Spatial analysis linking soil drainage characteristics to environmental impact
**Limitations**: Requires additional data on soil physical and chemical properties

### Data Access
- **Public Access**: Soil type maps and general classifications
- **Research Access**: Detailed soil attribute data for approved research projects
- **Download Options**: GeoParquet files for spatial analysis
- **Integration**: Links with agricultural fields, environmental monitoring, land use data

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Soil Types Dataset
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
soil_height | DOUBLE | Soil height measurement | 12.5
soil_description | TEXT | Danish description of soil type | "Sandjord"
soil_code | DOUBLE | Unique soil classification code | 1001
geometry | GEOMETRY | Soil type polygon boundary | POLYGON((...))
processed_at | TIMESTAMP | Processing timestamp | 2024-01-15 10:30:00
data_quality | TEXT | Quality validation status | "validated"
```

### Storage Locations
- **Bronze**: `gs://landbrugsdata-raw-data/bronze/soil_types/{timestamp}/`
- **Silver**: `gs://landbrugsdata-raw-data/silver/soil_types/{timestamp}/`
- **Integration**: Used by Field Area Analysis gold layer

### Processing Infrastructure
- **Platform**: Manual execution via GitHub Actions
- **Resources**: 4GB RAM, standard CPU
- **Dependencies**: Danish Environmental Portal WFS service
- **Performance**: ~30 minutes for complete national dataset

### WFS Service Details
- **Service URL**: `https://arld-extgeo.miljoeportal.dk/geoserver/wfs`
- **Layer Name**: `landbrugsdrift:DJF_FGJOR`
- **Output Format**: GeoJSON
- **Coordinate System**: Converted to EPSG:4326 (WGS84)

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Geospatial Data Team
- **Response Time**: 2-3 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Soil Types" label
- **Access Problems**: Contact geospatial support team
- **Feature Requests**: Submit enhancement requests via project channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when workflow changes
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural data accessible and trustworthy.*

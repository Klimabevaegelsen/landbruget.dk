# Wetlands Workflow

> **Manual Processing**: Danish wetland and peat area mapping from environmental monitoring data

---

## What This Workflow Does

The Wetlands workflow collects comprehensive wetland and peat area data from the Danish Ministry of Environment's WFS service. This data identifies areas with high carbon storage potential (peat) and wetland characteristics that are critical for environmental protection, climate policy, and agricultural land use planning.

### Why This Data Matters
- **Climate Policy**: Peat areas store significant carbon - disturbing them releases CO₂ to atmosphere
- **Environmental Protection**: Wetlands provide crucial ecosystem services including flood control and biodiversity
- **Agricultural Planning**: Identifies areas where farming practices must consider environmental sensitivity
- **Carbon Accounting**: Essential for national greenhouse gas inventories and climate commitments
- **Conservation**: Supports wetland protection and restoration programs

### Key Statistics
- **Data Volume**: ~100,000+ wetland and peat features across Denmark
- **Coverage**: National coverage of carbon-storing peat areas and wetlands
- **Update Frequency**: Manual execution (on-demand basis)
- **Data Source**: Danish Ministry of Environment (WFS service)
- **Processing**: Bronze and Silver layers, plus dissolved polygons for analysis

---

## Data Sources and Collection

### Official Sources
This workflow collects data from the Danish Ministry of Environment:

| Data Source | Agency | Purpose | Data Type |
|-------------|--------|---------|-----------|
| Carbon Storage Areas 2022 | Danish Ministry of Environment | Wetland and peat carbon storage mapping | WFS (Web Feature Service) |

### How We Collect the Data

#### WFS Service Access
- **Collection Method**: WFS GetFeature requests to Ministry of Environment geoserver
- **Endpoint**: `https://wfs2-miljoegis.mim.dk/natur/wfs`
- **Layer**: `natur:kulstof2022` (Carbon Storage Areas 2022)
- **Format**: XML/GML format with spatial geometries
- **Quality Controls**: HTTP status validation, XML parsing validation, feature count verification

#### Data Processing Architecture
- **Bronze Layer**: Parallel data fetching with retry logic and error handling
- **Silver Layer**: XML parsing, geometry processing, and spatial analysis with DuckDB-spatial
- **Batch Processing**: 10,000 features per request to optimize performance
- **Parallel Fetching**: Up to 3 concurrent requests to balance speed and server load

#### Advanced Spatial Processing
- **Overlap Resolution**: Handles overlapping peat percentage areas by prioritizing higher percentages (>12% beats 6-12%)
- **Adjacency Detection**: Uses DuckDB-spatial SPATIAL_JOIN operator for high-performance adjacent polygon detection
- **Polygon Merging**: Creates dissolved (merged) version by combining adjacent wetlands with same characteristics
- **Coordinate Preservation**: Performs adjacency detection in original coordinates before transformation to preserve precision

### Data Privacy and Compliance
- **Personal Data**: None - purely environmental and geographic data
- **Anonymization**: Not applicable - no personal information involved
- **Legal Compliance**: Public environmental data from official Danish sources
- **Access Restrictions**: No restrictions on environmental monitoring data

---

## Data Processing Steps

### 🥉 Bronze Layer: Parallel Data Collection
**What happens**: We fetch wetland data from WFS service using parallel requests with retry logic
**Why**: Large dataset requires efficient parallel processing to complete in reasonable time
**Output**: Raw XML/GML responses containing wetland features and geometries

**Parallel Processing Features**:
- **Batch Size**: 10,000 features per request
- **Concurrency**: Up to 3 concurrent requests
- **Retry Logic**: Exponential backoff for failed requests
- **SSL Handling**: Disabled certificate verification for service compatibility

### 🥈 Silver Layer: Advanced Spatial Processing
**What happens**: We parse XML data and create both standard and dissolved (merged) wetland datasets
**Why**: Raw XML needs parsing and spatial analysis to create analysis-ready datasets

**Specific transformations**:
- **XML Parsing**: Extract wetland features, peat percentages, and polygon geometries from GML
- **Overlap Resolution**: Handle overlapping peat areas by prioritizing higher percentages (>12% over 6-12%)
- **Adjacency Detection**: Use DuckDB-spatial SPATIAL_JOIN operator to find touching wetland polygons
- **Polygon Merging**: Dissolve adjacent wetlands with same characteristics into larger polygons
- **Coordinate Transformation**: Convert from Danish UTM (EPSG:25832) to WGS84 (EPSG:4326)

**Quality checks**:
- **Geometry Validation**: Check polygon validity and fix invalid geometries
- **Feature Counts**: Verify expected number of features processed
- **Coordinate Bounds**: Validate coordinates are within Denmark bounds
- **Peat Percentage Logic**: Ensure overlap resolution preserved data integrity

**Output**: Two datasets - original wetlands and dissolved (merged) wetlands, both as GeoParquet files

### 🥇 Gold Layer: Integration with Field Analysis
**What happens**: Wetlands data is used by the Field Area Analysis workflow
**Why**: Wetland proximity and overlap analysis is critical for agricultural environmental impact assessment

**Integration**:
- **Field-Wetland Proximity**: Calculate distances from agricultural fields to wetlands
- **Environmental Constraints**: Identify fields with wetland proximity restrictions
- **Impact Assessment**: Support analysis of agricultural activities near sensitive wetland areas

**Output**: Integrated field-wetland analysis for environmental compliance and planning

---

## Workflow Schedule and Execution

### Manual Execution Schedule
- **Execution Type**: Manual trigger only (not automated)
- **Typical Usage**: When updated wetland mapping data is available (typically annually)
- **Processing Duration**: ~45-60 minutes for complete national dataset
- **Dependencies**: None (independent data source)

### Processing Performance
- **Data Volume**: ~100,000+ wetland features across Denmark
- **Memory Usage**: High (~8-12GB) due to complex spatial operations
- **Network**: Multiple parallel HTTP requests to WFS service
- **Storage**: ~200MB for processed GeoParquet outputs (original + dissolved)

### Spatial Processing Optimization
- **SPATIAL_JOIN Operator**: Uses DuckDB-spatial's optimized SPATIAL_JOIN for adjacency detection
- **Original Coordinates**: Performs adjacency detection before coordinate transformation to preserve precision
- **Batch Processing**: Processes large connected component groups in batches to manage memory

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Official government source, comprehensive national coverage |
| **Accuracy** | Excellent | Based on detailed environmental surveys and carbon storage analysis |
| **Timeliness** | Good | Updated periodically based on new environmental assessments |
| **Consistency** | Good | Standardized processing with overlap resolution and geometry validation |

### Known Issues and Limitations

#### Data Characteristics
- **Temporal Snapshot**: Represents wetland conditions at time of survey (2022 dataset)
- **Peat Percentage Categories**: Uses broad categories (>12%, 6-12%, etc.) rather than precise measurements
- **Overlap Areas**: Some areas have overlapping peat percentage classifications requiring resolution

#### Quality Issues
- **Geometric Complexity**: Some wetland polygons may have complex shapes requiring validation
- **Adjacency Precision**: Coordinate transformation may introduce tiny gaps between originally adjacent polygons
- **Processing Intensive**: Large dataset requires significant computational resources

#### Methodological Limitations
- **Classification Boundaries**: Sharp wetland boundaries may not reflect gradual ecological transitions
- **Scale Resolution**: Based on available survey resolution, may miss small wetland features
- **Carbon Estimates**: Peat percentages are estimates, not direct carbon content measurements

### Recommended Uses
✅ **This data is good for**:
- Environmental impact assessment of agricultural activities
- Climate policy and carbon accounting applications
- Wetland conservation and restoration planning
- Land use planning considering environmental constraints
- Research on wetland-agriculture interactions

⚠️ **Use with caution for**:
- Precise carbon content calculations - Use peat percentages as indicators, not exact measurements
- Field-level environmental compliance - Consider buffer zones and uncertainty

❌ **Not recommended for**:
- Real-time wetland monitoring - This is a periodic survey dataset
- Detailed carbon stock calculations - Requires additional soil sampling data

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **Which agricultural areas are near environmentally sensitive wetlands?** - Spatial proximity analysis with field boundaries
2. **Where are the highest carbon storage peat areas in Denmark?** - Filter by peat percentage categories and analyze spatial distribution
3. **How much wetland area exists in a specific region?** - Aggregate wetland areas by municipality or other boundaries

### Example Analyses
#### Agricultural Environmental Impact Assessment
**Question**: Which farms operate near high-carbon peat areas that require special protection?
**Data Used**: Wetlands with >12% peat, agricultural field boundaries, farm ownership data
**Method**: Spatial proximity analysis to identify fields within buffer distances of high-carbon wetlands
**Limitations**: Requires definition of appropriate buffer distances and consideration of local drainage patterns

#### Wetland Conservation Planning
**Question**: Where should wetland restoration efforts be prioritized for maximum carbon storage benefit?
**Data Used**: Dissolved wetlands dataset, peat percentage classifications, land use data
**Method**: Identify large connected wetland areas with high carbon storage potential
**Limitations**: Restoration potential depends on current land use and hydrological conditions not captured in this dataset

### Data Access
- **Public Access**: Wetland maps and general environmental information
- **Research Access**: Detailed wetland datasets for approved environmental research
- **Download Options**: GeoParquet files for both original and dissolved wetland polygons
- **Integration**: Links with agricultural fields, environmental monitoring, conservation planning

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Wetlands Dataset (Original)
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
id | TEXT | Unique wetland feature identifier | "kulstof2022.123456"
gridcode | INTEGER | Grid classification code | 1
toerv_pct | TEXT | Peat percentage category | ">12"
geometry | GEOMETRY | Wetland polygon boundary | POLYGON((...))
```

#### Wetlands Dissolved Dataset (Merged)
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
wetland_id | INTEGER | Unique dissolved wetland identifier | 1
toerv_pct | TEXT | Dominant peat percentage category | ">12"
geometry | GEOMETRY | Merged wetland polygon boundary | MULTIPOLYGON((...))
```

### Storage Locations
- **Bronze**: `gs://landbrugsdata-raw-data/bronze/wetlands/{timestamp}/`
- **Silver**: `gs://landbrugsdata-raw-data/silver/wetlands/{timestamp}/`
- **Silver Dissolved**: `gs://landbrugsdata-raw-data/silver/wetlands_dissolved/{timestamp}/`
- **Integration**: Used by Field Area Analysis gold layer

### Processing Infrastructure
- **Platform**: Manual execution via GitHub Actions
- **Resources**: 16GB RAM, high CPU for spatial operations
- **Dependencies**: Danish Ministry of Environment WFS service
- **Performance**: ~60 minutes for complete national dataset with spatial processing

### WFS Service Details
- **Service URL**: `https://wfs2-miljoegis.mim.dk/natur/wfs`
- **Layer Name**: `natur:kulstof2022`
- **Output Format**: XML/GML
- **Coordinate System**: Source EPSG:25832, converted to EPSG:4326 (WGS84)
- **Batch Size**: 10,000 features per request
- **Concurrency**: Up to 3 parallel requests

### Spatial Processing Features
- **Overlap Resolution**: Prioritizes >12% peat over 6-12% peat in overlapping areas
- **SPATIAL_JOIN Optimization**: Uses DuckDB-spatial's optimized operator for adjacency detection
- **Connected Components**: Groups adjacent wetlands for merging using iterative algorithm
- **Geometry Validation**: ST_Buffer(geometry, 0) to fix invalid polygons

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Environmental Data Team
- **Response Time**: 2-3 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Wetlands" label
- **Access Problems**: Contact environmental data support team
- **Feature Requests**: Submit enhancement requests via project channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when workflow changes
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural data accessible and trustworthy.*

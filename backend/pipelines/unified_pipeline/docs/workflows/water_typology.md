# Water Typology Workflow

> **Manual Processing**: Danish water body classification data for environmental assessment and regulatory compliance

---

## What This Workflow Does

The Water Typology workflow collects comprehensive water body classification data from the Danish Ministry of Environment's WFS service. This data provides detailed typological classifications for lakes, coastal waters, and watercourses that are essential for environmental monitoring, water quality assessment, and EU Water Framework Directive compliance.

### Why This Data Matters
- **Environmental Monitoring**: Water typology classifications are fundamental for water quality assessment and ecological status evaluation
- **EU Compliance**: Required for Water Framework Directive reporting and environmental status monitoring
- **Agricultural Impact**: Identifies water bodies that may be affected by agricultural runoff and land use practices
- **Conservation Planning**: Supports water body protection and restoration program development
- **Research Support**: Provides standardized water body classifications for environmental research

### Key Statistics
- **Data Volume**: ~10,000+ water body features across three categories
- **Coverage**: Complete national coverage of Danish water bodies with typological classifications
- **Update Frequency**: Manual execution (on-demand basis)
- **Data Source**: Danish Ministry of Environment (VP3 Final 2022 dataset)
- **Processing**: Bronze and Silver layers, no dissolving (individual features preserved)

---

## Data Sources and Collection

### Official Sources
This workflow collects data from the Danish Ministry of Environment:

| Water Type | Layer | Purpose | Geometry Type |
|------------|-------|---------|---------------|
| Lakes | vp3e2022_soe_samlet | Lake typology classification | MULTIPOLYGON |
| Coastal Waters | vp3e2022_marin_samlet | Coastal water typology | MULTIPOLYGON |
| Watercourses | vp3e2022_vandloeb_samlet | River/stream typology | MULTILINESTRING |

### How We Collect the Data

#### WFS Service Access
- **Collection Method**: WFS GetFeature requests to Ministry of Environment geoserver
- **Endpoint**: `https://wfs2-miljoegis.mim.dk/vp3endelig2022/ows`
- **Layers**: Three separate layers for different water body types
- **Format**: XML/GML format with complex geometries
- **Quality Controls**: HTTP status validation, XML parsing validation, geometry type verification

#### Data Processing Architecture
- **Bronze Layer**: Concurrent data fetching from three different water typology layers
- **Silver Layer**: XML/GML parsing with geometry type-specific processing and validation
- **Batch Processing**: 10,000 features per request to optimize performance
- **Parallel Fetching**: Up to 10 concurrent requests across different layers

#### Advanced Geometry Processing
- **Multi-Geometry Support**: Handles MULTIPOLYGON (lakes/coastal) and MULTILINESTRING (watercourses)
- **GML Parsing**: Custom parser for complex GML geometry structures with coordinate extraction
- **Geometry Validation**: DuckDB-spatial validation and repair using ST_MakeValid for invalid geometries
- **Type Preservation**: Maintains individual water body features (no dissolving) for typological analysis

### Data Privacy and Compliance
- **Personal Data**: None - purely environmental classification data
- **Anonymization**: Not applicable - no personal information involved
- **Legal Compliance**: Public environmental data from official Danish sources
- **Access Restrictions**: No restrictions on water body classification data

---

## Data Processing Steps

### 🥉 Bronze Layer: Multi-Layer Data Collection
**What happens**: We fetch water typology data from three different WFS layers concurrently
**Why**: Different water body types (lakes, coastal, watercourses) have different characteristics and classifications
**Output**: Raw XML/GML responses for each water body type stored separately

**Layer-Specific Processing**:
- **Lakes (Søer)**: MULTIPOLYGON geometries with lake-specific attributes
- **Coastal Waters (Kystvande)**: MULTIPOLYGON geometries with marine-specific attributes  
- **Watercourses (Vandløb)**: MULTILINESTRING geometries with river/stream-specific attributes

### 🥈 Silver Layer: Advanced Multi-Geometry Processing
**What happens**: We parse XML/GML data and create unified water typology dataset with proper geometry handling
**Why**: Raw XML/GML needs specialized parsing for different geometry types and complex coordinate structures

**Specific transformations**:
- **XML/GML Parsing**: Extract water body features and attributes from complex XML structures
- **Geometry Type Detection**: Automatically detect and handle MULTIPOLYGON vs MULTILINESTRING geometries
- **Coordinate Extraction**: Parse GML posList elements and convert to WKT format
- **Schema Unification**: Combine different layer schemas into unified table structure with NULL handling
- **Geometry Validation**: Use DuckDB-spatial ST_MakeValid to repair invalid geometries

**Quality checks**:
- **Geometry Conversion**: Track success rate of GML→WKT conversion (target >95%)
- **Geometry Validity**: Validate all geometries and repair with ST_MakeValid if needed
- **Feature Completeness**: Verify expected features from all three water body types
- **Attribute Preservation**: Ensure all water body attributes are correctly extracted

**Output**: Single unified GeoParquet dataset containing all water body types with validated geometries

### 🥇 Gold Layer: Integration with Environmental Analysis
**What happens**: Water typology data is used by environmental analysis workflows
**Why**: Water body classifications are essential for environmental impact assessment and proximity analysis

**Integration**:
- **Pesticide Proximity**: Used in pesticide proximity analysis to identify applications near sensitive water bodies
- **Environmental Assessment**: Supports analysis of agricultural activities' impact on different water body types
- **Regulatory Compliance**: Enables compliance checking against water body protection requirements

**Output**: Integrated environmental analysis incorporating water body typological classifications

---

## Workflow Schedule and Execution

### Manual Execution Schedule
- **Execution Type**: Manual trigger only (not automated)
- **Typical Usage**: When updated water typology classifications are available (typically every few years)
- **Processing Duration**: ~30-45 minutes for complete national dataset
- **Dependencies**: None (independent data source)

### Processing Performance
- **Data Volume**: ~10,000+ water body features across three categories
- **Memory Usage**: Moderate (~4-8GB) due to complex geometry processing
- **Network**: Multiple concurrent HTTP requests to WFS service (up to 10 concurrent)
- **Storage**: ~100MB for processed GeoParquet output

### Geometry Processing Optimization
- **Custom GML Parser**: Specialized parser for complex GML geometry structures
- **Type-Specific Handling**: Different processing paths for MULTIPOLYGON vs MULTILINESTRING
- **Validation & Repair**: Automatic geometry validation and repair using DuckDB-spatial
- **Success Rate Monitoring**: Tracks and reports geometry conversion success rates

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Official government source, comprehensive national coverage |
| **Accuracy** | Excellent | Based on detailed environmental surveys and EU Water Framework Directive requirements |
| **Timeliness** | Good | Updated periodically based on water body assessments (VP3 Final 2022 dataset) |
| **Consistency** | Good | Standardized processing with geometry validation and type-specific handling |

### Known Issues and Limitations

#### Data Characteristics
- **Temporal Snapshot**: Represents water body classifications at specific assessment period (2022)
- **Typological Categories**: Based on EU Water Framework Directive classification system
- **Complex Geometries**: Water bodies may have complex multi-part geometries requiring specialized processing

#### Quality Issues
- **GML Complexity**: Some GML geometries may have complex coordinate structures requiring custom parsing
- **Geometry Validity**: Some source geometries may be invalid requiring repair with ST_MakeValid
- **Coordinate Precision**: Geometry conversion may introduce minor precision changes

#### Methodological Limitations
- **Classification Boundaries**: Water body boundaries may not align perfectly with other spatial datasets
- **Typological Changes**: Water body classifications may change over time based on environmental conditions
- **Scale Dependencies**: Classification resolution depends on original survey methodology

### Recommended Uses
✅ **This data is good for**:
- Environmental impact assessment of agricultural activities near water bodies
- Water Framework Directive compliance monitoring and reporting
- Water quality assessment and ecological status evaluation
- Environmental research on water body typology and classification
- Integration with agricultural and land use datasets for proximity analysis

⚠️ **Use with caution for**:
- Precise water body boundary determination - Use for classification context, not exact boundaries
- Real-time water quality assessment - This is classification data, not monitoring data

❌ **Not recommended for**:
- Navigation or hydrographic purposes - This is environmental classification, not navigational data
- Detailed hydrological modeling - Requires additional hydrological parameters not included

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **What types of water bodies are near agricultural fields?** - Spatial analysis with field boundaries to identify proximity to different water body types
2. **Which agricultural areas are near sensitive coastal waters?** - Filter coastal water features and analyze proximity to agricultural activities
3. **Where are watercourses that could be affected by agricultural runoff?** - Identify watercourse features and analyze upstream agricultural land use

### Example Analyses
#### Agricultural Water Impact Assessment
**Question**: Which farms operate near environmentally sensitive water bodies requiring special protection measures?
**Data Used**: Water typology classifications, agricultural field boundaries, water body sensitivity ratings
**Method**: Spatial proximity analysis to identify fields within buffer distances of sensitive water bodies
**Limitations**: Requires definition of appropriate buffer distances and consideration of hydrological connectivity

#### Water Framework Directive Compliance
**Question**: How are agricultural practices distributed around different water body types for EU reporting?
**Data Used**: Water typology classifications, agricultural land use data, pesticide application records
**Method**: Spatial analysis of agricultural intensity by water body type and classification
**Limitations**: Compliance assessment requires additional water quality monitoring data not included in this dataset

### Data Access
- **Public Access**: Water body classification maps and general typological information
- **Research Access**: Detailed water typology datasets for approved environmental research
- **Download Options**: GeoParquet files with all three water body types in unified format
- **Integration**: Links with agricultural fields, environmental monitoring, regulatory compliance systems

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### Water Typology Dataset (Unified)
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
layer | VARCHAR | Source layer (water body type) | "vp3e2022_soe_samlet"
source | VARCHAR | Dataset source identifier | "water_typology"
geometry_spatial | GEOMETRY | Water body geometry (MULTIPOLYGON/MULTILINESTRING) | MULTIPOLYGON((...))
geometry | TEXT | WKT geometry representation | "MULTIPOLYGON((...)"
[layer-specific attributes] | VARCHAR | Water body classification attributes | varies by layer
```

#### Layer-Specific Attributes
- **Lakes**: Lake typology codes, size classification, ecological status indicators
- **Coastal Waters**: Marine typology, salinity classification, coastal zone indicators
- **Watercourses**: Stream order, flow characteristics, catchment classifications

### Storage Locations
- **Bronze**: `gs://landbrugsdata-raw-data/bronze/water_typology_{layer}/{timestamp}/`
- **Silver**: `gs://landbrugsdata-raw-data/silver/water_typology/{timestamp}/`
- **Integration**: Used by pesticide proximity and environmental analysis workflows

### Processing Infrastructure
- **Platform**: Manual execution via GitHub Actions
- **Resources**: 8GB RAM, standard CPU with spatial processing optimization
- **Dependencies**: Danish Ministry of Environment WFS service (VP3 Final 2022)
- **Performance**: ~45 minutes for complete national dataset with geometry processing

### WFS Service Details
- **Service URL**: `https://wfs2-miljoegis.mim.dk/vp3endelig2022/ows`
- **Layer Names**: 
  - `vp3endelig2022:vp3e2022_soe_samlet` (Lakes)
  - `vp3endelig2022:vp3e2022_marin_samlet` (Coastal Waters)  
  - `vp3endelig2022:vp3e2022_vandloeb_samlet` (Watercourses)
- **Output Format**: XML/GML
- **Coordinate System**: Source coordinates, converted to EPSG:4326 (WGS84)
- **Batch Size**: 10,000 features per request
- **Concurrency**: Up to 10 parallel requests

### Geometry Processing Features
- **Multi-Type Support**: MULTIPOLYGON for area features, MULTILINESTRING for linear features
- **Custom GML Parser**: Handles complex GML coordinate structures and multi-part geometries
- **Validation & Repair**: ST_MakeValid for invalid geometries, success rate monitoring
- **Type Preservation**: No dissolving - maintains individual water body features for classification analysis

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Environmental Data Team
- **Response Time**: 2-3 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "Water Typology" label
- **Access Problems**: Contact environmental data support team
- **Feature Requests**: Submit enhancement requests via project channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when workflow changes
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural data accessible and trustworthy.*

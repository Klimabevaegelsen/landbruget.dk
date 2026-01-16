# DMI (Danish Meteorological Institute) Workflow

> **Manual Processing**: Danish climate data from official meteorological services

---

## What This Workflow Does

The DMI (Danish Meteorological Institute) workflow collects and processes official Danish climate data from the DMI GovCloud API. This workflow fetches monthly climate parameters essential for agricultural analysis, including potential evaporation and precipitation data, transforming raw meteorological observations into analysis-ready spatial datasets.

### Why This Data Matters
- **Agricultural Planning**: Climate data essential for crop planning, irrigation management, and agricultural decision-making
- **Environmental Analysis**: Support environmental impact assessments and climate change studies
- **Water Management**: Precipitation and evaporation data crucial for water resource planning
- **Research Support**: Official meteorological data for agricultural and environmental research
- **Policy Development**: Climate information for agricultural policy and adaptation strategies

### Key Statistics
- **Data Coverage**: National Danish climate grid data (10km resolution)
- **Temporal Scope**: Monthly data from 2011 to present (DMI grid data availability)
- **Parameters**: 2 core climate parameters (potential evaporation, accumulated precipitation)
- **Processing**: Complete Bronze and Silver layer processing with spatial transformation
- **Data Source**: Official DMI GovCloud API with authenticated access

---

## Data Sources and Collection

### Official Sources
This workflow collects data from the Danish Meteorological Institute:

| Parameter | Purpose | Units | Resolution |
|-----------|---------|-------|------------|
| **Potential Evaporation (Makkink)** | Agricultural water demand estimation | mm/month | 10km grid |
| **Accumulated Precipitation** | Rainfall and water supply analysis | mm/month | 10km grid |

### Data Collection Process

#### DMI GovCloud API Integration
- **Authentication**: Secure API access using DMI GovCloud API key
- **Endpoint**: `https://dmigw.govcloud.dk/v2/climateData/collections/10kmGridValue/items`
- **Data Format**: GeoJSON responses with climate values and spatial coordinates
- **Temporal Strategy**: Month-by-month fetching to avoid API limits (10,000 features max)
- **Concurrent Processing**: Up to 5 concurrent API requests for efficient data collection

#### Advanced Temporal Processing
- **Historical Coverage**: Complete data from January 2011 to present
- **Monthly Aggregation**: DMI API automatically provides monthly aggregated values
- **Incremental Fetching**: Month-by-month iteration ensures complete historical coverage
- **Retry Logic**: Exponential backoff retry strategy for robust API communication
- **Rate Limiting**: Built-in rate limit handling with appropriate delays

#### Spatial Data Processing
- **Native CRS**: Data collected in EPSG:25832 (Danish UTM Zone 32N)
- **Target CRS**: Transformed to EPSG:4326 (WGS84) for standardization
- **Grid Resolution**: 10km spatial resolution covering all of Denmark
- **Geometry Handling**: Full spatial geometry preservation with coordinate transformation

### Data Privacy and Compliance
- **Public Data**: Official meteorological observations, no privacy concerns
- **API Authentication**: Secure access through official DMI GovCloud credentials
- **Data Licensing**: Complies with Danish meteorological data sharing agreements
- **Access Controls**: API key management through secure environment variables

---

## Data Processing Steps

### 🥉 Bronze Layer: Comprehensive Climate Data Collection
**What happens**: We fetch raw monthly climate data from DMI GovCloud API with comprehensive temporal coverage
**Why**: Official meteorological data requires authenticated access and careful temporal management to ensure complete historical coverage

**Specific processing**:
- **API Authentication**: Secure connection using DMI GovCloud API key
- **Temporal Iteration**: Month-by-month fetching from 2011 to present to avoid API limits
- **Parameter Processing**: Concurrent collection of multiple climate parameters
- **Error Handling**: Robust retry logic with exponential backoff for API reliability
- **Data Persistence**: Raw GeoJSON responses stored with processing metadata

**Quality controls**:
- **API Response Validation**: Verify response structure and feature presence
- **Temporal Completeness**: Ensure continuous monthly coverage without gaps
- **Feature Count Monitoring**: Track number of features per month and parameter
- **Error Recovery**: Graceful handling of API timeouts and rate limits

**Output**: Raw monthly climate data with full spatial and temporal coverage

### 🥈 Silver Layer: Advanced Spatial Data Transformation
**What happens**: We transform raw climate data into analysis-ready spatial datasets using DuckDB-spatial
**Why**: Raw GeoJSON data needs coordinate transformation, statistical processing, and standardized formatting for analytical use

**Specific transformations**:
- **GeoJSON Processing**: Extract climate values and spatial geometries from API responses
- **CRS Transformation**: Convert from EPSG:25832 (Danish UTM) to EPSG:4326 (WGS84) using DuckDB-spatial
- **Statistical Aggregation**: Calculate monthly statistics (avg, min, max, stddev) for each parameter
- **Spatial Analysis**: Generate centroid and bounding box geometries for coverage areas
- **Metadata Enhancement**: Add processing timestamps and coordinate system information

**Quality checks**:
- **Geometry Validation**: Ensure successful coordinate transformation and valid geometries
- **Statistical Validation**: Verify reasonable value ranges for climate parameters
- **Completeness Assessment**: Monitor feature counts through transformation pipeline
- **Spatial Consistency**: Validate coordinate transformation accuracy

**Output**: Processed monthly climate datasets with standardized coordinates and statistical summaries

### 🥇 Gold Layer: Integration with Agricultural Analysis
**What happens**: Climate data is integrated with agricultural workflows for comprehensive analysis
**Why**: Climate parameters are essential inputs for agricultural modeling and environmental assessment

**Integration applications**:
- **Irrigation Planning**: Evapotranspiration data for agricultural water management
- **Crop Modeling**: Climate inputs for agricultural yield and growth models
- **Environmental Assessment**: Climate context for agricultural impact analysis
- **Risk Analysis**: Weather pattern analysis for agricultural risk assessment

**Output**: Climate data ready for agricultural and environmental analysis workflows

---

## Workflow Schedule and Execution

### Manual Processing Schedule
- **Execution Type**: Manual trigger only (not automated)
- **Typical Usage**: When updated climate analysis is needed or new historical data becomes available
- **Processing Duration**: ~45-60 minutes for complete historical dataset (2011-present)
- **Dependencies**: DMI GovCloud API access and authentication
- **Data Refresh**: Monthly updates as new DMI data becomes available

### Processing Performance
- **Data Volume**: ~10,000+ monthly climate observations per parameter across 13+ years
- **Memory Usage**: Moderate (~8GB) for spatial processing and coordinate transformation
- **API Calls**: Hundreds of monthly API requests with rate limiting and retry logic
- **Storage**: ~100-500MB for processed climate datasets depending on temporal coverage
- **Network**: Sustained API communication with DMI GovCloud services

### Advanced Features
- **Month-by-Month Processing**: Avoids API limits while ensuring complete temporal coverage
- **Concurrent Parameter Fetching**: Parallel processing of multiple climate parameters
- **Spatial Transformation**: Full coordinate system conversion using DuckDB-spatial
- **Statistical Processing**: Automated calculation of climate statistics and spatial summaries

---

## Data Quality and Limitations

### Data Quality Assessment
| Quality Metric | Status | Details |
|----------------|--------|---------|
| **Completeness** | Excellent | Complete monthly coverage from 2011 to present via official DMI API |
| **Accuracy** | Excellent | Official Danish Meteorological Institute observations and calculations |
| **Timeliness** | Good | Monthly updates as new DMI data becomes available |
| **Spatial Resolution** | Good | 10km grid resolution covering all of Denmark |

### Known Issues and Limitations

#### API and Data Constraints
- **API Rate Limits**: 10,000 features per request requiring month-by-month processing
- **Authentication Dependency**: Requires valid DMI GovCloud API key for access
- **Network Dependency**: Processing success depends on stable API connectivity
- **Temporal Availability**: DMI grid data only available from 2011 onwards

#### Spatial and Temporal Limitations
- **Grid Resolution**: 10km spatial resolution may be coarse for detailed local analysis
- **Parameter Selection**: Limited to 2 core parameters (evaporation, precipitation)
- **Monthly Aggregation**: No sub-monthly temporal resolution available
- **Coordinate Transformation**: Minor precision changes during CRS conversion

#### Processing Considerations
- **Processing Time**: Complete historical processing requires significant time investment
- **API Stability**: Processing success depends on DMI API service availability
- **Memory Requirements**: Large temporal datasets require adequate processing resources
- **Storage Costs**: Historical climate data requires substantial storage capacity

### Recommended Uses
✅ **This data is excellent for**:
- Agricultural water management and irrigation planning using official climate data
- Environmental impact assessment with authoritative meteorological context
- Climate change analysis and agricultural adaptation planning
- Research requiring official Danish meteorological observations
- Integration with agricultural models requiring climate inputs

⚠️ **Use with caution for**:
- High-resolution local analysis - 10km grid may be too coarse for field-level applications
- Real-time applications - Monthly aggregation and processing delays limit real-time use
- Sub-monthly analysis - Data aggregated to monthly resolution only

❌ **Not recommended for**:
- Daily or hourly weather analysis - Use dedicated weather services for higher temporal resolution
- Non-Danish applications - Data specific to Danish territory and coordinate systems
- Real-time weather monitoring - Historical and monthly data, not real-time observations

---

## Usage Examples and Access

### Common Questions This Data Answers
1. **What are the long-term precipitation trends across Denmark?** - Time series analysis of accumulated precipitation from 2011-present
2. **How does potential evaporation vary spatially across agricultural regions?** - Spatial analysis of Makkink evaporation patterns
3. **What climate conditions correlate with agricultural productivity patterns?** - Integration of climate data with agricultural yield analysis
4. **How have climate patterns changed over the past decade in Denmark?** - Trend analysis of temperature and precipitation evolution

### Example Analyses
#### Agricultural Water Balance Assessment
**Question**: Which Danish regions have the highest water stress for agriculture?
**Data Used**: Monthly potential evaporation and precipitation data across all grid cells
**Method**: Calculate water balance (precipitation - evaporation) and identify deficit regions
**Output**: Spatial maps showing agricultural water stress patterns and seasonal variations
**Limitations**: 10km resolution may not capture local variations; monthly data misses short-term patterns

#### Climate Trend Analysis for Agriculture
**Question**: How have growing season climate conditions changed since 2011?
**Data Used**: Monthly climate parameters aggregated by growing season (April-September)
**Method**: Trend analysis of growing season precipitation and evaporation patterns
**Output**: Time series showing climate trends relevant to agricultural planning
**Limitations**: Relatively short time series (2011-present); monthly aggregation limits seasonal detail

#### Irrigation Planning Support
**Question**: When and where is irrigation most needed based on climate patterns?
**Data Used**: Monthly evaporation and precipitation data with spatial and temporal analysis
**Method**: Calculate irrigation requirements based on crop water demand and precipitation supply
**Output**: Seasonal irrigation planning maps and schedules for different regions
**Limitations**: Requires integration with crop-specific water requirement data not included

### Data Access
- **Research Access**: Full climate datasets for academic and scientific research
- **Agricultural Access**: Climate data for agricultural planning and management applications
- **Policy Access**: Official climate information for policy development and planning
- **Public Access**: Aggregate climate statistics and trend summaries

---

## Technical Details

<details>
<summary>Click to expand technical specifications</summary>

### Data Schema
#### DMI Climate Dataset (Silver Layer)
```
Field Name | Type | Description | Example
-----------|------|-------------|--------
parameter_id | VARCHAR | Climate parameter identifier | "pot_evaporation_makkink"
valid_time | VARCHAR | Temporal validity (month) | "2023-07-01T00:00:00Z"
created | VARCHAR | Data creation timestamp | "2023-08-15T10:30:00Z"
avg_value | DOUBLE | Monthly average value | 85.4
min_value | DOUBLE | Monthly minimum value | 65.2
max_value | DOUBLE | Monthly maximum value | 105.8
count | BIGINT | Number of grid cells | 1247
stddev_value | DOUBLE | Standard deviation | 12.3
centroid_geometry | VARCHAR | GeoJSON centroid | {"type":"Point","coordinates":[...]}
bbox_geometry | VARCHAR | GeoJSON bounding box | {"type":"Polygon","coordinates":[...]}
processing_time | VARCHAR | Processing timestamp | "2025-01-15T14:20:00"
source_crs | VARCHAR | Source coordinate system | "EPSG:25832"
target_crs | VARCHAR | Target coordinate system | "EPSG:4326"
original_feature_count | INTEGER | Original feature count | 1247
```

#### Climate Parameters
- **pot_evaporation_makkink**: Potential evapotranspiration using Makkink method (mm/month)
- **acc_precip**: Accumulated precipitation (mm/month)

#### Coordinate Systems
- **Source CRS**: EPSG:25832 (UTM Zone 32N, Danish national grid)
- **Target CRS**: EPSG:4326 (WGS84, global standard)
- **Transformation**: DuckDB-spatial ST_Transform function

### Storage Locations
- **Bronze**: `gs://landbrugsdata-raw-data/bronze/dmi/{timestamp}/{parameter}_data.json`
- **Bronze Metadata**: `gs://landbrugsdata-raw-data/bronze/dmi/{timestamp}/{parameter}_metadata.json`
- **Silver**: `gs://landbrugsdata-raw-data/silver/dmi_{parameter}/{timestamp}/data.parquet`

### Processing Infrastructure
- **Platform**: Manual execution via GitHub Actions
- **Resources**: 8GB RAM, spatial processing optimization
- **Dependencies**: DMI GovCloud API access, DuckDB-spatial extension
- **Performance**: ~60 minutes for complete historical processing (2011-present)
- **API Configuration**: 5 concurrent requests, exponential backoff retry

### API Integration Details
- **Base URL**: `https://dmigw.govcloud.dk/v2/climateData`
- **Endpoint**: `/collections/10kmGridValue/items`
- **Authentication**: X-Gravitee-Api-Key header with DMI GovCloud API key
- **Response Format**: GeoJSON with climate values and geometries
- **Rate Limits**: 10,000 features per request (handled via month-by-month processing)
- **Timeout Configuration**: 600s total, 60s connect, 300s read

### Spatial Processing Features
- **Grid Resolution**: 10km spatial resolution
- **Coverage**: Complete Danish territory
- **CRS Transformation**: EPSG:25832 → EPSG:4326 using DuckDB-spatial
- **Geometry Processing**: Point geometries with spatial aggregation
- **Statistical Analysis**: Monthly averages, ranges, and spatial summaries

### Quality Assurance Features
- **API Response Validation**: Verify GeoJSON structure and feature presence
- **Temporal Completeness**: Month-by-month processing ensures no gaps
- **Spatial Validation**: Coordinate transformation verification
- **Statistical Validation**: Climate value range checking and outlier detection

</details>

---

## Contact and Support

### Workflow Maintainer
- **Primary Contact**: Climate Data Team
- **Response Time**: 2-3 business days

### Reporting Issues
- **Data Quality Issues**: Report via GitHub issues with "DMI" label
- **API Access Problems**: Contact system administrators for DMI GovCloud API issues
- **Processing Problems**: Submit technical issues via project channels

### Documentation Updates
- **Last Updated**: January 2025
- **Update Schedule**: Reviewed when DMI API changes or processing requirements evolve
- **Version**: 1.0

---

*This documentation is part of the Landbruget.dk transparency initiative to make agricultural climate data accessible and trustworthy.*

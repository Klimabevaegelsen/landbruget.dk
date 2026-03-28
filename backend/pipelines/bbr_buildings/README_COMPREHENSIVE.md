# BBR Buildings Pipeline: Danish Building Registry and Spatial Analysis

> **Monthly Processing**: Comprehensive collection and processing of Danish building data for agricultural infrastructure analysis and pesticide exposure risk assessment

---

## What This Pipeline Does

The BBR Buildings Pipeline is a sophisticated geospatial data processing system that collects, processes, and analyzes Danish building data from the national building registry (Bygnings- og Boligregistret - BBR). The pipeline combines multiple authoritative data sources including INSPIRE standardized building data, GeoDanmark vector services, and detailed BBR classification codes to create comprehensive building datasets for agricultural analysis and public health research. Using advanced spatial processing techniques and performance optimizations, it identifies and categorizes buildings critical for pesticide exposure risk assessment, agricultural infrastructure analysis, and proximity-based safety evaluations.

### Why This Data Matters
- **Pesticide Exposure Risk Assessment**: Identification of schools, daycare centers, and residential buildings near agricultural fields for health impact analysis
- **Agricultural Infrastructure Analysis**: Comprehensive mapping of agricultural buildings, greenhouses, and livestock facilities
- **Public Health Protection**: Spatial analysis capabilities for assessing proximity between agricultural activities and sensitive populations
- **Policy Development**: Data-driven insights for agricultural zoning and safety regulation development
- **Research Applications**: High-quality building data for academic research in agricultural geography and public health

### Key Technical Statistics
- **Data Volume**: 5.56+ million building records covering all of Denmark
- **Data Sources Integration**: 3 major Danish government data sources with advanced cross-referencing
- **Spatial Processing**: DuckDB Spatial v1.2.2 SPATIAL_JOIN operator for high-performance geospatial operations
- **Classification System**: 4 primary building categories with detailed BBR usage codes (210-series agricultural, 110-540 residential, 420-441 educational)
- **Performance Optimization**: UUID-based joins achieving 63.9% match rate with optimized memory management
- **Update Frequency**: Monthly automated processing with comprehensive data quality validation

---

## Data Sources and Dependencies

### Primary Data Source: INSPIRE BBR Building Data
This pipeline integrates with Denmark's official building registry through INSPIRE-standardized data:

| Data Source | Provider | Format | Content | Volume |
|-------------|----------|--------|---------|---------|
| **DK_INSPIRE_BBR.gpkg** | SDFE (Styrelsen for Dataforsyning og Infrastruktur) | GeoPackage | Complete BBR building data with INSPIRE standardization | 5.56M+ records (~761MB) |
| **Building Layer** | SDFE | Vector Polygons | Building footprints with usage classifications | Primary dataset |
| **OtherConstruction Layer** | SDFE | Vector Polygons | Technical installations and auxiliary structures | Supplementary dataset |

### Supplementary Data Source: GeoDanmark WFS Service
Advanced building cross-referencing through Denmark's national vector service:

| Service Component | Endpoint | Authentication | Purpose | Technical Details |
|-------------------|----------|----------------|---------|-------------------|
| **GeoDanmark WFS** | `https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS` | Datafordeler Username/Password | Building footprint cross-reference | WFS 2.0.0 protocol |
| **gdk60:Bygning** | GeoDanmark WFS | Authenticated | Building geometries with BBRUUID links | Vector polygon features |
| **gdk60:TekniskAnlaegFlade** | GeoDanmark WFS | Authenticated | Technical installation areas | BBR-registered facilities |
| **gdk60:TekniskAnlaegPunkt** | GeoDanmark WFS | Authenticated | Technical installation points | Precise location data |

### Enhanced Data Source: GraphQL BBR API
Detailed building classification through Denmark's official GraphQL API:

| API Component | Endpoint | Authentication | Purpose | Processing Details |
|---------------|----------|----------------|---------|-------------------|
| **Dataforsyningen GraphQL** | `https://api.dataforsyningen.dk/graphql` | API Key | Detailed BBR usage codes | Batch processing (1000 records) |
| **BBR Code Enrichment** | GraphQL API | Authenticated | Agricultural building classification | Codes 210-219 series |
| **Educational Facility Filtering** | GraphQL API | Authenticated | School/daycare identification | Codes 420-441 filtering |
| **Usage Code Validation** | GraphQL API | Authenticated | Building purpose verification | Real-time classification |

---

## Advanced Building Classification System

### Agricultural Buildings (Primary Target Category)
**Purpose**: Comprehensive identification of agricultural infrastructure for proximity analysis and regulatory compliance

| BBR Usage Code | Danish Description | English Description | Processing Strategy |
|----------------|-------------------|-------------------|-------------------|
| **210** | Bygning til landbrug, gartneri, råstofudvinding o.lign. | Buildings for agriculture, horticulture, raw material extraction | Primary agricultural classification |
| **211-219** | Specialized agricultural subcategories | Various agricultural building types | GraphQL API enrichment for detailed classification |

**INSPIRE Current Use Classifications**:
- `agriculture`: 551,113 buildings identified
- Includes: Greenhouses, livestock buildings, storage facilities, processing buildings
- **Processing Method**: Direct INSPIRE classification + GraphQL BBR code enrichment

### Residential Buildings (Exposure Risk Assessment)
**Purpose**: Identification of residential areas for pesticide exposure proximity analysis

| BBR Usage Code | Danish Description | English Description | Exposure Risk Category |
|----------------|-------------------|-------------------|----------------------|
| **110** | Stuehus til landbrugsejendom | Farmhouse on agricultural property | High (on-farm residence) |
| **120** | Fritliggende enfamiliehus (parcelhus) | Detached single-family house | Medium (suburban) |
| **130** | Række-, kæde- eller dobbelthus | Row houses, chain houses, semi-detached | Medium (suburban) |
| **140** | Etageboligbebyggelse (flerfamiliehus) | Apartment buildings (multi-family) | Low (urban) |
| **150** | Kollegium | Student housing | Medium (institutional) |
| **160** | Døgninstitution | Residential care institutions | High (vulnerable population) |
| **190** | Anden bygning til helårsbeboelse | Other year-round residential buildings | Variable |
| **510** | Sommerhus | Summer houses | Low (seasonal) |
| **540** | Kolonihavehus | Allotment garden houses | Low (recreational) |

**INSPIRE Current Use Statistics**:
- `individualResidence`: 1,813,391 buildings
- `collectiveResidence`: 100,875 buildings  
- `twoDwellings`: 32,299 buildings
- Total residential coverage: ~1.96M buildings

### Educational and Childcare Facilities (Critical Protection Category)
**Purpose**: Identification of locations with children present for enhanced pesticide protection measures

| BBR Usage Code | Danish Description | English Description | Protection Priority |
|----------------|-------------------|-------------------|-------------------|
| **420** | Bygning til undervisning og forskning (grundskoler) | Primary education buildings | Critical (children 6-16) |
| **421** | Bygning til undervisning og forskning (gymnasier) | Secondary education buildings | High (youth 16-19) |
| **422** | Bygning til undervisning og forskning (universiteter) | Higher education buildings | Medium (adults) |
| **429** | Anden undervisningsbygning | Other educational buildings | Variable |
| **440** | Bygning til daginstitutioner for børn | Daycare centers for children | Critical (children 0-6) |
| **441** | Anden bygning til daginstitution | Other childcare facilities | Critical (children 0-6) |

**Processing Strategy**: 
- Initial identification via INSPIRE `publicServices` classification (60,454 buildings)
- GraphQL API enrichment for precise BBR code filtering
- Educational facility filtering (codes 420-441 only)
- Enhanced protection zone mapping for pesticide applications

---

## Data Processing Steps

### 🥉 Bronze Layer: Multi-Source Data Collection and Integration
**What happens**: We systematically collect building data from multiple Danish government sources, performing complex authentication, bulk downloads, and metadata preservation
**Why**: Danish building data is distributed across multiple authoritative sources requiring sophisticated integration protocols for comprehensive coverage

**Specific processing**:

#### INSPIRE BBR Data Collection
- **Dynamic URL Parsing**: Automated parsing of SDFE FTP pages to extract current download URLs for INSPIRE BBR data
- **Bulk Download Management**: Download of complete DK_INSPIRE_BBR.zip file (~761MB) with progress tracking and integrity verification
- **GeoPackage Processing**: Extraction and processing of both building and otherConstruction layers using DuckDB spatial operations
- **Metadata Extraction**: Comprehensive metadata capture including file timestamps, checksums, and source provenance
- **Memory Optimization**: Streaming processing to minimize disk usage and memory footprint during large file operations

#### GeoDanmark WFS Integration
- **Bulk WFS Processing**: Paginated download of complete building dataset using WFS 2.0.0 protocol with concurrent request management
- **Authentication Management**: Secure handling of Datafordeler credentials with session management and retry logic
- **BBRUUID Cross-referencing**: Collection of building UUIDs for precise cross-referencing with INSPIRE BBR data
- **Spatial Data Optimization**: Processing in EPSG:4326 for consistency with downstream spatial analysis requirements
- **Performance Monitoring**: Real-time monitoring of download progress with configurable batch sizes (30,000 features per batch)

#### GraphQL API Enrichment
- **Batch Processing**: Efficient batch processing of building UUIDs through Dataforsyningen GraphQL API (1000 records per batch)
- **BBR Code Enrichment**: Detailed BBR usage code collection for agricultural and public service buildings
- **Educational Facility Filtering**: Precise filtering of public service buildings to identify only educational facilities (codes 420-441)
- **Retry Logic**: Robust error handling with exponential backoff for API rate limiting and network issues
- **Data Quality Validation**: Comprehensive validation of GraphQL responses and BBR code consistency

### 🥈 Silver Layer: Advanced Spatial Processing and Building Classification
**What happens**: We apply sophisticated spatial processing techniques, perform building classification, and create optimized datasets for proximity analysis and agricultural research
**Why**: Building data requires complex spatial processing, classification standardization, and performance optimization for large-scale geospatial analysis

**Specific processing**:

#### High-Performance Spatial Processing
- **DuckDB Spatial SPATIAL_JOIN Operator**: Utilization of DuckDB Spatial v1.2.2 SPATIAL_JOIN operator for optimized spatial operations
- **UUID-Based Join Optimization**: High-performance UUID matching between INSPIRE BBR and GeoDanmark data (63.9% match rate)
- **Memory Management**: Advanced memory optimization with configurable limits (12GB DuckDB memory limit) and garbage collection
- **Geometry Validation**: Comprehensive geometry validation and repair using `ST_IsValid` and spatial indexing
- **Coordinate System Standardization**: Consistent EPSG:4326 projection for all spatial outputs with coordinate transformation validation

#### Advanced Building Classification
- **Multi-Source Classification**: Integration of INSPIRE currentUse values with detailed BBR usage codes from GraphQL API
- **Category Standardization**: Standardized building categories (residential, agricultural, education, daycare) with consistent naming conventions
- **Agricultural Building Processing**: Complete processing of all agricultural buildings with detailed BBR code enrichment
- **Educational Facility Filtering**: Precise filtering of public service buildings to retain only educational and childcare facilities
- **Residential Building Optimization**: Efficient processing of residential buildings with exposure risk categorization

#### Spatial Analysis Optimization
- **Chunked Processing**: Configurable spatial chunk processing (25,000 records per chunk) for memory efficiency
- **Building Area Filtering**: Minimum area thresholds (5.0 m² building area, 1.0 m² geometry area) for data quality
- **Centroid Calculation**: Automated calculation of building centroids using `ST_Centroid` for point-based proximity analysis
- **GitHub Actions Optimization**: Specialized processing modes for GitHub Actions environment constraints
- **Performance Monitoring**: Real-time monitoring of processing performance with memory usage tracking

---

## Technical Implementation Details

### Advanced Spatial Processing Architecture

#### DuckDB Spatial Integration
```sql
-- High-performance UUID-based join optimization
CREATE OR REPLACE TABLE building_matches AS
SELECT 
    i.BBRUUID,
    i.geometry,
    i.current_use,
    g.bygningstype,
    g.building_area_m2
FROM inspire_buildings i
JOIN geodanmark_buildings g ON i.BBRUUID = g.bbruuid
WHERE ST_IsValid(i.geometry)
AND i.building_area_m2 > 5.0;
```

#### Spatial Performance Optimization
- **SPATIAL_JOIN Operator**: Advanced spatial join optimization for large-scale geospatial operations
- **Memory Limit Configuration**: `DUCKDB_MEMORY_LIMIT=12GB` with build-side memory limits for spatial joins
- **ST_Dump Optimization**: Geometry decomposition optimization for complex polygon processing
- **Spatial Indexing**: Automatic spatial indexing for improved query performance

### Building Classification Logic

#### Multi-Stage Classification Process
```python
def classify_building(current_use: str, bbr_code: int = None) -> str:
    """Advanced building classification with multi-source integration."""
    if current_use in ['individualResidence', 'collectiveResidence', 'twoDwellings']:
        return 'residential'
    elif current_use == 'agriculture':
        return 'agricultural'
    elif current_use == 'publicServices' and bbr_code in [420, 421, 422, 429, 440, 441]:
        return 'education' if bbr_code in [420, 421, 422, 429] else 'daycare'
    else:
        return 'other'
```

#### GraphQL API Integration
- **Batch Processing**: Efficient batch processing with configurable batch sizes (1000 records)
- **Error Handling**: Comprehensive error handling with retry logic and fallback strategies
- **Rate Limiting**: Automatic rate limiting compliance with exponential backoff
- **Data Validation**: Real-time validation of GraphQL responses and BBR code consistency

### Performance and Scalability Features

#### Memory Management and Optimization
- **Chunked Processing**: Configurable chunk sizes (25,000 records) for large dataset processing
- **Memory Cleanup**: Automated garbage collection with configurable frequency (every 5 chunks)
- **Resource Monitoring**: Real-time monitoring of memory and disk usage with warning thresholds
- **GitHub Actions Compatibility**: Specialized optimization for CI/CD environment constraints

#### Data Quality and Validation
- **Geometry Validation**: Comprehensive validation using `ST_IsValid` with automatic repair capabilities
- **Area Filtering**: Minimum area thresholds to exclude invalid or irrelevant geometries
- **Cross-Reference Validation**: Validation of UUID matches between data sources
- **Statistical Reporting**: Detailed processing statistics and data quality metrics

---

## Output Data Products and Schema

### Comprehensive Building Dataset Schema
```sql
CREATE TABLE processed_buildings (
    building_uuid STRING,                    -- BBR UUID for cross-referencing
    geo_building_polygon GEOMETRY,          -- Building footprint (EPSG:4326)
    geo_building_centroid GEOMETRY,         -- Building centroid (EPSG:4326)
    building_type STRING,                   -- GeoDanmark building type
    building_floor_area_sqm FLOAT,          -- Floor area in square meters
    building_usage_category STRING,         -- Standardized category (residential/agricultural/education/daycare)
    inspire_current_use STRING,             -- Original INSPIRE classification
    inspire_building_nature STRING,         -- INSPIRE building nature
    inspire_construction_year INTEGER,      -- Construction year from INSPIRE
    inspire_floor_area FLOAT,              -- INSPIRE floor area
    inspire_floors INTEGER,                 -- Number of floors
    inspire_dwellings INTEGER,              -- Number of dwelling units
    address_full STRING,                    -- Complete building address
    bbr_usage_code INTEGER,                 -- Detailed BBR usage code (from GraphQL)
    join_status STRING,                     -- UUID join success status
    last_updated DATE                       -- Processing timestamp
);
```

### Data Quality Statistics
- **Total Buildings Processed**: 5.56M+ records
- **Agricultural Buildings**: ~551K buildings (agriculture currentUse)
- **Residential Buildings**: ~1.96M buildings (various residence types)
- **Educational/Childcare Facilities**: Filtered subset of 60K+ public service buildings
- **UUID Match Rate**: 63.9% successful matches between INSPIRE BBR and GeoDanmark
- **Spatial Coverage**: Complete Denmark with EPSG:4326 projection

---

## Usage and Configuration

### Command-Line Interface
```bash
# Complete pipeline execution (Bronze + Silver)
python main.py --layer both --log-level INFO

# Bronze layer only (data collection)
python main.py --layer bronze --source inspire_bbr --output-dir data/bronze

# Silver layer only (processing existing bronze data)
python main.py --layer silver --input-dir data/bronze/20241201_120000 --output-dir data/silver

# Sample processing for development
python main.py --sample-size 10000 --log-level DEBUG

# Enhanced classification with GraphQL enrichment
python main.py --layer silver --enhance-classification --log-level INFO
```

### Environment Configuration
```bash
# Data Source Configuration
INSPIRE_BBR_URL=https://ftp.dataforsyningen.dk/Bygninger_og_Adresser/Bygninger_og_Adresser_INSPIRE/DK_INSPIRE_BBR.zip
GEODANMARK_WFS_URL=https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS
GRAPHQL_ENDPOINT=https://api.dataforsyningen.dk/graphql

# Authentication
DATAFORDELER_USERNAME=your_username
DATAFORDELER_PASSWORD=your_password
DATAFORDELER_GRAPHQL_API_KEY=your_api_key

# Processing Optimization
DUCKDB_MEMORY_LIMIT=12GB
SPATIAL_CHUNK_SIZE=25000
SPATIAL_JOIN_BUILD_SIDE_MEMORY_LIMIT=8GB
ENABLE_SPATIAL_JOIN_OPERATOR=true

# Storage Configuration
STORAGE_BUCKET=landbruget-data
GOOGLE_APPLICATION_CREDENTIALS=path/to/credentials.json
```

### Docker Deployment
```bash
# Build and run with optimized configuration
docker-compose build
docker-compose up

# Production deployment with resource limits
docker run --memory=16g --cpus=4 bbr-buildings-pipeline
```

---

## Integration and Downstream Applications

### Pesticide Proximity Analysis Integration
- **Spatial Buffer Analysis**: Building datasets optimized for proximity analysis with agricultural fields
- **Educational Facility Protection**: Enhanced protection zones around schools and daycare centers
- **Residential Exposure Assessment**: Risk categorization based on building types and agricultural proximity
- **Policy Compliance Monitoring**: Automated compliance checking for pesticide application restrictions

### Agricultural Infrastructure Analysis
- **Farm Building Inventory**: Comprehensive mapping of agricultural infrastructure
- **Greenhouse Identification**: Specialized processing for horticultural facilities
- **Livestock Building Analysis**: Identification and classification of animal housing facilities
- **Agricultural Zoning Support**: Data products for agricultural land use planning

### Data Integration Capabilities
- **CVR Number Integration**: Cross-referencing with company registration data for farm-level analysis
- **Field Boundary Integration**: Spatial relationships with agricultural field boundaries
- **Address Geocoding**: Integration with Danish address data for precise location services
- **Temporal Analysis**: Historical building data processing for trend analysis

This pipeline serves as a critical component of Denmark's agricultural spatial analysis infrastructure, providing comprehensive building data for pesticide exposure risk assessment, agricultural infrastructure analysis, and evidence-based policy development through advanced geospatial processing and multi-source data integration.

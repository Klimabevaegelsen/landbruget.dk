# BBR Buildings Pipeline

This pipeline fetches and processes Danish building data from Bygnings- og Boligregistret (BBR) to support agricultural and public health analyses. The pipeline identifies buildings related to agricultural practices and locations where children are present (schools, daycare centers) for pesticide exposure risk assessments.

## Data Sources

### Primary Source: DK_INSPIRE_BBR.gpkg
- **Provider**: SDFE (Styrelsen for Dataforsyning og Infrastruktur)
- **Format**: GeoPackage with building and otherConstruction layers (5.56M+ records)
- **Content**: Complete BBR building data with INSPIRE standardization, including agricultural structures
- **Update Frequency**: Static file, manual updates available
- **Size**: ~761MB total (both building and otherConstruction layers loaded)

### Supplementary Source: GeoDanmark WFS
- **Provider**: Datafordeleren GeoDanmark Vector Service  
- **Endpoint**: `https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS`
- **Authentication**: Username/password (existing DATAFORDELER credentials)
- **Key Feature Types**:
  - `gdk60:Bygning` - Building footprints with BBRUUID links
  - `gdk60:TekniskAnlaegFlade` - Technical installations (BBR-registered)
  - `gdk60:TekniskAnlaegPunkt` - Technical installation points (BBR-registered)
- **Purpose**: Cross-reference for enhanced building classification

## Target Building Categories

### Agricultural Buildings
- **Purpose**: Identify buildings related to agricultural practices
- **BBR Usage Codes**: 
  - 210: Bygning til landbrug, gartneri, råstofudvinding o.lign.
- **Current Use Values** (INSPIRE data):
  - `agriculture`: 551,113 buildings
  - Includes: greenhouses, storage tanks, livestock buildings

### Dwellings
- **Purpose**: Identify residential areas for pesticide exposure analysis
- **BBR Usage Codes**:
  - 110: Stuehus til landbrugsejendom
  - 120: Fritliggende enfamiliehus (parcelhus)
  - 130: Række-, kæde- eller dobbelthus
  - 140: Etageboligbebyggelse (flerfamiliehus)
  - 150: Kollegium
  - 160: Døgninstitution
  - 190: Anden bygning til helårsbeboelse
  - 510: Sommerhus
  - 540: Kolonihavehus
- **Current Use Values** (INSPIRE data):
  - `individualResidence`: 1,813,391 buildings
  - `collectiveResidence`: 100,875 buildings
  - `twoDwellings`: 32,299 buildings
  - Other residential types: ~18,000 buildings

### Places Where Children Are Present
- **Purpose**: Identify schools, daycare centers for pesticide risk assessment
- **BBR Usage Codes**:
  - 160: Døgninstitution (some housing children)
  - 420: Bygning til undervisning og forskning (schools)
  - 440: Bygning til daginstitutioner for børn og unge (daycare)
- **Current Use Values** (INSPIRE data):
  - `publicServices`: 60,454 buildings (requires further classification)
- **Strategy**: Use GeoDanmark WFS BBRUUID cross-reference for precise identification

## Pipeline Architecture

### Bronze Layer
**Objective**: Fetch and store raw building data monthly

**Data Sources**:
1. **Primary**: DK_INSPIRE_BBR.gpkg building and otherConstruction layers
   - Download complete dataset (~761MB file, both layers)
   - Load both building and otherConstruction layers for comprehensive coverage
   - Store raw GeoPackage data without transformation
   - Extract metadata: file size, timestamp, source URL

2. **Supplementary**: GeoDanmark WFS samples
   - Fetch sample data from building-related feature types
   - Store WFS capabilities and sample responses
   - Maintain BBRUUID mapping data

**Storage**:
- **Production**: Google Cloud Storage
- **Development**: Local Parquet/GeoParquet files
- **Structure**: `bronze/YYYYMMDD_HHMMSS/`

### Silver Layer
**Objective**: Clean, harmonize, and filter building data

**Processing Steps**:
1. **Data Loading**: Load both GeoPackage building and otherConstruction layers using ibis-framework/duckdb
2. **Filtering**: Apply building usage type filters
3. **Geospatial Processing**:
   - Validate geometries
   - Ensure EPSG:4326 projection
   - Calculate building centroids for point analysis
4. **Data Harmonization**:
   - Standardize field names per project conventions
   - Cast data types (dates, numbers, booleans)
   - Handle null values consistently
5. **Enhanced Classification** (Optional):
   - Cross-reference with GeoDanmark WFS BBRUUID data
   - Refine school vs daycare classification
   - Add technical installation data

**Output Schema**:
```
geo_building_polygon: GEOMETRY (Polygon, EPSG:4326)
geo_building_centroid: GEOMETRY (Point, EPSG:4326)  
building_construction_year: INTEGER
building_usage_code: STRING
building_usage_category: STRING  # 'agricultural', 'residential', 'educational', 'daycare'
building_floor_area_sqm: FLOAT
building_floors_above_ground: INTEGER
building_dwellings_count: INTEGER
parcel_id: STRING
address_full: STRING
bbr_uuid: STRING
last_updated: DATE
```

**Storage**:
- **Production**: GCS as GeoParquet
- **Development**: Local GeoParquet files
- **Structure**: `silver/YYYYMMDD/buildings_filtered.parquet`

## Implementation Details

### Dependencies
```toml
[dependencies]
geopandas = ">=0.14.0"
duckdb = ">=0.9.0"
ibis-framework = {version = ">=6.0.0", extras = ["duckdb", "geospatial"]}
requests = ">=2.31.0"
pyogrio = ">=0.7.0"  # Fast GDAL I/O
google-cloud-storage = ">=2.10.0"
```

### Key Processing Functions

#### Bronze Layer
```python
def download_inspire_bbr_data(output_dir: Path) -> None:
    """Download DK_INSPIRE_BBR.gpkg building layer"""
    # Download from SDFE FTP or direct URL
    # Extract building layer only
    # Save with metadata

def fetch_geodanmark_samples(credentials: dict, output_dir: Path) -> None:
    """Fetch WFS samples for BBRUUID mapping"""
    # Query gdk60:Bygning, gdk60:TekniskAnlaegFlade, gdk60:TekniskAnlaegPunkt
    # Save sample data for cross-referencing
```

#### Silver Layer
```python
def load_and_filter_buildings(input_path: Path) -> ibis.Table:
    """Load buildings and apply usage filters"""
    # Load with ibis/duckdb for performance
    # Filter by currentUse values
    # Validate geometries

def standardize_schema(buildings: ibis.Table) -> ibis.Table:
    """Apply project naming conventions and data types"""
    # Rename fields per project standards
    # Cast types, handle nulls
    # Add derived fields

def enhance_classification(buildings: ibis.Table, wfs_data: dict) -> ibis.Table:
    """Cross-reference with GeoDanmark WFS for detailed classification"""
    # Match BBR UUIDs
    # Refine educational facility classification
    # Add technical installation context
```

### Configuration

**Environment Variables**:
```env
# Data Source Configuration
INSPIRE_BBR_URL=https://ftp.sdfe.dk/main.html?download&weblink=ca102693c712ad4159e4a6f343da60d5
GEODANMARK_WFS_URL=https://wfs.datafordeler.dk/GeoDanmarkVektor/GeoDanmark60_NOHIST_GML3/1.0.0/WFS

# Datafordeleren API credentials (inherited from existing setup)
DATAFORDELER_USERNAME=your_username  
DATAFORDELER_PASSWORD=your_password

# Storage Configuration
OUTPUT_BUCKET=your-gcs-bucket
ENVIRONMENT=dev

# Processing Configuration  
MAX_WORKERS=4
CHUNK_SIZE=50000
```

**Command Line Arguments**:
```bash
python main.py \
  --layer bronze \
  --source inspire_bbr \
  --output-dir data/bronze \
  --log-level INFO

python main.py \
  --layer silver \
  --input-dir data/bronze/20241201_120000 \
  --output-dir data/silver \
  --enhance-classification \
  --log-level INFO
```

## Data Quality and Validation

### Bronze Layer Validation
- Verify file download completeness
- Check GeoPackage integrity 
- Validate layer existence and record count
- Confirm spatial reference system

### Silver Layer Validation
- Geometry validation and repair
- Data type consistency checks
- Building category coverage verification
- Address and parcel ID completeness assessment
- Cross-reference validation against known building counts

### Expected Outputs
- **Total Buildings**: ~5.56M records
- **Agricultural**: ~551K buildings
- **Residential**: ~1.96M buildings  
- **Public Services**: ~60K buildings (requires sub-classification)
- **Coverage**: Complete Denmark
- **Coordinate System**: EPSG:4326

## Testing

### Unit Tests
```python
def test_building_filter():
    """Test building usage type filtering"""
    
def test_schema_standardization():
    """Test field naming and type conversion"""
    
def test_geometry_validation():
    """Test spatial data processing"""
```

### Integration Tests
```python
def test_bronze_to_silver_pipeline():
    """Test complete pipeline with sample data"""
    
def test_wfs_cross_reference():
    """Test GeoDanmark WFS integration"""
```

### Local Testing
```bash
# Test with sample data
python main.py --layer bronze --source inspire_bbr --sample-size 1000

# Test Docker container
docker-compose up --build

# Test GitHub Actions workflow locally
act workflow_dispatch -W .github/workflows/bbr_buildings.yml -n
```

## Deployment

### GitHub Actions Workflow
- **Trigger**: Monthly schedule + manual dispatch
- **Runner**: Ubuntu latest with 16GB RAM
- **Duration**: ~2-3 hours for complete pipeline
- **Artifacts**: Processed building data in GCS

### Monitoring
- Pipeline execution logs via GitHub Actions
- Data quality metrics in pipeline output
- Storage usage monitoring in GCS
- Error alerting via workflow notifications

## Known Issues and Solutions

### Performance Considerations
- **Large Dataset**: 5.56M records require chunked processing
- **Memory Usage**: Use ibis/duckdb streaming for large operations
- **Geometry Processing**: Leverage spatial indexing for performance

### Data Quality Issues
- **Missing Construction Dates**: Some records lack date information
- **Usage Classification**: Public services require additional classification steps
- **Geometry Validity**: Some building polygons may need repair

### Update Strategy
- **Static Source**: Manual monitoring for INSPIRE BBR updates
- **Incremental Processing**: Track changes via file timestamps
- **WFS Monitoring**: Regular checks for GeoDanmark WFS schema changes

## Related Issues

- **GitHub Issue #270**: Get buildings data (this pipeline)
- **GitHub Issue #271**: Calculate distance from fields with pesticide usage to buildings

## Contributing

1. Follow project medallion architecture guidelines
2. Use ibis-framework/duckdb for data processing
3. Ensure EPSG:4326 for all geospatial outputs
4. Include comprehensive logging and error handling
5. Test locally before submitting PRs
6. Document any changes to building classification logic

## References

- [BBR Documentation (Datafordeleren)](https://datafordeler.dk/dataoversigt/bygnings-og-boligregistret-bbr/bbr/)
- [GeoDanmark WFS Documentation](https://datafordeler.dk/dataoversigt/)
- [Building Usage Codes (Danmarks Statistik)](https://www.dst.dk/da/Statistik/dokumentation/Times/boligtaelling/bygningsanvendelse)
- [Project Medallion Architecture](../README.md) 
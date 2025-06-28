# Field Production Pipeline Migration to Gold Layer

## Overview

This document outlines the migration plan to move the `field_production_pipeline` from a standalone pipeline to the unified pipeline's **gold layer**. The field production pipeline combines agricultural fields data with DST (Danish Statistics) yield data to create comprehensive production estimates, making it a perfect fit for the gold layer's purpose of creating business-ready analytics datasets.

## Current State Analysis

### Existing Field Production Pipeline
- **Location**: `backend/pipelines/field_production_pipeline/`
- **Purpose**: Generate comprehensive field production estimates by combining agricultural fields with DST yield data
- **Architecture**: Standalone pipeline with optimized DuckDB Spatial v1.2.2 processing
- **Key Features**:
  - Spatial joins with DST zones for regional yield estimates
  - Optimized batch processing (5000 fields per batch)
  - Smart single-zone vs multi-zone field processing
  - Production estimates in hectograms (hkg)

### Dependencies
- **Silver Layer Inputs**:
  - `agricultural_fields`: Field geometry, crop types, areas
  - `dst_pipeline`: Regional yield data from HST77, GARTN1, FRO, HALM1 tables
  - `dst_zone_mapping`: Spatial zones for regional calculations

## Migration Architecture

### Gold Layer Integration Pattern

Following the `PropertyCadastralMergeGold` pattern:

```python
class FieldProductionGold(BaseSource[FieldProductionGoldConfig], GoldJobInterface):
    """
    Gold layer processor for field production estimates.
    
    Combines agricultural fields and DST yield data to create
    comprehensive production estimates for analytics and downstream consumption.
    """
```

### Configuration Model

```python
class FieldProductionGoldConfig(BaseJobConfig):
    """Configuration for Field Production gold layer."""
    
    name: str = "Field Production Gold"
    dataset: str = "field_production"
    type: str = "gold"
    description: str = "Comprehensive field production estimates using DST yield data"
    frequency: str = "weekly"
    bucket: str = os.getenv("GCS_BUCKET")
    
    # Input silver datasets
    agricultural_fields_dataset: str = "agricultural_fields"
    dst_zone_mapping_dataset: str = "dst_zone_mapping"
    
    # Processing configuration
    batch_size: int = 5000  # Optimized for SPATIAL_JOIN performance
    max_year_lag: int = 3   # Maximum years between field and DST data
    
    # DST data sources (local cache paths)
    dst_cache_dir: str = "data_cache/dst_pipeline"
    dst_tables: List[str] = ["HST77", "GARTN1", "FRO", "HALM1"]
    
    # Quality thresholds
    min_yield_coverage: float = 0.3  # Minimum acceptable yield coverage rate
    
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)
```

## Implementation Plan

### Phase 1: Create Gold Layer Structure

#### 1.1 Create Gold Layer Module
```bash
# Create the new gold layer file
touch backend/pipelines/unified_pipeline/src/unified_pipeline/gold/field_production.py
```

#### 1.2 Update CLI Configuration
Add field production to the unified pipeline CLI:

```python
# In unified_pipeline/model/cli.py
class Source(Enum):
    # ... existing sources ...
    field_production = "field_production"
```

#### 1.3 Register in App Configuration
```python
# In unified_pipeline/app.py
from unified_pipeline.gold.field_production import (
    FieldProductionGold,
    FieldProductionGoldConfig,
)

# Add to pipeline_map
cli.Source.field_production: {
    cli.Stage.gold: [(FieldProductionGold, FieldProductionGoldConfig)],
},
```

### Phase 2: Implement Gold Layer Class

#### 2.1 Core Gold Layer Implementation

```python
class FieldProductionGold(BaseSource[FieldProductionGoldConfig], GoldJobInterface):
    """Gold layer processor for field production estimates."""
    
    def __init__(self, config: FieldProductionGoldConfig, gcs_util: GCSUtil):
        super().__init__(config, gcs_util)
        
        # Configure optimized DuckDB for spatial operations
        self.conn.execute("SET memory_limit = '8GB'")
        self.conn.execute("SET threads = 4")
        self.conn.execute("INSTALL spatial")
        self.conn.execute("LOAD spatial")
        
        # Initialize DST data and yield estimator
        self.dst_data = self._load_dst_data()
        self.yield_estimator = self._setup_yield_estimator()
    
    async def run(self, silver_data: Optional[Dict[str, Any]] = None) -> None:
        """Run field production estimation gold processing."""
        
        # Load required silver datasets
        agricultural_fields = self._load_silver_data(
            self.config.agricultural_fields_dataset, silver_data
        )
        dst_zone_mapping = self._load_silver_data(
            self.config.dst_zone_mapping_dataset, silver_data
        )
        
        if agricultural_fields is None:
            self.log.error("No agricultural fields data available")
            return
            
        # Process field production estimates
        production_estimates = self._process_field_production(
            agricultural_fields, dst_zone_mapping
        )
        
        # Save to gold layer
        self._save_data(
            production_estimates, 
            self.config.dataset, 
            self.config.bucket, 
            stage="gold"
        )
```

#### 2.2 Spatial Processing Integration

Migrate the optimized DuckDB spatial processing:

```python
def _setup_optimized_spatial_connection(self, dst_zone_mapping: gpd.GeoDataFrame):
    """Setup optimized DuckDB with spatial zones for SPATIAL_JOIN."""
    
    # Create optimized dst_zones table
    self.conn.execute("""
        CREATE TABLE dst_zones_optimized (
            zone_id INTEGER,
            zone_name VARCHAR,
            geometry GEOMETRY,
            bbox_minx DOUBLE,
            bbox_miny DOUBLE, 
            bbox_maxx DOUBLE,
            bbox_maxy DOUBLE
        )
    """)
    
    # Insert zone data with WKB geometries
    # ... (migrate existing spatial optimization code)

def _process_batch_spatial_yields(self, fields_batch: List[Dict], year: int) -> Dict[str, Dict]:
    """Process field yields using optimized SPATIAL_JOIN operator."""
    
    # Create temporary fields table
    self.conn.execute("DROP TABLE IF EXISTS temp_fields_batch")
    # ... (migrate existing batch processing logic)
    
    # Execute optimized spatial join
    spatial_join_query = """
    SELECT f.field_id, f.crop_type, f.area_ha, z.zone_name
    FROM temp_fields_batch f
    INNER JOIN dst_zones_optimized z 
        ON ST_Intersects(f.geometry, z.geometry)
    """
    
    # ... (migrate yield calculation logic)
```

### Phase 3: Data Integration

#### 3.1 Silver Data Loading

```python
def _load_silver_data(self, dataset: str, silver_data: Optional[Dict[str, Any]]) -> Optional[Any]:
    """Load silver data with fallback to storage."""
    
    if silver_data and dataset in silver_data:
        self.log.info(f"Using in-memory silver data for {dataset}")
        return silver_data[dataset]
    
    # Fallback to storage
    self.log.info(f"Reading {dataset} from GCS storage")
    return self._read_data_from_storage(dataset, self.config.bucket, stage="silver")
```

#### 3.2 DST Data Integration

```python
def _load_dst_data(self) -> Dict[str, pd.DataFrame]:
    """Load DST data from local cache."""
    
    dst_data = {}
    for table_name in self.config.dst_tables:
        try:
            table_path = Path(self.config.dst_cache_dir) / f"{table_name.lower()}_processed.parquet"
            if table_path.exists():
                dst_data[table_name] = pd.read_parquet(table_path)
                self.log.info(f"Loaded {table_name}: {len(dst_data[table_name])} records")
            else:
                self.log.warning(f"DST table {table_name} not found in cache")
                dst_data[table_name] = pd.DataFrame()
        except Exception as e:
            self.log.error(f"Error loading {table_name}: {e}")
            dst_data[table_name] = pd.DataFrame()
    
    return dst_data
```

### Phase 4: Output Schema Normalization

#### 4.1 Normalized Gold Output

Following the proposed normalized schema from `proposed_schema.md`:

```python
def _create_production_estimates(self, fields_gdf: gpd.GeoDataFrame, yields: Dict[str, Dict]) -> pd.DataFrame:
    """Create normalized field production estimates."""
    
    production_data = []
    
    for _, field in fields_gdf.iterrows():
        field_id = field['field_id']
        block_id = field['block_id']
        
        # Get yield estimate for this field
        yield_info = yields.get(f"{field_id}_{block_id}")
        
        if yield_info:
            production_estimate = {
                # JOIN KEYS
                "field_id": field_id,
                "block_id": block_id,
                "year": field['year'],
                
                # YIELD DATA
                "yield_estimate_hkg_ha": yield_info["yield_value"],
                "yield_source_table": yield_info["source_table"],
                "yield_source_unit": yield_info["source_unit"],
                "yield_conversion_applied": yield_info["conversion_applied"],
                "production_estimate_hkg": field['area_ha'] * yield_info["yield_value"],
                "production_unit": "hkg",
                
                # DST MAPPING INFO
                "has_dst_mapping": True,
                "dst_table": yield_info["source_table"],
                "dst_category": yield_info.get("dst_category"),
                "dst_zone": yield_info.get("zone_name", "Hele landet"),
                
                # METADATA
                "estimation_method": yield_info["estimation_method"],
                "created_at": pd.Timestamp.now(),
            }
            production_data.append(production_estimate)
    
    return pd.DataFrame(production_data)
```

### Phase 5: CLI Integration and Testing

#### 5.1 CLI Usage

```bash
# Run field production gold layer
python -m unified_pipeline.app -s field_production -j gold

# Run with specific environment
python -m unified_pipeline.app -e local -s field_production -j gold
```

#### 5.2 Testing Strategy

```python
# Test script: test_field_production_gold.py
async def test_field_production_gold():
    """Test field production gold layer processing."""
    
    config = FieldProductionGoldConfig()
    gcs_util = GCSUtil(GCSConfig())
    
    processor = FieldProductionGold(config, gcs_util)
    
    # Test with mock silver data
    mock_silver_data = {
        "agricultural_fields": mock_fields_gdf,
        "dst_zone_mapping": mock_zones_gdf
    }
    
    await processor.run(silver_data=mock_silver_data)
    
    # Verify output
    assert processor.production_estimates is not None
    assert len(processor.production_estimates) > 0
```

## Migration Benefits

### 1. **Unified Architecture**
- Consistent with other gold layer processors
- Leverages unified pipeline infrastructure
- Standardized configuration and logging

### 2. **Better Data Integration**
- Direct access to silver layer data
- In-memory data passing for performance
- Consistent GCS storage patterns

### 3. **Improved Maintainability**
- Single codebase for all data processing
- Shared utilities and base classes
- Consistent error handling and monitoring

### 4. **Enhanced Scalability**
- Leverages unified pipeline's GCS integration
- Standardized batch processing patterns
- Better resource management

## Migration Timeline

### Week 1: Infrastructure Setup
- [ ] Create gold layer module structure
- [ ] Update CLI configuration
- [ ] Implement basic FieldProductionGold class

### Week 2: Core Logic Migration
- [ ] Migrate spatial processing logic
- [ ] Implement DST data integration
- [ ] Add yield calculation methods

### Week 3: Testing and Validation
- [ ] Create comprehensive test suite
- [ ] Validate output against existing pipeline
- [ ] Performance testing and optimization

### Week 4: Deployment and Cleanup
- [ ] Deploy to production environment
- [ ] Update documentation
- [ ] Archive old standalone pipeline

## Risk Mitigation

### 1. **Data Consistency**
- **Risk**: Output differences between old and new implementation
- **Mitigation**: Comprehensive validation testing with sample data

### 2. **Performance Regression**
- **Risk**: Slower processing due to unified pipeline overhead
- **Mitigation**: Maintain optimized DuckDB spatial processing, benchmark performance

### 3. **Dependency Issues**
- **Risk**: DST data cache dependencies
- **Mitigation**: Robust error handling and fallback mechanisms

### 4. **Integration Complexity**
- **Risk**: Complex integration with existing silver layer data
- **Mitigation**: Phased migration with thorough testing at each step

## Success Criteria

1. **Functional Parity**: New gold layer produces identical results to standalone pipeline
2. **Performance**: Processing time within 10% of original pipeline
3. **Integration**: Seamless operation within unified pipeline architecture
4. **Maintainability**: Reduced code duplication and improved error handling
5. **Scalability**: Better resource utilization and GCS integration

## Post-Migration Cleanup

1. **Archive Standalone Pipeline**: Move `field_production_pipeline/` to `legacy/`
2. **Update Documentation**: Update all references to use unified pipeline
3. **Update Deployment Scripts**: Remove standalone pipeline from CI/CD
4. **Clean Up Dependencies**: Remove unused dependencies from standalone pipeline

This migration transforms the field production pipeline from a standalone system into a properly integrated gold layer component, following established patterns and improving overall system architecture. 
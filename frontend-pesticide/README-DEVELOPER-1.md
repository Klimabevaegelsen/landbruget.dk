# Developer 1: Backend Infrastructure & Data Management

This document describes the backend infrastructure and data management components implemented for the H3 PFAS Visualization Frontend.

## 🏗️ **Architecture Overview**

The backend infrastructure provides:
- **Database & Schema Management**: Supabase PostgreSQL with PostGIS spatial support
- **Data Processing Pipeline**: H3 data transformation and aggregation
- **API Layer**: RESTful endpoints with streaming support
- **Data Synchronization**: GCS to Supabase pipeline integration
- **Performance Optimization**: Caching, virtualization, and spatial indexing

## 📁 **File Structure**

```
src/
├── lib/
│   ├── shared-constants.ts      # Shared configuration constants
│   ├── supabase.ts             # Supabase client and utilities
│   ├── data-processing.ts      # Core DataManager class
│   ├── data-virtualization.ts  # Performance optimization
│   └── data-syncer.ts          # GCS to Supabase synchronization
├── types/
│   ├── database.ts             # Supabase database types
│   ├── h3-data.ts             # H3 data type definitions
│   ├── bnbo-data.ts           # BNBO data type definitions
│   └── bbr-data.ts            # BBR data type definitions
└── app/api/
    ├── h3-data/route.ts       # H3 data API endpoint
    ├── bnbo-data/route.ts     # BNBO data API endpoint
    └── bbr-data/route.ts      # BBR data API endpoint
```

## 🗄️ **Database Schema**

### H3 PFAS Exposure Table
```sql
CREATE TABLE h3_pfas_exposure (
    id BIGSERIAL PRIMARY KEY,
    h3_id BIGINT NOT NULL,
    year INTEGER NOT NULL,
    total_pesticide_load DOUBLE PRECISION,  -- kg/ha equivalent
    total_pfas_grams DOUBLE PRECISION,      -- PFAS active ingredient mass
    pesticide_application_count INTEGER,
    field_count INTEGER,
    agricultural_area_ha DOUBLE PRECISION,
    avg_field_coverage DOUBLE PRECISION,
    geometry GEOMETRY(POLYGON, 4326),       -- H3 hexagon geometry
    h3_centroid GEOMETRY(POINT, 4326),      -- H3 center point
    h3_resolution INTEGER DEFAULT 10,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    CONSTRAINT unique_h3_year UNIQUE (h3_id, year)
);

-- Spatial indexes
CREATE INDEX idx_h3_pfas_geometry ON h3_pfas_exposure USING GIST (geometry);
CREATE INDEX idx_h3_pfas_centroid ON h3_pfas_exposure USING GIST (h3_centroid);
CREATE INDEX idx_h3_pfas_year ON h3_pfas_exposure (year);
CREATE INDEX idx_h3_pfas_h3_id ON h3_pfas_exposure (h3_id);
```

### BNBO Status Areas Table
```sql
CREATE TABLE bnbo_status_areas (
    id BIGSERIAL PRIMARY KEY,
    bnbo_id VARCHAR NOT NULL,
    status_code VARCHAR NOT NULL, -- 'protected', 'buffer', 'agricultural', etc.
    status_description TEXT,
    area_ha DOUBLE PRECISION,
    geometry GEOMETRY(MULTIPOLYGON, 4326),
    year INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_bnbo_geometry ON bnbo_status_areas USING GIST (geometry);
CREATE INDEX idx_bnbo_status ON bnbo_status_areas (status_code);
```

### BBR Buildings Table
```sql
CREATE TABLE bbr_buildings (
    id BIGSERIAL PRIMARY KEY,
    bbr_id VARCHAR NOT NULL,
    building_code VARCHAR,
    building_type VARCHAR, -- 'Residential', 'Agricultural', 'Industrial', etc.
    construction_year INTEGER,
    floor_area DOUBLE PRECISION,
    geometry GEOMETRY(POINT, 4326), -- Building centroid
    address TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_bbr_geometry ON bbr_buildings USING GIST (geometry);
CREATE INDEX idx_bbr_type ON bbr_buildings (building_type);
```

## 🔧 **Core Components**

### 1. DataManager Class
**File**: `src/lib/data-processing.ts`

Primary class for all data operations:

```typescript
export class DataManager {
  // Fetch H3 data with filtering and aggregation
  async fetchH3Data(year: number, cumulativeMode: boolean, filter?: H3DataFilter): Promise<H3DataPoint[]>
  
  // Fetch BNBO status areas
  async fetchBNBOData(filter?: BNBODataFilter): Promise<BNBOArea[]>
  
  // Fetch BBR buildings
  async fetchBBRData(filter?: BBRDataFilter): Promise<BBRBuilding[]>
  
  // Get data quality metrics
  async getH3DataQuality(year?: number): Promise<H3DataQuality>
}
```

**Features**:
- Intelligent caching with TTL
- Spatial filtering with PostGIS
- Data aggregation for cumulative mode
- Performance optimization
- Error handling and logging

### 2. DataVirtualizer Class
**File**: `src/lib/data-virtualization.ts`

Performance optimization through data virtualization:

```typescript
export class DataVirtualizer {
  // Filter data based on viewport
  filterH3Data(data: H3DataPoint[], viewport: ViewState): H3DataPoint[]
  filterBNBOData(data: BNBOArea[], viewport: ViewState): BNBOArea[]
  filterBBRData(data: BBRBuilding[], viewport: ViewState): BBRBuilding[]
  
  // Adaptive layer configuration
  getLayerConfigForZoom(zoom: number): LayerConfig
}
```

**Features**:
- Viewport-based filtering
- Zoom-level adaptive loading
- Performance mode adjustment
- Memory usage optimization
- Data prioritization algorithms

### 3. H3DataSyncer Class
**File**: `src/lib/data-syncer.ts`

Pipeline integration and data synchronization:

```typescript
export class H3DataSyncer {
  // Sync H3 data for specific year
  async syncH3Data(year: number): Promise<SyncResult>
  
  // Sync BNBO and BBR data
  async syncBNBOData(): Promise<SyncResult>
  async syncBBRData(): Promise<SyncResult>
  
  // Full synchronization
  async syncAllData(): Promise<{h3: SyncResult[]; bnbo: SyncResult; bbr: SyncResult}>
  
  // Data integrity validation
  async validateDataIntegrity(year: number): Promise<H3DataQuality>
}
```

**Features**:
- GCS to Supabase synchronization
- WKT to PostGIS geometry transformation
- Batch processing with conflict resolution
- Sync status tracking
- Data integrity validation

## 🚀 **API Endpoints**

### H3 Data API
**Endpoint**: `GET /api/h3-data`

**Parameters**:
- `year` (required): Data year (2020-2025)
- `cumulative` (optional): Cumulative mode flag
- `minPesticideLoad`, `maxPesticideLoad`: Pesticide filtering
- `minPfasGrams`, `maxPfasGrams`: PFAS filtering
- `bbox`: Spatial bounding box filtering
- `viewport`: Viewport-based filtering
- `stream`: Enable streaming for large datasets

**Response**:
```json
{
  "data": [...],
  "metadata": {
    "year": 2023,
    "cumulative": false,
    "totalRecords": 1250,
    "fetchDuration": 85,
    "filters": {...},
    "timestamp": "2024-01-15T10:30:00Z"
  }
}
```

### BNBO Data API
**Endpoint**: `GET /api/bnbo-data`

**Parameters**:
- `statusCodes`: Comma-separated status codes
- `minAreaHa`, `maxAreaHa`: Area filtering
- `year`: Year filtering
- `bbox`: Spatial filtering
- `viewport`: Viewport filtering

### BBR Data API
**Endpoint**: `GET /api/bbr-data`

**Parameters**:
- `buildingTypes`: Comma-separated building types
- `minConstructionYear`, `maxConstructionYear`: Year filtering
- `minFloorArea`, `maxFloorArea`: Area filtering
- `bbox`: Spatial filtering
- `proximity`: Proximity-based filtering
- `viewport`: Viewport filtering

## 🔄 **Data Flow**

### 1. Pipeline Integration
```
H3 PFAS Pipeline → GCS Storage → H3DataSyncer → Supabase → API → Frontend
```

### 2. Data Transformation
```
WKT Geometry → PostGIS Geometry → GeoJSON → Frontend Visualization
```

### 3. Caching Strategy
```
Database Query → DataManager Cache → API Response Cache → CDN Cache
```

## ⚡ **Performance Features**

### Spatial Indexing
- PostGIS GIST indexes on all geometry columns
- Optimized spatial queries with ST_Intersects
- Viewport-based filtering with buffering

### Caching Strategy
- **L1**: DataManager in-memory cache (5 minutes TTL)
- **L2**: API response cache (5-60 minutes TTL)
- **L3**: CDN cache headers for static content

### Data Virtualization
- Viewport-based data filtering
- Zoom-level adaptive loading
- Performance mode adjustment (high/medium/low)
- Data prioritization based on importance metrics

### Streaming Responses
- Large dataset streaming with NDJSON format
- Chunked data delivery (100 records per chunk)
- Progress tracking and cancellation support

## 🛠️ **Configuration**

### Environment Variables
```bash
# Supabase Configuration
NEXT_PUBLIC_SUPABASE_URL=https://your-project.supabase.co
NEXT_PUBLIC_SUPABASE_ANON_KEY=your-anon-key

# Database connection for data sync
DATABASE_URL=postgresql://postgres:password@db.your-project.supabase.co:5432/postgres

# GCS Configuration
GCS_BUCKET=landbrugsdata-raw-data
```

### Shared Constants
**File**: `src/lib/shared-constants.ts`

```typescript
export const YEARS = [2020, 2021, 2022, 2023, 2024, 2025];
export const H3_RESOLUTION = 10;
export const DEFAULT_VIEWPORT = { latitude: 56.26392, longitude: 9.501785, zoom: 7 };
export const API_ENDPOINTS = {
  H3_DATA: '/api/h3-data',
  BNBO_DATA: '/api/bnbo-data',
  BBR_DATA: '/api/bbr-data'
};
```

## 🧪 **Testing & Validation**

### Data Quality Metrics
- **Total Records**: Count of all data points
- **Geometry Coverage**: Percentage with valid geometry
- **Data Completeness**: Percentage with all required fields
- **Spatial Extent**: Geographic bounds validation
- **Temporal Coverage**: Year range validation

### Performance Benchmarks
- **API Response Time**: <500ms for typical queries
- **Database Query Time**: <200ms for spatial queries
- **Cache Hit Rate**: >80% for repeated queries
- **Memory Usage**: <100MB for typical datasets

## 🔗 **Integration Points**

### For Developer 2 (Map Visualization)
- **Data Contracts**: Standardized H3DataPoint, BNBOArea, BBRBuilding interfaces
- **API Response Formats**: Consistent JSON structures with metadata
- **Error Handling**: Standardized error response formats

### For Developer 3 (UI Components)
- **Store Integration**: Data fetching hooks and state management interfaces
- **Loading States**: Standardized loading and error state definitions
- **Cache Keys**: Predefined cache key naming conventions

## 🚀 **Deployment**

### Database Setup
1. Create Supabase project
2. Run SQL schema creation scripts
3. Configure PostGIS extension
4. Set up spatial indexes

### Environment Configuration
1. Set Supabase connection variables
2. Configure GCS access credentials
3. Set performance monitoring keys
4. Configure caching settings

### Data Synchronization
1. Run initial data sync: `H3DataSyncer.syncAllData()`
2. Set up automated sync schedule (daily at 2 AM)
3. Monitor sync status and data quality
4. Configure error alerting

## 📊 **Monitoring & Maintenance**

### Health Checks
- Database connection status
- API endpoint availability
- Data sync status monitoring
- Cache performance metrics

### Maintenance Tasks
- Regular cache cleanup
- Data integrity validation
- Performance optimization
- Index maintenance

This backend infrastructure provides a robust, scalable foundation for the H3 PFAS visualization frontend, with comprehensive data management, performance optimization, and pipeline integration capabilities. 
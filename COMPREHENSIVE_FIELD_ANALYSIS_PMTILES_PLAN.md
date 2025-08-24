# Comprehensive Field Analysis PMTiles Visualization Plan

## 🎯 Project Overview

Create a new subpage for landbruget.dk that visualizes the comprehensive field analysis data (from `recreate_original_csv_structure.py`) as interactive PMTiles using Kepler.gl. This will provide a powerful field-level visualization of Danish agricultural data including pesticide usage, environmental areas (BNBO, wetlands), soil types, and building proximity data.

## 📊 Data Analysis

### Source Data Structure (from recreate_original_csv_structure.py)

**Key Data Components:**
- **Field Identification**: field_uuid, cvr_number, markblok_id, mark_id, kommune
- **Spatial Data**: GPS coordinates, geometry (WKT format for Kepler.gl)
- **Agricultural**: area_hectares, is_organic, crop_name
- **Environmental Areas**: 
  - BNBO areas (biodiversity/nature restoration)
  - Wetlands areas
  - Water coverage calculations
- **Soil Data**: soil_type_count, dominant_soil_type, dominant_soil_coverage_pct
- **Pesticide Data**: 
  - Applications by type (kg, liters, tablets)
  - PFAS, diquat, glyphosate specific data
  - Environmental burden (belastning) calculations
  - Partial coverage indicators
- **Proximity Data**: 
  - Residential buildings
  - Educational facilities  
  - Water distance
  - All formatted with semicolon separators

**Output Formats Supported:**
- CSV with geometry (WKT format)
- Parquet with geometry (optimized for Kepler.gl)
- ~46 core columns + geometry + proximity data

## 🏗️ Technical Architecture

### Frontend Architecture
**Base Framework**: Next.js 15 (following existing landbruget.dk structure)
**Visualization**: Kepler.gl for PMTiles rendering
**Styling**: Tailwind CSS v4 (consistent with existing frontend)
**State Management**: Zustand (following frontend-pesticide pattern)

### Data Pipeline
1. **Data Generation**: `recreate_original_csv_structure.py` → Parquet with geometry
2. **PMTiles Conversion**: Parquet → GeoJSON → PMTiles using tippecanoe
3. **Tile Serving**: Static PMTiles files served via CDN/GCS
4. **Frontend Consumption**: Kepler.gl + PMTiles integration

## 📁 Project Structure

```
frontend/
├── src/app/(main)/
│   └── markanalyse/              # New field analysis page
│       ├── page.tsx              # Main visualization page
│       ├── loading.tsx           # Loading state
│       └── components/           # Page-specific components
│           ├── FieldAnalysisMap.tsx      # Main Kepler.gl map
│           ├── DataControls.tsx          # Filter controls
│           ├── LayerControls.tsx         # Layer visibility
│           ├── FieldDetailsPanel.tsx     # Selected field info
│           └── ExportControls.tsx        # Data export options
├── src/components/field-analysis/       # Reusable components
│   ├── KeplerGLMap.tsx          # Kepler.gl wrapper
│   ├── PMTilesLoader.tsx        # PMTiles data loading
│   ├── FieldTooltip.tsx         # Hover information
│   └── DataLegend.tsx           # Color scale legends
├── src/lib/field-analysis/      # Utilities and services
│   ├── pmtiles-config.ts        # PMTiles configuration
│   ├── kepler-config.ts         # Kepler.gl layer configs
│   ├── data-processing.ts       # Data transformation utils
│   └── color-schemes.ts         # Visualization color schemes
└── src/types/field-analysis.ts  # TypeScript definitions
```

### Data Processing Scripts
```
scripts/analysis/
├── generate_field_analysis_pmtiles.py   # Convert parquet to PMTiles
├── optimize_field_geometries.py         # Geometry simplification
└── validate_pmtiles_output.py           # Quality assurance
```

## 🎨 Visualization Design

### Map Layers (Kepler.gl Configuration)

1. **Agricultural Fields Layer** (Primary)
   - **Geometry**: Field polygons with comprehensive analysis data
   - **Color Coding**: By pesticide load, PFAS, organic status, crop type
   - **Properties**: All field-level analysis data (pesticides, soil, proximity info)
   - **Interactive**: Click for detailed field information

2. **BNBO Environmental Areas Layer**
   - **Geometry**: Polygon data from BNBO dataset in GCS
   - **Color Coding**: By BNBO status (Action Required, Completed, etc.)
   - **Properties**: Status categories, area coverage, water project information
   - **Transparency**: Semi-transparent overlay on fields

3. **Wetlands Layer**
   - **Geometry**: Polygon data from wetlands dataset in GCS
   - **Color Coding**: Blue gradient by wetland type and water coverage
   - **Properties**: Wetland classification, water project status, area
   - **Transparency**: Semi-transparent blue overlay

4. **Buildings Layer (Proximity Filtered)**
   - **Geometry**: Point data from BBR dataset in GCS
   - **Filter**: Only buildings within 100m of agricultural fields
   - **Color Coding**: By building type (Residential, Educational, Agricultural, etc.)
   - **Size**: Based on building importance/floor area
   - **Interactive**: Show building details and distance to nearest field

### Interactive Features

1. **Multi-Layer Toggle**: Enable/disable different data layers
2. **Filter Controls**:
   - Kommune selection dropdown
   - Crop type filters
   - Organic/conventional toggle
   - Pesticide usage thresholds
   - Area size ranges
3. **Time Series**: If multiple years available
4. **Field Selection**: Click for detailed information panel
5. **Export Options**: Selected data download

### Color Schemes

**Pesticide Intensity**: Red gradient (0 = transparent, high = dark red)
**Environmental Status**: 
- BNBO Action Required: Orange
- BNBO Completed: Green
- Wetlands: Blue gradient
**Soil Types**: Categorical color palette (browns/tans)
**Crop Types**: Categorical color palette (greens/yellows)
**Organic Fields**: Green border highlight

## 🛠️ Implementation Phases

### Phase 1: Data Analysis & Pipeline Setup 🔄 (In Progress)
- [x] Analyze recreate_original_csv_structure.py output structure
- [x] Generate comprehensive field analysis data with full geometry coverage (617,774 fields)
- [x] Create `generate_field_analysis_pmtiles.py` script
- [ ] Generate field analysis PMTiles (primary layer)
- [ ] Create BNBO environmental areas PMTiles generation script
- [ ] Create wetlands PMTiles generation script  
- [ ] Create buildings (100m proximity filtered) PMTiles generation script
- [ ] Set up PMTiles hosting (GCS bucket integration)
- [ ] Validate all PMTiles quality and performance

### Phase 2: Frontend Foundation 
- [ ] Create new `/markanalyse` page structure in landbruget.dk
- [ ] Add Kepler.gl dependencies to existing package.json
- [ ] Create basic page layout matching landbruget.dk design patterns
- [ ] Set up PMTiles loading service (inspired by frontend-pesticide)
- [ ] Implement responsive container and navigation integration

### Phase 3: Core Kepler.gl Integration
- [ ] Create KeplerGLMap component wrapper
- [ ] Configure base map layers (following frontend-pesticide patterns)
- [ ] Implement PMTiles data source connection
- [ ] Set up basic field polygon visualization
- [ ] Add map controls and viewport management

### Phase 4: Advanced Visualization Layers
- [ ] Configure pesticide usage layers (PFAS, diquat, glyphosate)
- [ ] Implement environmental areas layers (BNBO, wetlands)
- [ ] Add soil type visualization layer
- [ ] Create building proximity point layer
- [ ] Implement layer toggle controls with color legends

### Phase 5: Interactive Features
- [ ] Add field selection and details panel
- [ ] Implement filtering controls (kommune, crop type, organic)
- [ ] Create interactive tooltips with comprehensive field data
- [ ] Add data export functionality
- [ ] Optimize performance for large field datasets

### Phase 6: Integration & Polish
- [ ] Add to landbruget.dk main navigation menu
- [ ] Implement loading states and error handling
- [ ] Ensure mobile responsiveness
- [ ] Performance testing and optimization
- [ ] User acceptance testing

### Phase 7: Documentation & Deployment
- [ ] Create user documentation and help text
- [ ] Set up automated PMTiles regeneration pipeline
- [ ] Deploy to production environment
- [ ] Monitor performance and gather user feedback

## 📋 Technical Requirements

### Dependencies (Additional to existing frontend)
```json
{
  "kepler.gl": "^3.2.0",
  "deck.gl": "^9.1.0", 
  "@deck.gl/layers": "^9.1.0",
  "@deck.gl/aggregation-layers": "^9.1.0",
  "protomaps": "^2.1.0",
  "pmtiles": "^3.2.0",
  "react-kepler.gl": "^3.2.0",
  "react-redux": "^9.1.0",
  "redux": "^5.0.0"
}
```

### Data Processing Dependencies
```bash
# Python packages
pip install tippecanoe pyarrow geopandas duckdb

# System dependencies  
brew install tippecanoe  # macOS
apt-get install tippecanoe  # Ubuntu/Debian
```

## 🔧 Detailed Technical Implementation

### 1. Data Pipeline Architecture

#### A. PMTiles Generation Script (`scripts/analysis/generate_field_analysis_pmtiles.py`)

```python
#!/usr/bin/env python3
"""
Generate PMTiles from comprehensive field analysis data for Kepler.gl visualization.
Converts parquet output from recreate_original_csv_structure.py into optimized vector tiles.
"""

import argparse
import subprocess
import tempfile
import json
from pathlib import Path

def generate_field_analysis_pmtiles(
    input_parquet: str,
    output_pmtiles: str,
    max_zoom: int = 14,
    min_zoom: int = 4,
    base_zoom: int = 10
):
    """
    Convert field analysis parquet to PMTiles using tippecanoe.
    
    Args:
        input_parquet: Path to parquet file with geometry
        output_pmtiles: Output PMTiles file path
        max_zoom: Maximum zoom level (14 for field-level detail)
        min_zoom: Minimum zoom level (4 for country overview)  
        base_zoom: Base zoom for optimal detail (10 for regional)
    """
    
    # Step 1: Convert parquet to GeoJSON
    # Step 2: Optimize geometries for web display
    # Step 3: Generate PMTiles with tippecanoe
    # Step 4: Validate output and generate metadata
```

#### B. Data Structure Optimization

**Field Properties for PMTiles:**
```json
{
  "field_uuid": "string",
  "kommune": "string", 
  "cvr_number": "number",
  "area_hectares": "number",
  "crop_name": "string",
  "is_organic": "boolean",
  
  // Pesticide data (simplified for visualization)
  "total_pesticide_belastning": "number",
  "total_pfas_kg": "number", 
  "total_diquat_kg": "number",
  "total_glyphosate_kg": "number",
  "is_partial_coverage": "boolean",
  
  // Environmental areas
  "bnbo_area_hectares": "number",
  "bnbo_status": "string",
  "wetland_area_hectares": "number",
  
  // Soil and proximity (simplified)
  "dominant_soil_type": "string",
  "has_residential_proximity": "boolean",
  "has_school_proximity": "boolean",
  "water_distance_m": "number"
}
```

### 2. Frontend Architecture Details

#### A. Page Structure (`frontend/src/app/(main)/markanalyse/page.tsx`)

```typescript
'use client';

import { Suspense } from 'react';
import { Container } from '@/components/layout/container';
import { FieldAnalysisMap } from './components/FieldAnalysisMap';
import { DataControls } from './components/DataControls';
import { FieldDetailsPanel } from './components/FieldDetailsPanel';
import { LoadingSpinner } from '@/components/ui/loading-spinner';

export default function FieldAnalysisPage() {
  return (
    <div className="min-h-screen bg-gray-50">
      <Container className="py-8">
        <div className="mb-6">
          <h1 className="text-3xl font-bold text-gray-900">
            Markanalyse - Omfattende Landbrugsdata
          </h1>
          <p className="mt-2 text-gray-600">
            Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, 
            miljøområder og nærliggende bygninger.
          </p>
        </div>
        
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6 h-[800px]">
          {/* Controls Sidebar */}
          <div className="lg:col-span-1">
            <DataControls />
          </div>
          
          {/* Main Map */}
          <div className="lg:col-span-2">
            <Suspense fallback={<LoadingSpinner />}>
              <FieldAnalysisMap />
            </Suspense>
          </div>
          
          {/* Details Panel */}
          <div className="lg:col-span-1">
            <FieldDetailsPanel />
          </div>
        </div>
      </Container>
    </div>
  );
}
```

#### B. Kepler.gl Integration (`components/FieldAnalysisMap.tsx`)

```typescript
'use client';

import { useEffect, useRef, useState } from 'react';
import KeplerGl from 'kepler.gl';
import { addDataToMap } from 'kepler.gl/actions';
import { PMTiles } from 'pmtiles';
import { fieldAnalysisMapConfig } from '@/lib/field-analysis/kepler-config';

interface FieldAnalysisMapProps {
  className?: string;
}

export function FieldAnalysisMap({ className }: FieldAnalysisMapProps) {
  const mapRef = useRef<HTMLDivElement>(null);
  const [mapData, setMapData] = useState(null);
  
  useEffect(() => {
    // Load PMTiles data
    loadFieldAnalysisPMTiles().then(setMapData);
  }, []);

  return (
    <div ref={mapRef} className={`relative ${className}`}>
      {mapData && (
        <KeplerGl
          id="field-analysis-map"
          width={800}
          height={600}
          mapboxApiAccessToken={process.env.NEXT_PUBLIC_MAPBOX_TOKEN}
          onSaveConfig={(config) => {
            // Save user customizations
          }}
        />
      )}
    </div>
  );
}

async function loadFieldAnalysisPMTiles() {
  // Implementation following frontend-pesticide patterns
  const pmtilesUrl = 'https://data.pesticidkortet.dk/pmtiles/field_analysis_2024.pmtiles';
  
  try {
    const pmtiles = new PMTiles(pmtilesUrl);
    const data = await pmtiles.getGeoJSON();
    return data;
  } catch (error) {
    console.error('Failed to load PMTiles:', error);
    return null;
  }
}
```

#### C. Kepler.gl Configuration (`lib/field-analysis/kepler-config.ts`)

```typescript
export const fieldAnalysisMapConfig = {
  version: 'v1',
  config: {
    visState: {
      filters: [],
      layers: [
        {
          id: 'field-polygons',
          type: 'geojson',
          config: {
            dataId: 'field_analysis_data',
            label: 'Agricultural Fields',
            color: [34, 139, 34],
            highlightColor: [252, 242, 26, 255],
            columns: {
              geojson: 'geometry'
            },
            isVisible: true,
            visConfig: {
              opacity: 0.8,
              strokeOpacity: 0.8,
              thickness: 1,
              strokeColor: [34, 139, 34],
              colorRange: {
                name: 'Global Warming',
                type: 'sequential',
                category: 'Uber',
                colors: ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300']
              },
              strokeColorRange: {
                name: 'Global Warming',
                type: 'sequential', 
                category: 'Uber',
                colors: ['#5A1846', '#900C3F', '#C70039', '#E3611C', '#F1920E', '#FFC300']
              },
              radius: 10,
              sizeRange: [0, 10],
              radiusRange: [0, 50],
              heightRange: [0, 500],
              elevationScale: 5,
              enableElevationZoomFactor: true,
              stroked: true,
              filled: true,
              enable3d: false,
              wireframe: false
            },
            hidden: false,
            textLabel: [
              {
                field: null,
                color: [255, 255, 255],
                size: 18,
                offset: [0, 0],
                anchor: 'start',
                alignment: 'center'
              }
            ]
          },
          visualChannels: {
            colorField: {
              name: 'total_pesticide_belastning',
              type: 'real'
            },
            colorScale: 'quantile',
            strokeColorField: null,
            strokeColorScale: 'quantile',
            sizeField: null,
            sizeScale: 'linear',
            heightField: null,
            heightScale: 'linear',
            radiusField: null,
            radiusScale: 'linear'
          }
        }
      ],
      interactionConfig: {
        tooltip: {
          fieldsToShow: {
            field_analysis_data: [
              {
                name: 'field_uuid',
                format: null
              },
              {
                name: 'kommune', 
                format: null
              },
              {
                name: 'area_hectares',
                format: null
              },
              {
                name: 'crop_name',
                format: null
              },
              {
                name: 'total_pesticide_belastning',
                format: null
              }
            ]
          },
          compareMode: false,
          compareType: 'absolute',
          enabled: true
        },
        brush: {
          size: 0.5,
          enabled: false
        },
        geocoder: {
          enabled: false
        },
        coordinate: {
          enabled: false
        }
      },
      layerBlending: 'normal',
      splitMaps: [],
      animationConfig: {
        currentTime: null,
        speed: 1
      }
    },
    mapState: {
      bearing: 0,
      dragRotate: false,
      latitude: 56.26392,
      longitude: 9.501785,
      pitch: 0,
      zoom: 7,
      isSplit: false
    },
    mapStyle: {
      styleType: 'light',
      topLayerGroups: {},
      visibleLayerGroups: {
        label: true,
        road: true,
        border: false,
        building: true,
        water: true,
        land: true,
        '3d building': false
      },
      threeDBuildingColor: [9.665468314072013, 17.18305478057247, 31.1442867897876],
      mapStyles: {}
    }
  }
};
```

### Performance Considerations
- **PMTiles Size**: Optimize tile size vs detail level
- **Layer Management**: Efficient layer switching
- **Memory Usage**: Handle large field datasets
- **Mobile Performance**: Simplified view for mobile devices

## 🎯 Success Metrics

1. **Data Coverage**: All fields from recreate_original_csv_structure.py visualized
2. **Performance**: < 3 second initial load time
3. **Interactivity**: Smooth zoom/pan with < 100ms response
4. **Mobile Compatibility**: Functional on tablets and phones
5. **Data Accuracy**: 100% consistency with source data

## 🚀 Getting Started

### Prerequisites
1. Existing landbruget.dk frontend setup
2. Access to GCS bucket for PMTiles hosting
3. Generated data from recreate_original_csv_structure.py

### First Steps
1. Run recreate_original_csv_structure.py with --format=parquet --include-geometry
2. Create generate_field_analysis_pmtiles.py script
3. Set up /markanalyse page structure
4. Begin Kepler.gl integration

## 📝 Progress Tracking

- [ ] **Phase 1**: Data Pipeline Setup
- [ ] **Phase 2**: Frontend Foundation  
- [ ] **Phase 3**: Core Visualization
- [ ] **Phase 4**: Advanced Features
- [ ] **Phase 5**: Integration & Polish
- [ ] **Phase 6**: Documentation & Deployment

### 3. Navigation Integration

#### A. Add to Main Navigation (`frontend/src/components/layout/templates/navbar.tsx`)

```typescript
const links = [
  { href: "/?section=overview", label: "Oversigt" },
  { href: "/?section=explore", label: "Udforsk" },
  { href: "/markanalyse", label: "Markanalyse" }, // New field analysis page
  { href: "/?section=blog", label: "Blog" },
];
```

#### B. Route Configuration
- **URL**: `/markanalyse`
- **Page Title**: "Markanalyse - Omfattende Landbrugsdata"
- **Meta Description**: "Interaktiv visualisering af danske landbrugsmarker med pesticidforbrug, miljøområder og nærliggende bygninger"

### 4. Data Hosting Strategy

#### A. GCS Bucket Structure
```
gs://landbrugsdata-raw-data/
├── gold/
│   └── pmtiles/
│       └── field_analysis/
│           ├── field_analysis_2024.pmtiles (907MB)
│           ├── bnbo_all_2024.pmtiles (7.1MB)
│           ├── wetlands_all_2024.pmtiles (518MB)
│           ├── water_projects_2024.pmtiles (13.4MB)
│           └── buildings_proximity_2024.pmtiles (TBD)
```

#### B. Cloudflare CDN Integration ⚡
- **Primary CDN**: Cloudflare for ultra-fast global PMTiles delivery
- **PMTiles URLs**: `https://pmtiles.landbruget.dk/[filename].pmtiles`
- **HTTP/2 Support**: Parallel tile loading for optimal performance
- **Automatic Gzip**: ~60% compression reduces actual transfer sizes
- **Edge Caching**: PMTiles cached at 300+ global edge locations
- **Range Request Support**: Essential for PMTiles random access pattern
- **Cache Headers**: Long-term caching (1 year) for immutable PMTiles
- **CORS**: Enabled for landbruget.dk domain access
- **Performance**: First tile load <200ms globally, subsequent <50ms

#### C. User Experience Impact
- **Initial Load**: 6-15MB (overview tiles only, not full files)
- **Progressive Loading**: Additional detail as user zooms/pans
- **Never Downloads Full Files**: PMTiles protocol loads only needed tiles
- **Cached Performance**: Subsequent visits load instantly from edge cache

## 🎛️ User Interface Design

### Control Panel Features

1. **Data Layer Toggles**
   - ☑️ Agricultural Fields (base layer)
   - ☑️ PFAS Applications  
   - ☑️ Diquat Applications
   - ☑️ Glyphosate Applications
   - ☑️ BNBO Areas
   - ☑️ Wetlands
   - ☑️ Soil Types
   - ☑️ Building Proximity

2. **Filter Controls**
   - Kommune dropdown (multi-select)
   - Crop type checkboxes
   - Organic/Conventional toggle
   - Area size slider (0-100+ hectares)
   - Pesticide load threshold slider

3. **Display Options**
   - Color scheme selector
   - Opacity controls per layer
   - 3D elevation toggle
   - Satellite/Street map toggle

### Field Details Panel

When a field is selected:

```typescript
interface SelectedFieldData {
  // Basic Information
  field_uuid: string;
  kommune: string;
  cvr_number: number;
  area_hectares: number;
  crop_name: string;
  is_organic: boolean;
  
  // Pesticide Usage
  total_pesticide_applications: number;
  total_pesticide_belastning: number;
  pfas_applications: number;
  diquat_applications: number;
  glyphosate_applications: number;
  is_partial_coverage: boolean;
  
  // Environmental Areas
  bnbo_area_hectares: number;
  bnbo_status_categories: string;
  wetland_area_hectares: number;
  
  // Soil Information
  dominant_soil_type: string;
  dominant_soil_coverage_pct: number;
  
  // Proximity Data
  residential_buildings_proximity: string;
  educational_facilities_proximity: string;
  water_distance_proximity: string;
}
```

## 📊 Data Quality & Validation

### Quality Metrics to Track
- **Geometry Validity**: % of fields with valid polygons
- **Data Completeness**: % of fields with all required attributes
- **Spatial Accuracy**: Coordinate system consistency
- **Pesticide Data Coverage**: % of fields with pesticide applications
- **Environmental Data Coverage**: % of fields with BNBO/wetland intersections

### Validation Checks
1. **Geometry Validation**: Valid WKT format, non-self-intersecting polygons
2. **Attribute Validation**: Required fields present, data types correct
3. **Spatial Validation**: Coordinates within Denmark bounds
4. **Cross-Reference Validation**: CVR numbers exist, kommune names valid
5. **PMTiles Validation**: Tile structure correct, zoom levels appropriate

## 🚀 Performance Optimization

### PMTiles Optimization
- **Simplification**: Reduce polygon complexity at lower zoom levels
- **Attribute Filtering**: Include only essential attributes per zoom level
- **Compression**: Use gzip compression for tile serving
- **Chunking**: Optimal tile size (~500KB per tile)

### Frontend Optimization
- **Lazy Loading**: Load map components on demand
- **Layer Management**: Efficient show/hide layer operations
- **Memory Management**: Clear unused layer data
- **Caching**: Browser cache PMTiles metadata
- **Progressive Enhancement**: Basic functionality without JavaScript

---

## ✅ Acceptance Criteria

### Phase 1 Complete When:
- [ ] PMTiles generation script works with sample data
- [ ] Sample PMTiles file loads correctly
- [ ] Basic data validation passes
- [ ] GCS hosting setup is functional

### Phase 2 Complete When:
- [ ] `/markanalyse` page renders correctly
- [ ] Navigation integration works
- [ ] Basic Kepler.gl map displays
- [ ] Responsive layout functions on mobile/desktop

### Phase 3 Complete When:
- [ ] Field polygons display with proper styling
- [ ] Basic interactions (hover, click) work
- [ ] Map controls (zoom, pan) function smoothly
- [ ] Performance meets targets (<3s initial load)

### Final Deployment Ready When:
- [ ] All data layers display correctly
- [ ] Filter controls work as expected
- [ ] Field details panel shows complete information
- [ ] Export functionality works
- [ ] Mobile experience is fully functional
- [ ] Performance optimization complete
- [ ] User documentation available

---

**Last Updated**: Comprehensive Plan with Technical Details
**Current Status**: Ready to begin Phase 1 - Data Pipeline Setup
**Next Steps**: 
1. Run `recreate_original_csv_structure.py` to generate sample data
2. Create `generate_field_analysis_pmtiles.py` script
3. Begin frontend page structure setup

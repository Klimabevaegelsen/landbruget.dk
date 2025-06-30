// H3 Data type definitions for frontend processing
export interface H3DataPoint {
  h3_id: number;
  year: number;
  total_pesticide_load: number;
  total_pfas_grams: number;
  pesticide_application_count: number;
  field_count: number;
  agricultural_area_ha: number;
  avg_field_coverage: number;
  geometry: GeoJSON.Polygon;
  centroid_lon: number;
  centroid_lat: number;
  h3_resolution: number;
  // Calculated fields for visualization
  pfas_intensity?: number; // PFAS per hectare
  pesticide_intensity?: number; // Pesticide per hectare
}

// Raw data from pipeline (before transformation)
export interface H3RawData {
  h3_id: number;
  year: number;
  total_pesticide_load: number;
  total_pfas_grams: number;
  pesticide_application_count: number;
  field_count: number;
  agricultural_area_ha: number;
  avg_field_coverage: number;
  geometry_wkt: string; // WKT format from pipeline
  h3_centroid_lat: number;
  h3_centroid_lon: number;
  h3_resolution: number;
}

// Aggregated data for cumulative mode
export interface H3AggregatedData {
  h3_id: number;
  years: number[]; // Array of years included in aggregation
  total_pesticide_load: number; // Sum across years
  total_pfas_grams: number; // Sum across years
  total_pesticide_application_count: number; // Sum across years
  max_field_count: number; // Maximum field count across years
  avg_agricultural_area_ha: number; // Average area across years
  avg_field_coverage: number; // Average coverage across years
  geometry: GeoJSON.Polygon;
  centroid_lon: number;
  centroid_lat: number;
  h3_resolution: number;
  // Calculated fields
  avg_annual_pesticide_load: number;
  avg_annual_pfas_grams: number;
  cumulative_pesticide_intensity: number;
  cumulative_pfas_intensity: number;
}

// Data filtering options
export interface H3DataFilter {
  years?: number[];
  cumulativeMode?: boolean;
  minPesticideLoad?: number;
  maxPesticideLoad?: number;
  minPfasGrams?: number;
  maxPfasGrams?: number;
  h3Resolution?: number;
  bbox?: {
    minLon: number;
    maxLon: number;
    minLat: number;
    maxLat: number;
  };
}

// Data processing configuration
export interface H3ProcessingConfig {
  aggregationMethod: 'sum' | 'average' | 'max';
  includeIntensityCalculations: boolean;
  geometryFormat: 'geojson' | 'wkt';
  coordinateSystem: 'EPSG:4326' | 'EPSG:3857';
}

// Data quality metadata
export interface H3DataQuality {
  totalRecords: number;
  recordsWithGeometry: number;
  recordsWithPesticideData: number;
  recordsWithPfasData: number;
  yearRange: {
    min: number;
    max: number;
  };
  spatialExtent: {
    minLon: number;
    maxLon: number;
    minLat: number;
    maxLat: number;
  };
  dataCompleteness: number; // Percentage
  lastUpdated: string; // ISO date string
} 
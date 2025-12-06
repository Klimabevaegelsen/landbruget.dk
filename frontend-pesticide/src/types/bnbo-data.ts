// BNBO Data type definitions
export interface BNBOArea {
  id: number;
  bnbo_id: string;
  status_code: BNBOStatusCode;
  status_description: string;
  area_ha: number;
  geometry: GeoJSON.MultiPolygon | GeoJSON.Polygon;
  year: number;
  created_at: string;
}

// BNBO status codes
export type BNBOStatusCode = 
  | 'protected'
  | 'buffer'
  | 'agricultural'
  | 'transition'
  | 'unprotected';

// BNBO status descriptions mapping
export const BNBO_STATUS_DESCRIPTIONS: Record<BNBOStatusCode, string> = {
  protected: 'Fully Protected Area',
  buffer: 'Buffer Zone',
  agricultural: 'Agricultural Buffer Zone',
  transition: 'Transition Zone',
  unprotected: 'Unprotected Area'
};

// BNBO color mapping for visualization
export const BNBO_STATUS_COLORS: Record<BNBOStatusCode, string> = {
  protected: '#2d8659',      // Dark green - fully protected
  buffer: '#52c878',         // Light green - buffer zone
  agricultural: '#ffd23f',   // Yellow - agricultural buffer
  transition: '#ff8c42',     // Orange - transition zone
  unprotected: '#e5e5e5'     // Gray - no protection
};

// Raw BNBO data from pipeline
export interface BNBORawData {
  bnbo_id: string;
  status_code: string;
  status_description: string;
  area_ha: number;
  geometry_wkt: string; // WKT format from pipeline
  year: number;
}

// BNBO data filtering options
export interface BNBODataFilter {
  statusCodes?: BNBOStatusCode[];
  minAreaHa?: number;
  maxAreaHa?: number;
  year?: number;
  bbox?: {
    minLon: number;
    maxLon: number;
    minLat: number;
    maxLat: number;
  };
}

// BNBO statistics for data panel
export interface BNBOStatistics {
  totalAreas: number;
  totalAreaHa: number;
  statusBreakdown: Record<BNBOStatusCode, {
    count: number;
    totalAreaHa: number;
    percentage: number;
  }>;
  averageAreaHa: number;
  largestAreaHa: number;
  protectionCoverage: number; // Percentage of total area that is protected
} 
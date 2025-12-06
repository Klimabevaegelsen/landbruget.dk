// BBR Data type definitions
export interface BBRBuilding {
  id: number;
  bbr_id: string;
  building_code: string | null;
  building_type: BBRBuildingType;
  construction_year: number | null;
  floor_area: number | null;
  geometry: GeoJSON.Point;
  address: string | null;
  created_at: string;
}

// BBR building types
export type BBRBuildingType = 
  | 'Residential'
  | 'Agricultural'
  | 'Industrial'
  | 'Commercial'
  | 'Public'
  | 'Other';

// BBR building type color mapping for visualization
export const BBR_TYPE_COLORS: Record<BBRBuildingType, string> = {
  'Residential': '#4a90e2',    // Blue
  'Agricultural': '#7ed321',   // Green
  'Industrial': '#f5a623',     // Orange
  'Commercial': '#d0021b',     // Red
  'Public': '#9013fe',         // Purple
  'Other': '#50e3c2'          // Teal
};

// BBR building type descriptions
export const BBR_TYPE_DESCRIPTIONS: Record<BBRBuildingType, string> = {
  'Residential': 'Residential Buildings',
  'Agricultural': 'Agricultural Buildings',
  'Industrial': 'Industrial Buildings',
  'Commercial': 'Commercial Buildings',
  'Public': 'Public Buildings',
  'Other': 'Other Building Types'
};

// Raw BBR data from pipeline
export interface BBRRawData {
  bbr_id: string;
  building_code: string | null;
  building_type: string;
  construction_year: number | null;
  floor_area: number | null;
  geometry_wkt: string; // WKT format from pipeline (POINT)
  address: string | null;
}

// BBR data filtering options
export interface BBRDataFilter {
  buildingTypes?: BBRBuildingType[];
  minConstructionYear?: number;
  maxConstructionYear?: number;
  minFloorArea?: number;
  maxFloorArea?: number;
  bbox?: {
    minLon: number;
    maxLon: number;
    minLat: number;
    maxLat: number;
  };
  proximityFilter?: {
    centerLon: number;
    centerLat: number;
    radiusKm: number;
  };
}

// BBR statistics for data panel
export interface BBRStatistics {
  totalBuildings: number;
  typeBreakdown: Record<BBRBuildingType, {
    count: number;
    percentage: number;
    avgFloorArea: number;
    avgConstructionYear: number;
  }>;
  constructionYearRange: {
    min: number;
    max: number;
  };
  totalFloorArea: number;
  averageFloorArea: number;
  buildingDensity: number; // Buildings per square kilometer
} 
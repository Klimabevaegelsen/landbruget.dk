export interface FieldAnalysisData {
  field_uuid: string;
  kommune: string;
  cvr_number: string;
  area_hectares: number;
  crop_name: string;
  is_organic: boolean;
  total_pesticide_belastning: number;
  total_pfas_active_ingredient_kg: number;
  bnbo_area_hectares: number;
  wetland_area_hectares: number;
  residential_buildings_proximity: string;
  // ... other properties
}

export interface LayerVisibility {
  fields: boolean;
  bnbo: boolean;
  wetlands: boolean;
  waterProjects: boolean;
  buildings: boolean;
}

export interface FilterState {
  kommune: string[];
  cropTypes: string[];
  organicOnly: boolean;
  areaRange: [number, number];
  pesticideThreshold: number;
}

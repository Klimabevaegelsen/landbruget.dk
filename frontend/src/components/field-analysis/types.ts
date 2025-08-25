export interface FieldAnalysisData {
  field_uuid: string;
  kommune: string;
  cvr_number: string;
  area_hectares: number;
  crop_name: string;
  is_organic: boolean;

  // Pesticide totals
  total_pesticide_belastning: number;
  total_pesticide_applications?: number;

  // PFAS data
  total_pfas_active_ingredient_kg: number;
  total_pfas_belastning?: number;
  pfas_applications?: number;

  // All pesticide dosage data (different units based on what's used)
  total_dosage_kg?: number;
  total_dosage_liters?: number;
  total_dosage_grams?: number;
  total_dosage_ml?: number;
  total_dosage_tablets?: number;
  applications_kg?: number;
  applications_liters?: number;
  applications_grams?: number;
  applications_ml?: number;
  applications_tablets?: number;

  // Chemical-specific data
  total_diquat_belastning?: number;
  diquat_applications?: number;
  total_glyphosate_active_ingredient_kg?: number;
  total_glyphosate_belastning?: number;
  glyphosate_applications?: number;

  // Environmental areas
  bnbo_area_hectares: number;
  wetland_area_hectares: number;

  // BNBO status data
  bnbo_action_required_hectares?: number;
  bnbo_completed_hectares?: number;
  bnbo_status_categories?: string;

  // Proximity data
  residential_buildings_proximity: string;
  educational_facilities_proximity?: string;
  water_distance_proximity?: string;

  // Additional fields
  unique_pesticide_products?: number;
  is_partial_coverage?: boolean;
}

export interface LayerVisibility {
  fields: boolean;
  bnbo: boolean;
  wetlands: boolean;
  water_projects: boolean;
  buildings: boolean;
}

export type VisualizationMode =
  | 'total_pesticide_belastning'
  | 'pfas_belastning'
  | 'diquat_belastning'
  | 'glyphosate_belastning'
  | 'applications_count'
  | 'organic_status'
  | 'area_size';

export type ColorUnit = 'total' | 'belastning' | 'per_hectare' | 'applications';

export interface FilterState {
  kommune: string[];
  cropTypes: string[];
  organicOnly: boolean;
  areaRange: [number, number];
  pesticideThreshold: number;
  pfasThreshold: number;
  diquatThreshold: number;
  glyphosateThreshold: number;
  chemicalFilter: 'all' | 'pfas' | 'diquat' | 'glyphosate' | 'none';
  visualizationMode: VisualizationMode;
  colorUnit: ColorUnit;
  useDecileColoring: boolean;
}

export type PesticideGrade =
  | 'TOP_1'
  | 'TOP_5'
  | 'TOP_10'
  | 'TOP_25'
  | 'TOP_50'
  | 'UNDER_AVG';

export interface GradeInfo {
  grade: PesticideGrade;
  label: string;
  description: string;
}

export type PesticideGradeStatus = 'loading' | 'ready' | 'no_data';

export interface NearbyFieldSummary {
  field_uuid: string;
  crop_name: string;
  area_hectares: number;
  distance_m: number;
  is_organic: boolean;
  kommune: string;
  total_pesticide_belastning: number;
  total_pesticide_applications: number;
  pfas_applications: number;
  pfas_products_detail?: string;
  diquat_applications: number;
  diquat_products_detail?: string;
  glyphosate_applications: number;
  glyphosate_products_detail?: string;
  other_applications: number;
  other_products_detail?: string;
  residential_buildings_proximity?: string;
  educational_facilities_proximity?: string;
  water_distance_proximity?: string;
  bnbo_area_hectares?: number;
  total_pfas_active_ingredient_kg?: number;
  total_glyphosate_active_ingredient_kg?: number;
}

export interface ExposureSummary {
  radius_m: number;
  fields_sprayed: number;
  total_applications: number;
  pfas_fields_count: number;
}

export interface PesticideReport {
  address: string;
  lat: number;
  lng: number;
  radius_m: number;
  year: number;
  grade: GradeInfo | null;
  grade_status: PesticideGradeStatus;
  score: number;
  fields_count: number;
  pfas_fields_count: number;
  avg_burden: number;
  nearest_field_m: number;
  fields: NearbyFieldSummary[];
  has_bnbo_overlap: boolean;
  has_violations: boolean;
  exposure_100m: ExposureSummary;
  exposure_1000m: ExposureSummary;
}

export interface FieldHoverData {
  cropName: string;
  cvrNumber?: string;
  burden: number;
  x: number;
  y: number;
}

export interface AddressResult {
  lat: number;
  lng: number;
  address: string;
}

// Database type definitions for Supabase integration
export interface Database {
  public: {
    Tables: {
      h3_pfas_exposure: {
        Row: H3PfasExposureRow;
        Insert: H3PfasExposureInsert;
        Update: H3PfasExposureUpdate;
      };
      bnbo_status_areas: {
        Row: BnboStatusAreaRow;
        Insert: BnboStatusAreaInsert;
        Update: BnboStatusAreaUpdate;
      };
      bbr_buildings: {
        Row: BbrBuildingRow;
        Insert: BbrBuildingInsert;
        Update: BbrBuildingUpdate;
      };
    };
    Views: Record<string, never>;
    Functions: Record<string, never>;
    Enums: Record<string, never>;
  };
}

// H3 PFAS Exposure table types
export interface H3PfasExposureRow {
  id: number;
  h3_id: number;
  year: number;
  total_pesticide_load: number | null;
  total_pfas_grams: number | null;
  pesticide_application_count: number | null;
  field_count: number | null;
  agricultural_area_ha: number | null;
  avg_field_coverage: number | null;
  geometry: string; // PostGIS geometry as WKT
  h3_centroid: string; // PostGIS point as WKT
  created_at: string;
  h3_resolution: number;
}

export interface H3PfasExposureInsert {
  h3_id: number;
  year: number;
  total_pesticide_load?: number | null;
  total_pfas_grams?: number | null;
  pesticide_application_count?: number | null;
  field_count?: number | null;
  agricultural_area_ha?: number | null;
  avg_field_coverage?: number | null;
  geometry: string;
  h3_centroid: string;
  h3_resolution?: number;
}

export interface H3PfasExposureUpdate {
  h3_id?: number;
  year?: number;
  total_pesticide_load?: number | null;
  total_pfas_grams?: number | null;
  pesticide_application_count?: number | null;
  field_count?: number | null;
  agricultural_area_ha?: number | null;
  avg_field_coverage?: number | null;
  geometry?: string;
  h3_centroid?: string;
  h3_resolution?: number;
}

// BNBO Status Areas table types
export interface BnboStatusAreaRow {
  id: number;
  bnbo_id: string;
  status_code: string;
  status_description: string | null;
  area_ha: number | null;
  geometry: string; // PostGIS geometry as WKT
  year: number | null;
  created_at: string;
}

export interface BnboStatusAreaInsert {
  bnbo_id: string;
  status_code: string;
  status_description?: string | null;
  area_ha?: number | null;
  geometry: string;
  year?: number | null;
}

export interface BnboStatusAreaUpdate {
  bnbo_id?: string;
  status_code?: string;
  status_description?: string | null;
  area_ha?: number | null;
  geometry?: string;
  year?: number | null;
}

// BBR Buildings table types
export interface BbrBuildingRow {
  id: number;
  bbr_id: string;
  building_code: string | null;
  building_type: string | null;
  construction_year: number | null;
  floor_area: number | null;
  geometry: string; // PostGIS point as WKT
  address: string | null;
  created_at: string;
}

export interface BbrBuildingInsert {
  bbr_id: string;
  building_code?: string | null;
  building_type?: string | null;
  construction_year?: number | null;
  floor_area?: number | null;
  geometry: string;
  address?: string | null;
}

export interface BbrBuildingUpdate {
  bbr_id?: string;
  building_code?: string | null;
  building_type?: string | null;
  construction_year?: number | null;
  floor_area?: number | null;
  geometry?: string;
  address?: string | null;
}

// Utility types for API responses
export type DatabaseError = {
  message: string;
  details?: string;
  hint?: string;
  code?: string;
};

export type DatabaseResponse<T> = {
  data: T | null;
  error: DatabaseError | null;
  count?: number;
}; 
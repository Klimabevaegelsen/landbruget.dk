export interface PesticideAnalysisFilters {
  geography: string; // 'country' or municipality name
  years: number[]; // Array of selected years, empty array means 'all'
  type: 'total' | 'pfas' | 'diquat' | 'glyphosate';
  cvr: string;
  page: number;
  limit: number;
  sortBy: 'belastning' | 'applications' | 'area';
  sortOrder: 'asc' | 'desc';
}

export interface CompanySummary {
  cvr_number: string;
  company_name: string;
  municipality: string;
  total_belastning: number;
  pfas_belastning: number;
  diquat_belastning: number;
  glyphosate_belastning: number;
  total_applications: number;
  unique_products: number;
  total_treated_area_ha: number;
  years_active: number[];
}

export interface PesticideAnalysisResponse {
  companies: CompanySummary[];
  total_count: number;
  page: number;
  limit: number;
  summary?: {
    total_applications?: number;
    total_companies?: number;
    unique_pesticides?: number;
    total_municipalities?: number;
    total_treated_area_ha?: number;
    total_dosage?: number;
  };
  top_pesticides?: Array<{
    name?: string;
    application_count?: number;
    total_dosage?: number;
    total_area_ha?: number;
  }>;
  filters: {
    available_years: number[];
    available_municipalities: string[];
    total_companies: number;
    companies_with_pfas: number;
    companies_with_diquat: number;
    companies_with_glyphosate: number;
  };
}

export interface CompanyDetailsResponse {
  company: CompanySummary;
  yearly_breakdown: Array<{
    year: number;
    total_belastning: number;
    pfas_belastning: number;
    diquat_belastning: number;
    glyphosate_belastning: number;
    total_applications: number;
    applications_by_product: Array<{
      product_name: string;
      registration_number: string;
      applications: number;
      total_belastning: number;
      contains_pfas: boolean;
      contains_diquat: boolean;
      contains_glyphosate: boolean;
    }>;
    fields_count?: number;
  }>;
  municipality_ranking: {
    rank: number;
    total_companies_in_municipality: number;
    percentile: number;
  };
}

export interface PesticideProduct {
  product_name: string;
  registration_number: string;
  applications: number;
  total_belastning: number;
  contains_pfas: boolean;
  contains_diquat: boolean;
  contains_glyphosate: boolean;
}

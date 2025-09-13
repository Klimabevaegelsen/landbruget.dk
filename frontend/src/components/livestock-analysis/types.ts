/**
 * Type definitions for CVR Fertilizer & Nutrient Analysis
 */

export interface FertilizerData {
  // Company/CVR info
  cvr_number: string;
  company_name: string;
  municipality: string;
  address_latitude?: number;
  address_longitude?: number;
  
  // Animal Types (C_2001-C_2030)
  c_2001_dyreart?: string;
  c_2002_chr_nr?: string;
  c_2003_besaetningsnr?: string;
  c_2004_dyretype?: string;
  c_2029_dyretypekode?: string;
  c_2005_staldtype?: string;
  c_2030_kode_for_staltype?: string;
  c_2006_antal_prod_dyr_aarsdyr?: number;
  
  // Manure/Nitrogen Production (C_2015-C_2025)
  c_2015_1_tilhoerende_goedningstype_1?: string;
  c_2015_2_normproduktion_kvaelstof_ghi_beregnet_goedningstype_a?: number;
  c_2015_3_tilhoerende_goedningstype_2?: string;
  c_2015_4_normproduktion_kvaelstof_ghi_beregnet_goedningstype_b?: number;
  c_2016_normproduktion_kvaelstof_ghi_beregnet?: number;
  c_2021_normproduktion_fosfor_ghi_beregnet?: number;
  
  // Animal Characteristics
  c_2007_maelkeydelse?: number;
  c_2008_vaegt_indgang?: number;
  c_2009_vaegt_afgang?: number;
  c_2010_alder_indgang?: number;
  c_2011_alder_afgang?: number;
  
  // Feed Data (LF - C_2031-C_2038)
  c_2031_lf_fodermaengde?: number;
  c_2032_lf_protein_i_foder?: number;
  c_2033_lf_antal_fravaennede_smaagrise?: number;
  c_2034_lf_fravaenningsvaegt?: number;
  c_2035_lf_tilvaekst_fjerkrae?: number;
  c_2036_lf_kg_aeg?: number;
  c_2037_der_har_i_planperioden_vaeret_malkekvag_jersey_krydsninger_i_besaetningen?: boolean;
  c_2038_der_har_i_planperioden_vaeret_opdraet_jersey_krydsninger_i_besaetningen?: boolean;
  
  // Field Data (F_917, F_101-F_106)
  f_917_ophoersdato?: string;
  f_101_1_samlet_dyrket_udyrket_og_udtaget_areal_ha_ghi_beregnet?: number;
  f_101_2_samlet_dyrket_udyrket_og_udtaget_areal_ha_manuelt_udfyldt?: number;
  f_106_1_harmoniareal_ha_ghi_beregnet?: number;
  f_106_2_harmoniareal_ha_manuelt_udfyldt?: number;
  
  // Nitrogen Management
  f_303_1_normproduktion_kg_n_ghi_beregnet?: number;
  f_303_3_normproduktion_kg_p_ghi_beregnet?: number;
  f_512_bedriftens_korrigerede_n_kvote_i_alt_kg_n?: number;
  f_901_virksomhedens_samlede_forbrug_af_kvaelstof_kg_n?: number;
  f_902_kvaelstofkvote_minus_forbrug_af_kvaelstof?: number;
  
  // Phosphorus Management  
  f_232_kg_fosfor_i_egen_husdyrgoedning_og_modtaget_organisk_goedning_kg_p?: number;
  f_237_samlet_forbrug_af_fosfor_i_husdyrgoedning_og_anden_organisk_goedning_og_bioaske_kg_p?: number;
  f_242_virksomhedens_samlede_fosforarealkrav_ha?: number;
  f_244_harmoniareal_minus_fosforarealkrav_ha?: number;
  
  // Manure Types (F_601-F_617)
  f_601_2_svinegylle_kg_n?: number;
  f_602_2_kvaeggylle_kg_n?: number;
  f_613_2_minkgylle_og_gylle_fra_oevrige_koedaedende_pelsdyr_kg_n?: number;
  f_614_2_fjerkregylle_kg_n?: number;
  f_604_2_fast_goedning_kg_n?: number;
  
  // Commercial Fertilizers
  f_703_1_indkoebt_kunstgoedning_fratrukket_solgt_kunstgoedning_kg_n?: number;
  f_706_1_samlet_forbrug_af_handelsgoedning_kg_n?: number;
  
  year?: number;
}

export interface FertilizerMapFilters {
  fertilizerType?: string;
  municipality?: string;
  minNitrogen?: number;
  maxNitrogen?: number;
  minPhosphorus?: number;
  maxPhosphorus?: number;
  minBiogas?: number;
  maxBiogas?: number;
  minCommercialFertilizer?: number;
  maxCommercialFertilizer?: number;
  visualizationMode: 'nitrogen_production' | 'phosphorus_production' | 'commercial_fertilizer' | 'biogas' | 'manure_types' | 'nutrient_balance';
  year?: number;
}

export interface FertilizerMapProps {
  data?: FertilizerData[];
  filters: FertilizerMapFilters;
  onFiltersChange: (filters: Partial<FertilizerMapFilters>) => void;
  onCompanySelect?: (company: FertilizerData) => void;
  className?: string;
}

export interface MapLayerVisibility {
  companies: boolean;
  density: boolean;
  municipalities: boolean;
}

export interface FertilizerTooltipData {
  cvr_number: string;
  company_name: string;
  municipality: string;
  total_nitrogen_production?: number;
  total_phosphorus_production?: number;
  commercial_fertilizer_usage?: number;
  biogas_production?: number;
  dominant_fertilizer_type?: string;
  coordinate: [number, number];
}

// Color scheme interfaces
export interface ColorScheme {
  name: string;
  colors: string[];
  domain: [number, number];
}

export interface FertilizerAnalysisFilters {
  geography: 'country' | 'municipality';
  municipality?: string;
  fertilizerTypes: string[];
  cvr?: string;
  minNitrogenProduction?: number;
  maxNitrogenProduction?: number;
  minPhosphorusProduction?: number;
  maxPhosphorusProduction?: number;
  year: number;
  availableYears: number[];
}

export interface FertilizerAnalysisResponse {
  data: FertilizerData[];
  summary: {
    total_companies: number;
    total_nitrogen_production_kg: number;
    total_phosphorus_production_kg: number;
    total_commercial_fertilizer_kg: number;
    total_biogas_production_kg: number;
    municipalities: string[];
    fertilizer_types: string[];
  };
  filters: FertilizerAnalysisFilters;
  error?: string;
}

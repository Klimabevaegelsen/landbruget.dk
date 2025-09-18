// Municipality Details API Endpoint
// Returns detailed company breakdown for a specific municipality ranking

import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

interface MunicipalityDetailsRequest {
  municipality: string;
  category: 'land_use' | 'production' | 'pesticide_burden' | 'pesticide_pfas' | 'pesticide_glyphosate' | 'antibiotic_usage' | 'environmental' | 'organic_farming';
  year?: number;
  limit?: number;
}

interface CompanyContribution {
  company_id: string;
  company_name: string;
  cvr_number: string;
  value: number;
  percentage_of_municipality: number;
  rank_in_municipality: number;
  additional_data?: Record<string, unknown>;
}

interface MunicipalityDetailsResponse {
  municipality: string;
  category: string;
  year: number;
  total_municipality_value: number;
  municipality_rank: number | null;
  companies: CompanyContribution[];
  metadata: {
    total_companies: number;
    generated_at: string;
  };
}

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders })
  }

  try {
    // Parse request parameters
    const url = new URL(req.url);
    const params: MunicipalityDetailsRequest = {
      municipality: url.searchParams.get('municipality') || '',
      category: url.searchParams.get('category') as any || 'land_use',
      year: parseInt(url.searchParams.get('year') || '2024'),
      limit: parseInt(url.searchParams.get('limit') || '50')
    };

    if (!params.municipality) {
      throw new Error('Municipality parameter is required');
    }

    // Initialize Supabase client
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!
    const supabase = createClient(supabaseUrl, supabaseAnonKey)

    let response: MunicipalityDetailsResponse;

    // Get detailed breakdown based on category
    switch (params.category) {
      case 'land_use':
        response = await getLandUseDetails(supabase, params);
        break;
      case 'production':
        response = await getProductionDetails(supabase, params);
        break;
      case 'pesticide_burden':
        response = await getPesticideBurdenDetails(supabase, params);
        break;
      case 'pesticide_pfas':
        response = await getPFASDetails(supabase, params);
        break;
      case 'pesticide_glyphosate':
        response = await getGlyphosateDetails(supabase, params);
        break;
      case 'antibiotic_usage':
        response = await getAntibioticDetails(supabase, params);
        break;
      case 'environmental':
        response = await getEnvironmentalDetails(supabase, params);
        break;
      case 'organic_farming':
        response = await getOrganicFarmingDetails(supabase, params);
        break;
      default:
        throw new Error(`Unsupported category: ${params.category}`);
    }

    return new Response(
      JSON.stringify(response),
      {
        headers: {
          ...corsHeaders,
          'Content-Type': 'application/json'
        }
      }
    )

  } catch (error) {
    console.error('Municipality details error:', error);
    return new Response(
      JSON.stringify({
        error: 'Internal server error',
        message: error.message
      }),
      {
        status: 500,
        headers: {
          ...corsHeaders,
          'Content-Type': 'application/json'
        }
      }
    )
  }
})

// Land Use Details - companies by total agricultural area (FIELD LOCATION BASED)
async function getLandUseDetails(supabase: any, params: MunicipalityDetailsRequest): Promise<MunicipalityDetailsResponse> {
  // Get field data based on FIELD municipality (not company municipality)
  const { data: fieldData, error: fieldError } = await supabase
    .from('field_yearly_data')
    .select(`
      *,
      companies!inner(id, company_name, cvr_number)
    `)
    .eq('year', params.year);

  if (fieldError) throw fieldError;

  // Get municipality mapping from pesticide data - FIELD LOCATION BASED
  const { data: municipalityFields, error: munError } = await supabase
    .from('pesticide_applications_with_field_details')
    .select('field_uuid, municipality')
    .eq('municipality', params.municipality);

  if (munError) throw munError;

  const municipalityFieldUuids = new Set(municipalityFields.map((f: any) => f.field_uuid));

  // Filter by FIELD location and aggregate by company
  const companyAggregates = fieldData
    .filter((row: any) => municipalityFieldUuids.has(row.field_uuid))
    .reduce((acc: any, row: any) => {
      const companyId = row.companies.id;
      if (!acc[companyId]) {
        acc[companyId] = {
          company_id: companyId,
          company_name: row.companies.company_name,
          cvr_number: row.companies.cvr_number,
          total_area: 0,
          organic_area: 0,
          field_count: 0
        };
      }
      acc[companyId].total_area += row.area_ha || 0;
      acc[companyId].organic_area += row.is_organic ? (row.area_ha || 0) : 0;
      acc[companyId].field_count += 1;
      return acc;
    }, {});

  const companies = Object.values(companyAggregates)
    .map((company: any) => ({
      ...company,
      organic_percentage: company.total_area > 0 ? (company.organic_area / company.total_area * 100) : 0
    }))
    .sort((a: any, b: any) => b.total_area - a.total_area)
    .slice(0, params.limit);

  const totalMunicipalityValue = companies.reduce((sum: number, c: any) => sum + c.total_area, 0);

  return {
    municipality: params.municipality,
    category: params.category,
    year: params.year!,
    total_municipality_value: totalMunicipalityValue,
    municipality_rank: null, // TODO: Get from rankings
    companies: companies.map((company: any, index: number) => ({
      company_id: company.company_id,
      company_name: company.company_name,
      cvr_number: company.cvr_number,
      value: company.total_area,
      percentage_of_municipality: totalMunicipalityValue > 0 ? (company.total_area / totalMunicipalityValue * 100) : 0,
      rank_in_municipality: index + 1,
      additional_data: {
        field_count: company.field_count,
        organic_area: company.organic_area,
        organic_percentage: company.organic_percentage
      }
    })),
    metadata: {
      total_companies: companies.length,
      generated_at: new Date().toISOString()
    }
  };
}

// Production Details - companies by animal capacity (SITE LOCATION BASED)
async function getProductionDetails(supabase: any, params: MunicipalityDetailsRequest): Promise<MunicipalityDetailsResponse> {
  // Filter by SITE municipality (not company municipality)
  const { data, error } = await supabase
    .from('production_sites')
    .select(`
      *,
      companies!inner(id, company_name, cvr_number)
    `)
    .eq('municipality', params.municipality) // This is SITE municipality
    .not('capacity', 'is', null)
    .order('capacity', { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  const totalMunicipalityValue = data.reduce((sum: number, site: any) => sum + (site.capacity || 0), 0);

  return {
    municipality: params.municipality,
    category: params.category,
    year: params.year!,
    total_municipality_value: totalMunicipalityValue,
    municipality_rank: null,
    companies: data.map((site: any, index: number) => ({
      company_id: site.companies.id,
      company_name: site.companies.company_name,
      cvr_number: site.companies.cvr_number,
      value: site.capacity || 0,
      percentage_of_municipality: totalMunicipalityValue > 0 ? ((site.capacity || 0) / totalMunicipalityValue * 100) : 0,
      rank_in_municipality: index + 1,
      additional_data: {
        site_name: site.site_name,
        chr_number: site.chr,
        main_species: site.main_species_code,
        address: site.address
      }
    })),
    metadata: {
      total_companies: data.length,
      generated_at: new Date().toISOString()
    }
  };
}

// Pesticide Burden Details - companies by total pesticide burden (FIELD LOCATION BASED)
async function getPesticideBurdenDetails(supabase: any, params: MunicipalityDetailsRequest): Promise<MunicipalityDetailsResponse> {
  // Filter by FIELD municipality where pesticides are applied
  const { data, error } = await supabase
    .from('pesticide_applications')
    .select(`
      *,
      pesticide_applications_with_field_details!inner(municipality, cvr_number),
      companies!inner(id, company_name, cvr_number)
    `)
    .eq('year', params.year || 2023)
    .eq('pesticide_applications_with_field_details.municipality', params.municipality); // FIELD municipality

  if (error) throw error;

  // Aggregate by company
  const companyAggregates = data.reduce((acc: any, app: any) => {
    const companyId = app.companies.id;
    if (!acc[companyId]) {
      acc[companyId] = {
        company_id: companyId,
        company_name: app.companies.company_name,
        cvr_number: app.companies.cvr_number,
        total_burden: 0,
        health_burden: 0,
        application_count: 0,
        treated_area: 0
      };
    }
    acc[companyId].total_burden += app.total_burden_score || 0;
    acc[companyId].health_burden += app.health_burden_score || 0;
    acc[companyId].application_count += 1;
    acc[companyId].treated_area += app.treated_area_ha || 0;
    return acc;
  }, {});

  const companies = Object.values(companyAggregates)
    .sort((a: any, b: any) => b.total_burden - a.total_burden)
    .slice(0, params.limit);

  const totalMunicipalityValue = companies.reduce((sum: number, c: any) => sum + c.total_burden, 0);

  return {
    municipality: params.municipality,
    category: params.category,
    year: params.year || 2023,
    total_municipality_value: totalMunicipalityValue,
    municipality_rank: null,
    companies: companies.map((company: any, index: number) => ({
      company_id: company.company_id,
      company_name: company.company_name,
      cvr_number: company.cvr_number,
      value: company.total_burden,
      percentage_of_municipality: totalMunicipalityValue > 0 ? (company.total_burden / totalMunicipalityValue * 100) : 0,
      rank_in_municipality: index + 1,
      additional_data: {
        application_count: company.application_count,
        treated_area_ha: company.treated_area,
        health_burden: company.health_burden
      }
    })),
    metadata: {
      total_companies: companies.length,
      generated_at: new Date().toISOString()
    }
  };
}

// PFAS Details - companies by PFAS pesticide burden (FIELD LOCATION BASED)
async function getPFASDetails(supabase: any, params: MunicipalityDetailsRequest): Promise<MunicipalityDetailsResponse> {
  // Filter by FIELD municipality where PFAS pesticides are applied
  const { data, error } = await supabase
    .from('pesticide_applications')
    .select(`
      *,
      pesticide_applications_with_field_details!inner(municipality, cvr_number),
      companies!inner(id, company_name, cvr_number)
    `)
    .eq('year', params.year || 2023)
    .eq('pesticide_applications_with_field_details.municipality', params.municipality) // FIELD municipality
    .eq('contains_pfas', true);

  if (error) throw error;

  // Aggregate by company (similar to pesticide burden)
  const companyAggregates = data.reduce((acc: any, app: any) => {
    const companyId = app.companies.id;
    if (!acc[companyId]) {
      acc[companyId] = {
        company_id: companyId,
        company_name: app.companies.company_name,
        cvr_number: app.companies.cvr_number,
        pfas_burden: 0,
        pfas_applications: 0,
        treated_area: 0
      };
    }
    acc[companyId].pfas_burden += app.total_burden_score || 0;
    acc[companyId].pfas_applications += 1;
    acc[companyId].treated_area += app.treated_area_ha || 0;
    return acc;
  }, {});

  const companies = Object.values(companyAggregates)
    .sort((a: any, b: any) => b.pfas_burden - a.pfas_burden)
    .slice(0, params.limit);

  const totalMunicipalityValue = companies.reduce((sum: number, c: any) => sum + c.pfas_burden, 0);

  return {
    municipality: params.municipality,
    category: params.category,
    year: params.year || 2023,
    total_municipality_value: totalMunicipalityValue,
    municipality_rank: null,
    companies: companies.map((company: any, index: number) => ({
      company_id: company.company_id,
      company_name: company.company_name,
      cvr_number: company.cvr_number,
      value: company.pfas_burden,
      percentage_of_municipality: totalMunicipalityValue > 0 ? (company.pfas_burden / totalMunicipalityValue * 100) : 0,
      rank_in_municipality: index + 1,
      additional_data: {
        pfas_applications: company.pfas_applications,
        treated_area_ha: company.treated_area
      }
    })),
    metadata: {
      total_companies: companies.length,
      generated_at: new Date().toISOString()
    }
  };
}

// Glyphosate Details - companies by glyphosate pesticide burden (FIELD LOCATION BASED)
async function getGlyphosateDetails(supabase: any, params: MunicipalityDetailsRequest): Promise<MunicipalityDetailsResponse> {
  // Filter by FIELD municipality where glyphosate is applied
  const { data, error } = await supabase
    .from('pesticide_applications')
    .select(`
      *,
      pesticide_applications_with_field_details!inner(municipality, cvr_number),
      companies!inner(id, company_name, cvr_number)
    `)
    .eq('year', params.year || 2023)
    .eq('pesticide_applications_with_field_details.municipality', params.municipality) // FIELD municipality
    .eq('contains_glyphosate', true);

  if (error) throw error;

  // Aggregate by company (similar to PFAS)
  const companyAggregates = data.reduce((acc: any, app: any) => {
    const companyId = app.companies.id;
    if (!acc[companyId]) {
      acc[companyId] = {
        company_id: companyId,
        company_name: app.companies.company_name,
        cvr_number: app.companies.cvr_number,
        glyphosate_burden: 0,
        glyphosate_applications: 0,
        treated_area: 0
      };
    }
    acc[companyId].glyphosate_burden += app.total_burden_score || 0;
    acc[companyId].glyphosate_applications += 1;
    acc[companyId].treated_area += app.treated_area_ha || 0;
    return acc;
  }, {});

  const companies = Object.values(companyAggregates)
    .sort((a: any, b: any) => b.glyphosate_burden - a.glyphosate_burden)
    .slice(0, params.limit);

  const totalMunicipalityValue = companies.reduce((sum: number, c: any) => sum + c.glyphosate_burden, 0);

  return {
    municipality: params.municipality,
    category: params.category,
    year: params.year || 2023,
    total_municipality_value: totalMunicipalityValue,
    municipality_rank: null,
    companies: companies.map((company: any, index: number) => ({
      company_id: company.company_id,
      company_name: company.company_name,
      cvr_number: company.cvr_number,
      value: company.glyphosate_burden,
      percentage_of_municipality: totalMunicipalityValue > 0 ? (company.glyphosate_burden / totalMunicipalityValue * 100) : 0,
      rank_in_municipality: index + 1,
      additional_data: {
        glyphosate_applications: company.glyphosate_applications,
        treated_area_ha: company.treated_area
      }
    })),
    metadata: {
      total_companies: companies.length,
      generated_at: new Date().toISOString()
    }
  };
}

// Antibiotic Details - companies by antibiotic usage at production sites (SITE LOCATION BASED)
async function getAntibioticDetails(supabase: any, params: MunicipalityDetailsRequest): Promise<MunicipalityDetailsResponse> {
  // Filter by SITE municipality where antibiotics are used
  const { data, error } = await supabase
    .from('site_yearly_summary')
    .select(`
      *,
      production_sites!inner(municipality, company_id),
      companies!inner(id, company_name, cvr_number)
    `)
    .eq('year', params.year || 2024)
    .eq('production_sites.municipality', params.municipality) // SITE municipality
    .not('antibiotics_ddd', 'is', null)
    .order('antibiotics_ddd', { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  const totalMunicipalityValue = data.reduce((sum: number, site: any) => sum + (site.antibiotics_ddd || 0), 0);

  return {
    municipality: params.municipality,
    category: params.category,
    year: params.year || 2024,
    total_municipality_value: totalMunicipalityValue,
    municipality_rank: null,
    companies: data.map((site: any, index: number) => ({
      company_id: site.companies.id,
      company_name: site.companies.company_name,
      cvr_number: site.companies.cvr_number,
      value: site.antibiotics_ddd || 0,
      percentage_of_municipality: totalMunicipalityValue > 0 ? ((site.antibiotics_ddd || 0) / totalMunicipalityValue * 100) : 0,
      rank_in_municipality: index + 1,
      additional_data: {
        chr_number: site.chr,
        capacity_count: site.capacity_count,
        transport_count: site.transport_count
      }
    })),
    metadata: {
      total_companies: data.length,
      generated_at: new Date().toISOString()
    }
  };
}

// Environmental Details - companies by nitrogen leaching (FIELD LOCATION BASED)
async function getEnvironmentalDetails(supabase: any, params: MunicipalityDetailsRequest): Promise<MunicipalityDetailsResponse> {
  // Filter by FIELD municipality where nitrogen leaching occurs
  const { data: fieldData, error: fieldError } = await supabase
    .from('field_yearly_data')
    .select(`
      *,
      companies!inner(id, company_name, cvr_number)
    `)
    .eq('year', params.year)
    .not('n_leached_kg', 'is', null);

  if (fieldError) throw fieldError;

  // Get municipality mapping based on FIELD location
  const { data: municipalityFields, error: munError } = await supabase
    .from('pesticide_applications_with_field_details')
    .select('field_uuid, municipality')
    .eq('municipality', params.municipality);

  if (munError) throw munError;

  const municipalityFieldUuids = new Set(municipalityFields.map((f: any) => f.field_uuid));

  // Filter by FIELD location and aggregate by company
  const companyAggregates = fieldData
    .filter((row: any) => municipalityFieldUuids.has(row.field_uuid))
    .reduce((acc: any, row: any) => {
      const companyId = row.companies.id;
      if (!acc[companyId]) {
        acc[companyId] = {
          company_id: companyId,
          company_name: row.companies.company_name,
          cvr_number: row.companies.cvr_number,
          total_n_leached: 0,
          total_area: 0,
          field_count: 0
        };
      }
      acc[companyId].total_n_leached += (row.n_leached_kg || 0) * (row.area_ha || 0);
      acc[companyId].total_area += row.area_ha || 0;
      acc[companyId].field_count += 1;
      return acc;
    }, {});

  const companies = Object.values(companyAggregates)
    .map((company: any) => ({
      ...company,
      avg_n_leached_per_ha: company.total_area > 0 ? (company.total_n_leached / company.total_area) : 0
    }))
    .sort((a: any, b: any) => b.avg_n_leached_per_ha - a.avg_n_leached_per_ha)
    .slice(0, params.limit);

  const totalMunicipalityValue = companies.reduce((sum: number, c: any) => sum + c.total_n_leached, 0);

  return {
    municipality: params.municipality,
    category: params.category,
    year: params.year!,
    total_municipality_value: totalMunicipalityValue,
    municipality_rank: null,
    companies: companies.map((company: any, index: number) => ({
      company_id: company.company_id,
      company_name: company.company_name,
      cvr_number: company.cvr_number,
      value: company.avg_n_leached_per_ha,
      percentage_of_municipality: totalMunicipalityValue > 0 ? (company.total_n_leached / totalMunicipalityValue * 100) : 0,
      rank_in_municipality: index + 1,
      additional_data: {
        field_count: company.field_count,
        total_area_ha: company.total_area,
        total_n_leached_kg: company.total_n_leached
      }
    })),
    metadata: {
      total_companies: companies.length,
      generated_at: new Date().toISOString()
    }
  };
}

// Organic Farming Details - companies by organic farming percentage (FIELD LOCATION BASED)
async function getOrganicFarmingDetails(supabase: any, params: MunicipalityDetailsRequest): Promise<MunicipalityDetailsResponse> {
  // Filter by FIELD municipality where organic farming occurs
  const { data: fieldData, error: fieldError } = await supabase
    .from('field_yearly_data')
    .select(`
      *,
      companies!inner(id, company_name, cvr_number)
    `)
    .eq('year', params.year)
    .not('is_organic', 'is', null);

  if (fieldError) throw fieldError;

  // Get municipality mapping based on FIELD location
  const { data: municipalityFields, error: munError } = await supabase
    .from('pesticide_applications_with_field_details')
    .select('field_uuid, municipality')
    .eq('municipality', params.municipality);

  if (munError) throw munError;

  const municipalityFieldUuids = new Set(municipalityFields.map((f: any) => f.field_uuid));

  // Filter by FIELD location and aggregate by company
  const companyAggregates = fieldData
    .filter((row: any) => municipalityFieldUuids.has(row.field_uuid))
    .reduce((acc: any, row: any) => {
      const companyId = row.companies.id;
      if (!acc[companyId]) {
        acc[companyId] = {
          company_id: companyId,
          company_name: row.companies.company_name,
          cvr_number: row.companies.cvr_number,
          total_area: 0,
          organic_area: 0,
          field_count: 0
        };
      }
      acc[companyId].total_area += row.area_ha || 0;
      acc[companyId].organic_area += row.is_organic ? (row.area_ha || 0) : 0;
      acc[companyId].field_count += 1;
      return acc;
    }, {});

  const companies = Object.values(companyAggregates)
    .map((company: any) => ({
      ...company,
      organic_percentage: company.total_area > 0 ? (company.organic_area / company.total_area * 100) : 0
    }))
    .filter((company: any) => company.organic_area > 0) // Only companies with organic farming
    .sort((a: any, b: any) => b.organic_percentage - a.organic_percentage)
    .slice(0, params.limit);

  const totalMunicipalityValue = companies.reduce((sum: number, c: any) => sum + c.organic_area, 0);

  return {
    municipality: params.municipality,
    category: params.category,
    year: params.year!,
    total_municipality_value: totalMunicipalityValue,
    municipality_rank: null,
    companies: companies.map((company: any, index: number) => ({
      company_id: company.company_id,
      company_name: company.company_name,
      cvr_number: company.cvr_number,
      value: company.organic_percentage,
      percentage_of_municipality: totalMunicipalityValue > 0 ? (company.organic_area / totalMunicipalityValue * 100) : 0,
      rank_in_municipality: index + 1,
      additional_data: {
        field_count: company.field_count,
        total_area_ha: company.total_area,
        organic_area_ha: company.organic_area
      }
    })),
    metadata: {
      total_companies: companies.length,
      generated_at: new Date().toISOString()
    }
  };
}

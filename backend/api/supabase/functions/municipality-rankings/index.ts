// Municipality Rankings API Endpoint
// Returns municipality-level rankings across different categories

import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

interface MunicipalityRankingRequest {
  category?: 'land_use' | 'production' | 'environmental' | 'animal_health' | 'worker_safety' | 'all';
  year?: number;
  limit?: number;
  sort_by?: string;
  sort_direction?: 'asc' | 'desc';
}

interface MunicipalityRanking {
  municipality: string;
  rank: number;
  value: number;
  metric: string;
  additional_data?: Record<string, any>;
}

interface MunicipalityRankingResponse {
  rankings: {
    land_use?: MunicipalityRanking[];
    production?: MunicipalityRanking[];
    environmental?: MunicipalityRanking[];
    animal_health?: MunicipalityRanking[];
    worker_safety?: MunicipalityRanking[];
  };
  metadata: {
    year: number;
    total_municipalities: number;
    generated_at: string;
    categories_included: string[];
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
    const params: MunicipalityRankingRequest = {
      category: url.searchParams.get('category') as any || 'all',
      year: parseInt(url.searchParams.get('year') || '2024'),
      limit: parseInt(url.searchParams.get('limit') || '50'),
      sort_by: url.searchParams.get('sort_by') || 'total_area_ha',
      sort_direction: url.searchParams.get('sort_direction') as any || 'desc'
    };

    // Initialize Supabase client
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')!
    const supabase = createClient(supabaseUrl, supabaseAnonKey)

    const response: MunicipalityRankingResponse = {
      rankings: {},
      metadata: {
        year: params.year!,
        total_municipalities: 0,
        generated_at: new Date().toISOString(),
        categories_included: []
      }
    };

    // Land Use Rankings
    if (params.category === 'all' || params.category === 'land_use') {
      const landUseRankings = await getLandUseRankings(supabase, params);
      response.rankings.land_use = landUseRankings;
      response.metadata.categories_included.push('land_use');
    }

    // Production Rankings
    if (params.category === 'all' || params.category === 'production') {
      const productionRankings = await getProductionRankings(supabase, params);
      response.rankings.production = productionRankings;
      response.metadata.categories_included.push('production');
    }

    // Environmental Rankings
    if (params.category === 'all' || params.category === 'environmental') {
      const environmentalRankings = await getEnvironmentalRankings(supabase, params);
      response.rankings.environmental = environmentalRankings;
      response.metadata.categories_included.push('environmental');
    }

    // Animal Health Rankings
    if (params.category === 'all' || params.category === 'animal_health') {
      const animalHealthRankings = await getAnimalHealthRankings(supabase, params);
      response.rankings.animal_health = animalHealthRankings;
      response.metadata.categories_included.push('animal_health');
    }

    // Worker Safety Rankings
    if (params.category === 'all' || params.category === 'worker_safety') {
      const workerSafetyRankings = await getWorkerSafetyRankings(supabase, params);
      response.rankings.worker_safety = workerSafetyRankings;
      response.metadata.categories_included.push('worker_safety');
    }

    // Calculate total municipalities (use land use as baseline)
    if (response.rankings.land_use) {
      response.metadata.total_municipalities = response.rankings.land_use.length;
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
    console.error('Municipality rankings error:', error);
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

async function getLandUseRankings(supabase: any, params: MunicipalityRankingRequest): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from('municipality_land_use_summary')
    .select('*')
    .eq('year', 2025) // Use 2025 for field boundaries, 2024 organic data
    .order('total_area_ha', { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.total_area_ha,
    metric: 'total_agricultural_area_ha',
    additional_data: {
      total_fields: row.total_fields,
      avg_field_size: row.avg_field_size,
      organic_percentage: row.organic_percentage || 0,
      unique_companies: row.unique_companies,
      unique_crops: row.unique_crops
    }
  }));
}

async function getProductionRankings(supabase: any, params: MunicipalityRankingRequest): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from('municipality_production_summary')
    .select('*')
    .order('total_capacity', { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.total_capacity || 0,
    metric: 'total_production_capacity',
    additional_data: {
      total_sites: row.total_sites,
      sites_with_capacity: row.sites_with_capacity,
      avg_capacity: row.avg_capacity,
      unique_companies: row.unique_companies,
      total_antibiotics_ddd_2024: row.total_antibiotics_ddd_2024 || 0,
      total_transports_2024: row.total_transports_2024 || 0
    }
  }));
}

async function getEnvironmentalRankings(supabase: any, params: MunicipalityRankingRequest): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from('municipality_environmental_summary')
    .select('*')
    .order('organic_percentage', { ascending: false })
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.organic_percentage || 0,
    metric: 'organic_farming_percentage',
    additional_data: {
      total_fields: row.total_fields_with_env_data,
      organic_fields: row.organic_fields || 0,
      organic_area_ha: row.organic_area_ha || 0,
      environmental_burden_score: row.environmental_burden_score || 0,
      bnbo_not_dealt_with_count: row.bnbo_not_dealt_with_count || 0,
      wetland_not_restored_count: row.wetland_not_restored_count || 0
    }
  }));
}

async function getAnimalHealthRankings(supabase: any, params: MunicipalityRankingRequest): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from('municipality_animal_health_summary')
    .select('*')
    .eq('year', 2025)
    .order('avg_antibiotic_usage_add', { ascending: true }) // Lower is better for antibiotics
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.avg_antibiotic_usage_add || 0,
    metric: 'avg_antibiotic_usage_add_per_100_animals_per_day',
    additional_data: {
      companies_with_antibiotics: row.companies_with_antibiotics,
      sites_with_antibiotics: row.sites_with_antibiotics,
      total_antibiotic_doses: row.total_antibiotic_doses || 0,
      total_transports: row.total_transports || 0,
      total_animals_transported: row.total_animals_transported || 0,
      animal_health_burden_score: row.animal_health_burden_score || 0
    }
  }));
}

async function getWorkerSafetyRankings(supabase: any, params: MunicipalityRankingRequest): Promise<MunicipalityRanking[]> {
  const { data, error } = await supabase
    .from('municipality_worker_safety_summary')
    .select('*')
    .order('worker_safety_burden_score', { ascending: true }) // Lower is better for safety
    .limit(params.limit);

  if (error) throw error;

  return data.map((row: any, index: number) => ({
    municipality: row.municipality,
    rank: index + 1,
    value: row.worker_safety_burden_score || 0,
    metric: 'worker_safety_burden_score',
    additional_data: {
      companies_with_workers: row.companies_with_workers,
      total_estimated_employees: row.total_estimated_employees || 0,
      total_visa_workers: row.total_visa_workers || 0,
      visa_worker_percentage: row.visa_worker_percentage || 0,
      total_incidents: row.total_incidents || 0,
      incidents_per_1000_employees: row.incidents_per_1000_employees || 0
    }
  }));
}

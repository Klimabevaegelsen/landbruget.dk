import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

interface PesticideAnalysisRequest {
  geography?: string; // 'country' or municipality name
  years?: number[]; // Array of years, empty means 'all'
  type?: 'total' | 'pfas' | 'diquat' | 'glyphosate';
  cvr?: string;
  page?: number;
  limit?: number;
  sortBy?: 'belastning' | 'applications' | 'area';
  sortOrder?: 'asc' | 'desc';
}

interface CompanySummary {
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

interface PesticideAnalysisResponse {
  companies: CompanySummary[];
  total_count: number;
  page: number;
  limit: number;
  filters: {
    available_years: number[];
    available_municipalities: string[];
    total_companies: number;
    companies_with_pfas: number;
    companies_with_diquat: number;
    companies_with_glyphosate: number;
  };
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders })
  }

  try {
    // Parse request parameters
    const url = new URL(req.url)
    const yearsParams = url.searchParams.getAll('years')
    const years = yearsParams.length > 0 ? yearsParams.map(y => parseInt(y)).filter(y => !isNaN(y)) : []

    const params: PesticideAnalysisRequest = {
      geography: url.searchParams.get('geography') || 'country',
      years: years,
      type: (url.searchParams.get('type') as 'total' | 'pfas' | 'diquat' | 'glyphosate') || 'total',
      cvr: url.searchParams.get('cvr') || undefined,
      page: parseInt(url.searchParams.get('page') || '1'),
      limit: Math.min(parseInt(url.searchParams.get('limit') || '50'), 100), // Max 100 per page
      sortBy: (url.searchParams.get('sortBy') as 'belastning' | 'applications' | 'area') || 'belastning',
      sortOrder: (url.searchParams.get('sortOrder') as 'asc' | 'desc') || 'desc',
    }

    // Create Supabase client
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!
    const supabaseKey = Deno.env.get('SUPABASE_ANON_KEY')!
    const supabase = createClient(supabaseUrl, supabaseKey)



    // Use the RPC function instead of direct table query to get properly aggregated data
    const { data: rawCompanies, error: companiesError } = await supabase.rpc('get_pesticide_companies', {
      p_years: params.years && params.years.length > 0 ? params.years : null,
      p_geography: params.geography !== 'country' ? params.geography : null,
      p_cvr: params.cvr ? parseInt(params.cvr) : null,
      p_pesticide_type: params.type || 'total',
      p_sort_by: params.sortBy || 'total_belastning',
      p_sort_order: params.sortOrder || 'desc',
      p_limit: params.limit || 50,
      p_offset: ((params.page || 1) - 1) * (params.limit || 50)
    })

    if (companiesError) throw companiesError

    // Get metadata using the metadata function
    const { data: metadata, error: metadataError } = await supabase.rpc('get_pesticide_metadata', {
      p_years: params.years && params.years.length > 0 ? params.years : null,
      p_geography: params.geography !== 'country' ? params.geography : null,
      p_cvr: params.cvr ? parseInt(params.cvr) : null
    })

    if (metadataError) throw metadataError

    // The function already returns properly aggregated data, no need for JavaScript aggregation
    const paginatedCompanies = rawCompanies || []
    const metadataRow = metadata?.[0] || {}

    const response: PesticideAnalysisResponse = {
      companies: paginatedCompanies,
      total_count: Number(metadataRow.total_companies || 0),
      page: params.page || 1,
      limit: params.limit || 50,
      filters: {
        available_years: metadataRow.available_years || [],
        available_municipalities: metadataRow.available_municipalities || [],
        total_companies: Number(metadataRow.total_companies || 0),
        companies_with_pfas: Number(metadataRow.companies_with_pfas || 0),
        companies_with_diquat: Number(metadataRow.companies_with_diquat || 0),
        companies_with_glyphosate: Number(metadataRow.companies_with_glyphosate || 0)
      }
    }

    return new Response(
      JSON.stringify(response),
      {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200,
      },
    )

  } catch (error) {
    console.error('Error in pesticide-analysis:', error)
    return new Response(
      JSON.stringify({
        error: 'Internal server error',
        message: error.message
      }),
      {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500,
      },
    )
  }
})

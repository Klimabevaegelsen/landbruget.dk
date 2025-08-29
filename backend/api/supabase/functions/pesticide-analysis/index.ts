import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

interface PesticideAnalysisRequest {
  geography?: string; // 'country' or municipality name
  year?: number | 'all';
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
    const params: PesticideAnalysisRequest = {
      geography: url.searchParams.get('geography') || 'country',
      year: url.searchParams.get('year') === 'all' ? 'all' : parseInt(url.searchParams.get('year') || ''),
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

    // Build query for company pesticide summary
    let query = supabase
      .from('company_pesticide_summary')
      .select(`
        cvr_number,
        company_name,
        municipality,
        application_year,
        total_belastning,
        pfas_belastning,
        diquat_belastning,
        glyphosate_belastning,
        total_applications,
        unique_products,
        total_treated_area_ha
      `)

    // Apply filters
    if (params.geography !== 'country') {
      query = query.eq('municipality', params.geography)
    }

    if (params.year !== 'all' && params.year) {
      query = query.eq('application_year', params.year)
    }

    if (params.cvr) {
      query = query.eq('cvr_number', parseInt(params.cvr))
    }

    // Apply sorting
    let sortColumn = 'total_belastning'
    switch (params.sortBy) {
      case 'applications':
        sortColumn = 'total_applications'
        break
      case 'area':
        sortColumn = 'total_treated_area_ha'
        break
      default:
        // Handle different burden types for sorting
        if (params.type === 'pfas') sortColumn = 'pfas_belastning'
        else if (params.type === 'diquat') sortColumn = 'diquat_belastning'
        else if (params.type === 'glyphosate') sortColumn = 'glyphosate_belastning'
        break
    }

    query = query.order(sortColumn, { ascending: params.sortOrder === 'asc' })

    // Get total count for pagination
    const { count } = await supabase
      .from('company_pesticide_summary')
      .select('*', { count: 'exact', head: true })

    // Apply pagination
    const offset = (params.page - 1) * params.limit
    query = query.range(offset, offset + params.limit - 1)

    const { data: rawData, error } = await query

    if (error) {
      throw error
    }

    // Group data by company (since we might have multiple years per company)
    const companyMap = new Map<string, CompanySummary>()

    for (const row of rawData || []) {
      const cvr = row.cvr_number.toString()

      if (companyMap.has(cvr)) {
        const existing = companyMap.get(cvr)!
        existing.total_belastning += row.total_belastning || 0
        existing.pfas_belastning += row.pfas_belastning || 0
        existing.diquat_belastning += row.diquat_belastning || 0
        existing.glyphosate_belastning += row.glyphosate_belastning || 0
        existing.total_applications += row.total_applications || 0
        existing.unique_products += row.unique_products || 0
        existing.total_treated_area_ha += row.total_treated_area_ha || 0
        existing.years_active.push(row.application_year)
      } else {
        companyMap.set(cvr, {
          cvr_number: cvr,
          company_name: row.company_name || `Company ${cvr}`,
          municipality: row.municipality || 'Unknown',
          total_belastning: row.total_belastning || 0,
          pfas_belastning: row.pfas_belastning || 0,
          diquat_belastning: row.diquat_belastning || 0,
          glyphosate_belastning: row.glyphosate_belastning || 0,
          total_applications: row.total_applications || 0,
          unique_products: row.unique_products || 0,
          total_treated_area_ha: row.total_treated_area_ha || 0,
          years_active: [row.application_year]
        })
      }
    }

    const companies = Array.from(companyMap.values())

    // Filter by pesticide type if specified
    let filteredCompanies = companies
    if (params.type !== 'total') {
      filteredCompanies = companies.filter(company => {
        switch (params.type) {
          case 'pfas':
            return company.pfas_belastning > 0
          case 'diquat':
            return company.diquat_belastning > 0
          case 'glyphosate':
            return company.glyphosate_belastning > 0
          default:
            return true
        }
      })
    }

    // Get filter metadata
    const { data: yearData } = await supabase
      .from('company_pesticide_summary')
      .select('application_year')
      .order('application_year')

    const { data: municipalityData } = await supabase
      .from('company_pesticide_summary')
      .select('municipality')
      .not('municipality', 'is', null)

    const availableYears = [...new Set(yearData?.map(row => row.application_year) || [])]
    const availableMunicipalities = [...new Set(municipalityData?.map(row => row.municipality) || [])]

    const response: PesticideAnalysisResponse = {
      companies: filteredCompanies,
      total_count: count || 0,
      page: params.page,
      limit: params.limit,
      filters: {
        available_years: availableYears,
        available_municipalities: availableMunicipalities,
        total_companies: companyMap.size
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

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

    // Get all data without pagination to properly group and sort
    let baseQuery = supabase
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

    // Apply filters to base query
    if (params.geography !== 'country') {
      baseQuery = baseQuery.eq('municipality', params.geography)
    }

    if (params.years && params.years.length > 0) {
      baseQuery = baseQuery.in('application_year', params.years)
    }

    if (params.cvr) {
      baseQuery = baseQuery.eq('cvr_number', parseInt(params.cvr))
    }

    const { data: rawData, error } = await baseQuery

    if (error) {
      throw error
    }

    // Group data by company (since we might have multiple years per company)
    const companyMap = new Map<string, CompanySummary & { year_data: Array<{year: number, area: number}> }>()

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
        existing.years_active.push(row.application_year)
        existing.year_data.push({ year: row.application_year, area: row.total_treated_area_ha || 0 })
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
          total_treated_area_ha: 0, // Will be set below based on most recent year
          years_active: [row.application_year],
          year_data: [{ year: row.application_year, area: row.total_treated_area_ha || 0 }]
        })
      }
    }

    // Convert to final format and set area from most recent year
    let companies = Array.from(companyMap.values()).map(company => {
      // Find the most recent year's area data
      const mostRecentYear = Math.max(...company.year_data.map(yd => yd.year))
      const mostRecentYearData = company.year_data.find(yd => yd.year === mostRecentYear)

      return {
        cvr_number: company.cvr_number,
        company_name: company.company_name,
        municipality: '', // Keep for API compatibility but not displayed
        total_belastning: company.total_belastning,
        pfas_belastning: company.pfas_belastning,
        diquat_belastning: company.diquat_belastning,
        glyphosate_belastning: company.glyphosate_belastning,
        total_applications: company.total_applications,
        unique_products: company.unique_products,
        total_treated_area_ha: mostRecentYearData?.area || 0, // Use most recent year's area
        years_active: [...new Set(company.years_active)].sort((a, b) => b - a) // Remove duplicates and sort
      }
    })

    // Filter by pesticide type if specified
    if (params.type !== 'total') {
      companies = companies.filter(company => {
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

    // Apply sorting to grouped companies
    companies.sort((a, b) => {
      let aValue, bValue

      switch (params.sortBy) {
        case 'applications':
          aValue = a.total_applications
          bValue = b.total_applications
          break
        case 'area':
          aValue = a.total_treated_area_ha
          bValue = b.total_treated_area_ha
          break
        default:
          // Handle different burden types for sorting
          if (params.type === 'pfas') {
            aValue = a.pfas_belastning
            bValue = b.pfas_belastning
          } else if (params.type === 'diquat') {
            aValue = a.diquat_belastning
            bValue = b.diquat_belastning
          } else if (params.type === 'glyphosate') {
            aValue = a.glyphosate_belastning
            bValue = b.glyphosate_belastning
          } else {
            aValue = a.total_belastning
            bValue = b.total_belastning
          }
          break
      }

      return params.sortOrder === 'asc' ? aValue - bValue : bValue - aValue
    })

    // Get total count after filtering
    const totalCount = companies.length

    // Apply pagination to sorted companies
    const offset = (params.page - 1) * params.limit
    const paginatedCompanies = companies.slice(offset, offset + params.limit)

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

    // Calculate correct statistics based on all companies (before type filtering)
    const allCompaniesForStats = Array.from(companyMap.values()).map(company => {
      const mostRecentYear = Math.max(...company.year_data.map(yd => yd.year))
      const mostRecentYearData = company.year_data.find(yd => yd.year === mostRecentYear)

      return {
        cvr_number: company.cvr_number,
        company_name: company.company_name,
        municipality: company.municipality,
        total_belastning: company.total_belastning,
        pfas_belastning: company.pfas_belastning,
        diquat_belastning: company.diquat_belastning,
        glyphosate_belastning: company.glyphosate_belastning,
        total_applications: company.total_applications,
        unique_products: company.unique_products,
        total_treated_area_ha: mostRecentYearData?.area || 0,
        years_active: [...new Set(company.years_active)].sort((a, b) => b - a)
      }
    })

    const companiesWithPfas = allCompaniesForStats.filter(c => c.pfas_belastning > 0).length
    const companiesWithDiquat = allCompaniesForStats.filter(c => c.diquat_belastning > 0).length
    const companiesWithGlyphosate = allCompaniesForStats.filter(c => c.glyphosate_belastning > 0).length

    const response: PesticideAnalysisResponse = {
      companies: paginatedCompanies,
      total_count: totalCount,
      page: params.page,
      limit: params.limit,
      filters: {
        available_years: availableYears,
        available_municipalities: availableMunicipalities,
        total_companies: allCompaniesForStats.length,
        companies_with_pfas: companiesWithPfas,
        companies_with_diquat: companiesWithDiquat,
        companies_with_glyphosate: companiesWithGlyphosate
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

import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

interface CompanyDetailsResponse {
  company: {
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
  };
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

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders })
  }

  try {
    const url = new URL(req.url)
    const cvrParam = url.searchParams.get('cvr')
    const yearParam = url.searchParams.get('year')

    if (!cvrParam) {
      return new Response(
        JSON.stringify({ error: 'CVR parameter is required' }),
        {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 400,
        },
      )
    }

    const cvr = parseInt(cvrParam)
    const year = yearParam ? parseInt(yearParam) : null

    // Create Supabase client
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!
    const supabaseKey = Deno.env.get('SUPABASE_ANON_KEY')!
    const supabase = createClient(supabaseUrl, supabaseKey)

    // Get company summary data
    let summaryQuery = supabase
      .from('company_pesticide_summary')
      .select('*')
      .eq('cvr_number', cvr)

    if (year) {
      summaryQuery = summaryQuery.eq('application_year', year)
    }

    const { data: summaryData, error: summaryError } = await summaryQuery

    if (summaryError) {
      throw summaryError
    }

    if (!summaryData || summaryData.length === 0) {
      return new Response(
        JSON.stringify({ error: 'Company not found' }),
        {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 404,
        },
      )
    }

    // Aggregate company data across years
    const companyData = summaryData.reduce((acc, row) => {
      return {
        cvr_number: row.cvr_number.toString(),
        company_name: row.company_name || `Company ${row.cvr_number}`,
        municipality: row.municipality || 'Unknown',
        total_belastning: (acc.total_belastning || 0) + (row.total_belastning || 0),
        pfas_belastning: (acc.pfas_belastning || 0) + (row.pfas_belastning || 0),
        diquat_belastning: (acc.diquat_belastning || 0) + (row.diquat_belastning || 0),
        glyphosate_belastning: (acc.glyphosate_belastning || 0) + (row.glyphosate_belastning || 0),
        total_applications: (acc.total_applications || 0) + (row.total_applications || 0),
        unique_products: Math.max(acc.unique_products || 0, row.unique_products || 0),
        total_treated_area_ha: (acc.total_treated_area_ha || 0) + (row.total_treated_area_ha || 0),
      }
    }, {} as any)

    // Get yearly breakdown with product details
    const { data: applicationData, error: applicationError } = await supabase
      .from('company_pesticide_applications')
      .select(`
        application_year,
        pesticide_name,
        registration_number,
        total_burden_score,
        dosage_quantity,
        contains_pfas,
        contains_diquat,
        contains_glyphosate
      `)
      .eq('cvr_number', cvr)
      .order('application_year', { ascending: false })

    if (applicationError) {
      throw applicationError
    }

    // Group applications by year and product
    const yearlyBreakdown = new Map<number, any>()

    for (const app of applicationData || []) {
      const year = app.application_year

      if (!yearlyBreakdown.has(year)) {
        yearlyBreakdown.set(year, {
          year,
          total_belastning: 0,
          pfas_belastning: 0,
          diquat_belastning: 0,
          glyphosate_belastning: 0,
          total_applications: 0,
          applications_by_product: new Map<string, any>(),
        })
      }

      const yearData = yearlyBreakdown.get(year)
      const burden = (app.total_burden_score || 0) * (app.dosage_quantity || 0)

      yearData.total_belastning += burden
      yearData.total_applications += 1

      if (app.contains_pfas) yearData.pfas_belastning += burden
      if (app.contains_diquat) yearData.diquat_belastning += burden
      if (app.contains_glyphosate) yearData.glyphosate_belastning += burden

      // Group by product
      const productKey = app.registration_number || app.pesticide_name || 'Unknown'
      if (!yearData.applications_by_product.has(productKey)) {
        yearData.applications_by_product.set(productKey, {
          product_name: app.pesticide_name || 'Unknown Product',
          registration_number: app.registration_number || '',
          applications: 0,
          total_belastning: 0,
          contains_pfas: app.contains_pfas || false,
          contains_diquat: app.contains_diquat || false,
          contains_glyphosate: app.contains_glyphosate || false,
        })
      }

      const productData = yearData.applications_by_product.get(productKey)
      productData.applications += 1
      productData.total_belastning += burden
    }

    // Convert maps to arrays and sort
    const yearlyBreakdownArray = Array.from(yearlyBreakdown.values()).map(yearData => ({
      ...yearData,
      applications_by_product: Array.from(yearData.applications_by_product.values())
        .sort((a, b) => b.total_belastning - a.total_belastning)
    }))

    // Get municipality ranking
    const municipality = companyData.municipality
    const { data: municipalityRankData, error: rankError } = await supabase
      .from('municipality_pesticide_summary')
      .select('*')
      .eq('municipality', municipality)
      .order('total_belastning', { ascending: false })

    if (rankError) {
      console.warn('Could not get municipality ranking:', rankError)
    }

    const municipalityRanking = {
      rank: 1, // Default values
      total_companies_in_municipality: 1,
      percentile: 100,
    }

    // Calculate ranking if we have data
    if (municipalityRankData && municipalityRankData.length > 0) {
      const municipalityData = municipalityRankData[0]
      municipalityRanking.total_companies_in_municipality = municipalityData.company_count || 1

      // Simple ranking calculation (would need more sophisticated logic for real ranking)
      municipalityRanking.rank = Math.ceil(municipalityData.company_count * 0.1) // Assume top 10%
      municipalityRanking.percentile = 90 // Assume 90th percentile
    }

    const response: CompanyDetailsResponse = {
      company: companyData,
      yearly_breakdown: yearlyBreakdownArray,
      municipality_ranking: municipalityRanking,
    }

    return new Response(
      JSON.stringify(response),
      {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 200,
      },
    )

  } catch (error) {
    console.error('Error in pesticide-company-details:', error)
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

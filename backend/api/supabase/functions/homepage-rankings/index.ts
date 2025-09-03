import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

interface RankingItem {
  company_id: string;
  cvr_number: string;
  company_name: string;
  municipality?: string;
  rank: number;
  value: number;
  formatted_value: string;
  year?: number;
}

interface RankingTable {
  id: string;
  title: string;
  category: string;
  description: string;
  unit: string;
  items: RankingItem[];
  company_count: number;
  last_updated?: string;
}

interface HomepageRankingsResponse {
  rankings: RankingTable[];
  metadata: {
    generated_at: string;
    total_tables: number;
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
    const category = url.searchParams.get('category') // 'all', 'financial', 'field', 'environment', 'animal', 'worker'
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '20'), 50) // Max 50 per table
    const rankingId = url.searchParams.get('rankingId') // Optional: fetch only specific ranking

    // Create Supabase client
    const supabaseUrl = Deno.env.get('SUPABASE_URL')!
    const supabaseKey = Deno.env.get('SUPABASE_ANON_KEY')!
    const supabase = createClient(supabaseUrl, supabaseKey)

    const rankings: RankingTable[] = []

    // Financial Rankings
    if (!category || category === 'all' || category === 'financial') {
      // 1. Highest Profit
      const { data: profitData } = await supabase
        .from('yearly_financials')
        .select(`
          company_id,
          cvr_number,
          net_profit_loss,
          year,
          companies!inner(company_name, municipality)
        `)
        .not('net_profit_loss', 'is', null)
        .eq('year', 2024) // Use most recent complete year
        .order('net_profit_loss', { ascending: false })
        .limit(limit)

      const { count: profitCount } = await supabase
        .from('yearly_financials')
        .select('company_id', { count: 'exact' })
        .not('net_profit_loss', 'is', null)
        .eq('year', 2024)

      if (profitData) {
        rankings.push({
          id: 'highest_profit',
          title: 'Højest Overskud',
          category: 'financial',
          description: 'Virksomheder med det højeste nettoresultat i 2024',
          unit: 'DKK',
          company_count: profitCount || 0,
          items: profitData
            .filter(item => item.net_profit_loss > 0) // Only include companies with positive profit
            .map((item, index) => ({
              company_id: item.company_id,
              cvr_number: item.cvr_number.toString(),
              company_name: item.companies.company_name,
              municipality: item.companies.municipality,
              rank: index + 1,
              value: item.net_profit_loss,
              formatted_value: `${(item.net_profit_loss / 1000000).toFixed(1)}M kr`,
              year: item.year
            }))
        })
      }

      // 2. Largest Assets
      const { data: assetsData } = await supabase
        .from('yearly_financials')
        .select(`
          company_id,
          cvr_number,
          total_assets,
          year,
          companies!inner(company_name, municipality)
        `)
        .not('total_assets', 'is', null)
        .gt('total_assets', 0)
        .eq('year', 2024)
        .order('total_assets', { ascending: false })
        .limit(limit)

      const { count: assetsCount } = await supabase
        .from('yearly_financials')
        .select('company_id', { count: 'exact' })
        .not('total_assets', 'is', null)
        .gt('total_assets', 0)
        .eq('year', 2024)

      if (assetsData) {
        rankings.push({
          id: 'largest_assets',
          title: 'Størst Aktiver',
          category: 'financial',
          description: 'Virksomheder med de største samlede aktiver i 2024',
          unit: 'DKK',
          company_count: assetsCount || 0,
          items: assetsData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.cvr_number.toString(),
            company_name: item.companies.company_name,
            municipality: item.companies.municipality,
            rank: index + 1,
            value: item.total_assets,
            formatted_value: `${(item.total_assets / 1000000).toFixed(1)}M kr`,
            year: item.year
          }))
        })
      }

      // 3. Most Employees (Financial data)
      const { data: employeesData } = await supabase
        .from('yearly_financials')
        .select(`
          company_id,
          cvr_number,
          average_number_of_employees,
          year,
          companies!inner(company_name, municipality)
        `)
        .not('average_number_of_employees', 'is', null)
        .gt('average_number_of_employees', 0)
        .eq('year', 2024)
        .order('average_number_of_employees', { ascending: false })
        .limit(limit)

      const { count: employeesCount } = await supabase
        .from('yearly_financials')
        .select('company_id', { count: 'exact' })
        .not('average_number_of_employees', 'is', null)
        .gt('average_number_of_employees', 0)
        .eq('year', 2024)

      if (employeesData) {
        rankings.push({
          id: 'most_employees_financial',
          title: 'Flest Ansatte',
          category: 'financial',
          description: 'Virksomheder med flest ansatte ifølge regnskabsdata 2024',
          unit: 'ansatte',
          company_count: employeesCount || 0,
          items: employeesData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.cvr_number.toString(),
            company_name: item.companies.company_name,
            municipality: item.companies.municipality,
            rank: index + 1,
            value: item.average_number_of_employees,
            formatted_value: `${item.average_number_of_employees} ansatte`,
            year: item.year
          }))
        })
      }
    }

    // Agricultural Area Rankings
    if (!category || category === 'all' || category === 'field') {
      // 4. Largest Agricultural Area
      const { data: landAreaData } = await supabase
        .from('land_use_summary')
        .select(`
          company_id,
          total_area_ha,
          rank_dk_total_area,
          year
        `)
        .eq('year', 2025)
        .gt('total_area_ha', 0) // Only include companies with actual land area
        .order('rank_dk_total_area', { ascending: true })
        .limit(limit)

      const { count: landAreaCount } = await supabase
        .from('land_use_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2025)
        .gt('total_area_ha', 0)

      if (landAreaData?.length) {
        // Step 2: Get company details for the land area data
        const companyIds = landAreaData.map(item => item.company_id)
        const { data: landAreaCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(landAreaCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'largest_land_area',
          title: 'Størst Landbrugsareal',
          category: 'field',
          description: 'Virksomheder med det største samlede landbrugsareal i 2025',
          unit: 'hektar',
          company_count: landAreaCount || 0,
          items: landAreaData.map((item) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: item.rank_dk_total_area,
              value: item.total_area_ha,
              formatted_value: `${item.total_area_ha.toFixed(1)} ha`,
              year: item.year
            }
          })
        })
      }

      // 5. Largest Organic Area
      const { data: organicAreaData } = await supabase
        .from('land_use_summary')
        .select(`
          company_id,
          organic_area_ha,
          year
        `)
        .eq('year', 2025)
        .gt('organic_area_ha', 0)
        .order('organic_area_ha', { ascending: false })
        .limit(limit)

      const { count: organicAreaCount } = await supabase
        .from('land_use_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2025)
        .gt('organic_area_ha', 0)

      if (organicAreaData?.length) {
        // Step 2: Get company details for the organic area data
        const companyIds = organicAreaData.map(item => item.company_id)
        const { data: organicAreaCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(organicAreaCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'largest_organic_area',
          title: 'Størst Økologisk Areal',
          category: 'field',
          description: 'Virksomheder med det største økologiske landbrugsareal i 2024',
          unit: 'hektar',
          company_count: organicAreaCount || 0,
          items: organicAreaData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.organic_area_ha,
              formatted_value: `${item.organic_area_ha.toFixed(1)} ha`,
              year: 2024
            }
          })
        })
      }

      // 6. Highest Organic Percentage
      const { data: organicPercentData } = await supabase
        .from('land_use_summary')
        .select(`
          company_id,
          organic_percentage,
          total_area_ha,
          year
        `)
        .eq('year', 2025)
        .gt('total_area_ha', 50) // Only companies with substantial land
        .gt('organic_percentage', 0)
        .order('organic_percentage', { ascending: false })
        .order('total_area_ha', { ascending: false })
        .limit(limit)

      const { count: organicPercentCount } = await supabase
        .from('land_use_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2025)
        .gt('total_area_ha', 50)
        .gt('organic_percentage', 0)

      if (organicPercentData?.length) {
        // Step 2: Get company details for the organic percentage data
        const companyIds = organicPercentData.map(item => item.company_id)
        const { data: organicPercentCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(organicPercentCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'highest_organic_percentage',
          title: 'Højest Økologisk Andel',
          category: 'field',
          description: 'Virksomheder med den højeste andel økologisk landbrug (min. 50 ha) i 2024',
          unit: 'procent',
          company_count: organicPercentCount || 0,
          items: organicPercentData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.organic_percentage,
              formatted_value: `${item.organic_percentage.toFixed(1)}%`,
              year: 2024
            }
          })
        })
      }

      // 7. Most Fields
      const { data: fieldsData } = await supabase
        .from('land_use_summary')
        .select(`
          company_id,
          total_fields,
          year
        `)
        .eq('year', 2025)
        .gt('total_fields', 0) // Only include companies with actual fields
        .order('total_fields', { ascending: false })
        .limit(limit)

      const { count: fieldsCount } = await supabase
        .from('land_use_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2025)
        .gt('total_fields', 0)

      if (fieldsData?.length) {
        // Step 2: Get company details for the fields data
        const companyIds = fieldsData.map(item => item.company_id)
        const { data: fieldsCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(fieldsCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'most_fields',
          title: 'Flest Marker',
          category: 'field',
          description: 'Virksomheder med det største antal individuelle marker i 2025',
          unit: 'marker',
          company_count: fieldsCount || 0,
          items: fieldsData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.total_fields,
              formatted_value: `${item.total_fields} marker`,
              year: item.year
            }
          })
        })
      }
    }

    // Environment Rankings
    if (!category || category === 'all' || category === 'environment') {
      // 8. Highest Pesticide Burden
      const { data: pesticideData } = await supabase
        .from('company_pesticide_summary')
        .select(`
          company_id,
          cvr_number,
          company_name,
          municipality,
          total_belastning,
          application_year
        `)
        .eq('application_year', 2024)
        .order('total_belastning', { ascending: false })
        .limit(limit)

      const { count: pesticideCount } = await supabase
        .from('company_pesticide_summary')
        .select('company_id', { count: 'exact' })
        .eq('application_year', 2024)

      if (pesticideData) {
        rankings.push({
          id: 'highest_pesticide_burden',
          title: 'Højest Pesticidbelastning',
          category: 'environment',
          description: 'Virksomheder med den højeste samlede pesticidbelastning i 2024',
          unit: 'belastningsenheder',
          company_count: pesticideCount || 0,
          items: pesticideData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.cvr_number.toString(),
            company_name: item.company_name,
            municipality: item.municipality,
            rank: index + 1,
            value: item.total_belastning,
            formatted_value: `${item.total_belastning.toFixed(1)} BE`,
            year: item.application_year
          }))
        })
      }

      // 9. Most PFAS Usage
      const { data: pfasData } = await supabase
        .from('company_pesticide_summary')
        .select(`
          company_id,
          cvr_number,
          company_name,
          municipality,
          pfas_belastning,
          application_year
        `)
        .eq('application_year', 2024)
        .gt('pfas_belastning', 0)
        .order('pfas_belastning', { ascending: false })
        .limit(limit)

      const { count: pfasCount } = await supabase
        .from('company_pesticide_summary')
        .select('company_id', { count: 'exact' })
        .eq('application_year', 2024)
        .gt('pfas_belastning', 0)

      if (pfasData) {
        rankings.push({
          id: 'most_pfas_usage',
          title: 'Højest PFAS-forbrug',
          category: 'environment',
          description: 'Virksomheder med det højeste forbrug af PFAS-holdige pesticider i 2024',
          unit: 'belastningsenheder',
          company_count: pfasCount || 0,
          items: pfasData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.cvr_number.toString(),
            company_name: item.company_name,
            municipality: item.municipality,
            rank: index + 1,
            value: item.pfas_belastning,
            formatted_value: `${item.pfas_belastning.toFixed(1)} BE`,
            year: item.application_year
          }))
        })
      }

      // 10. Most Glyphosate Usage
      const { data: glyphosateData } = await supabase
        .from('company_pesticide_summary')
        .select(`
          company_id,
          cvr_number,
          company_name,
          municipality,
          glyphosate_belastning,
          application_year
        `)
        .eq('application_year', 2024)
        .gt('glyphosate_belastning', 0)
        .order('glyphosate_belastning', { ascending: false })
        .limit(limit)

      const { count: glyphosateCount } = await supabase
        .from('company_pesticide_summary')
        .select('company_id', { count: 'exact' })
        .eq('application_year', 2024)
        .gt('glyphosate_belastning', 0)

      if (glyphosateData) {
        rankings.push({
          id: 'most_glyphosate_usage',
          title: 'Højest Glyphosatforbrug',
          category: 'environment',
          description: 'Virksomheder med det højeste glyphosatforbrug i 2024',
          unit: 'belastningsenheder',
          company_count: glyphosateCount || 0,
          items: glyphosateData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.cvr_number.toString(),
            company_name: item.company_name,
            municipality: item.municipality,
            rank: index + 1,
            value: item.glyphosate_belastning,
            formatted_value: `${item.glyphosate_belastning.toFixed(1)} BE`,
            year: item.application_year
          }))
        })
      }

      // 11. Most Diquat Usage
      const { data: diquatData } = await supabase
        .from('company_pesticide_summary')
        .select(`
          company_id,
          cvr_number,
          company_name,
          municipality,
          diquat_belastning,
          application_year
        `)
        .eq('application_year', 2024)
        .gt('diquat_belastning', 0)
        .order('diquat_belastning', { ascending: false })
        .limit(limit)

      const { count: diquatCount } = await supabase
        .from('company_pesticide_summary')
        .select('company_id', { count: 'exact' })
        .eq('application_year', 2024)
        .gt('diquat_belastning', 0)

      if (diquatData) {
        rankings.push({
          id: 'most_diquat_usage',
          title: 'Højest Diquatforbrug',
          category: 'environment',
          description: 'Virksomheder med det højeste diquatforbrug i 2024',
          unit: 'belastningsenheder',
          company_count: diquatCount || 0,
          items: diquatData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.cvr_number.toString(),
            company_name: item.company_name,
            municipality: item.municipality,
            rank: index + 1,
            value: item.diquat_belastning,
            formatted_value: `${item.diquat_belastning.toFixed(1)} BE`,
            year: item.application_year
          }))
        })
      }

      // 12. Most BNBO Area Not Completed AND Not Covered by Projects
      console.log('🔍 Querying BNBO Action Required data...')
      const { data: bnboNotDealtData, error: bnboError1 } = await supabase
        .from('bnbo_summary')
        .select(`
          company_id,
          area_ha,
          year
        `)
        .eq('status', 'Action Required')
        .eq('year', 2025)
        .order('area_ha', { ascending: false })
        .limit(limit)

      const { count: bnboNotDealtCount } = await supabase
        .from('bnbo_summary')
        .select('company_id', { count: 'exact' })
        .eq('status', 'Action Required')
        .eq('year', 2025)

      console.log('BNBO Action Required result:', { data: bnboNotDealtData?.length || 0, error: bnboError1 })

      if (bnboNotDealtData?.length) {
        // Step 2: Get company details for the BNBO data
        const companyIds = bnboNotDealtData.map(item => item.company_id)
        const { data: bnboCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(bnboCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'most_bnbo_not_dealt_with',
          title: 'Mest BNBO-areal Ikke Håndteret',
          category: 'environment',
          description: 'Virksomheder med mest boringsnært beskyttelsesområde-areal der kræver handling i 2025',
          unit: 'hektar',
          company_count: bnboNotDealtCount || 0,
          items: bnboNotDealtData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.area_ha,
              formatted_value: `${item.area_ha.toFixed(1)} ha`,
              year: item.year
            }
          })
        })
      }

      // 13. Most BNBO Area Dealt With OR Covered by Projects
      console.log('🔍 Querying BNBO Completed data...')
      const { data: bnboCompletedData, error: bnboError2 } = await supabase
        .from('bnbo_summary')
        .select(`
          company_id,
          area_ha,
          year
        `)
        .in('status', ['Completed', 'Action Required, Completed'])
        .eq('year', 2025)
        .order('area_ha', { ascending: false })
        .limit(limit)

      const { count: bnboCompletedCount } = await supabase
        .from('bnbo_summary')
        .select('company_id', { count: 'exact' })
        .in('status', ['Completed', 'Action Required, Completed'])
        .eq('year', 2025)

      console.log('BNBO Completed result:', { data: bnboCompletedData?.length || 0, error: bnboError2 })

      if (bnboCompletedData?.length) {
        // Step 2: Get company details for the BNBO completed data
        const companyIds = bnboCompletedData.map(item => item.company_id)
        const { data: bnboCompletedCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(bnboCompletedCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'most_bnbo_dealt_with',
          title: 'Mest BNBO-areal Håndteret',
          category: 'environment',
          description: 'Virksomheder med mest boringsnært beskyttelsesområde-areal der er håndteret i 2025',
          unit: 'hektar',
          company_count: bnboCompletedCount || 0,
          items: bnboCompletedData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.area_ha,
              formatted_value: `${item.area_ha.toFixed(1)} ha`,
              year: item.year
            }
          })
        })
      }

      // 14. Most Wetlands Not Restored (Action Required)
      console.log('🔍 Querying wetlands not restored data...')
      const { data: wetlandNotRestoredData, error: wetlandError1 } = await supabase
        .from('wetlands_environmental_status')
        .select(`
          company_id,
          action_required_hectares,
          year
        `)
        .eq('year', 2025)
        .gt('action_required_hectares', 0)
        .order('action_required_hectares', { ascending: false })
        .limit(limit)

      const { count: wetlandNotRestoredCount } = await supabase
        .from('wetlands_environmental_status')
        .select('company_id', { count: 'exact' })
        .eq('year', 2025)
        .gt('action_required_hectares', 0)

      console.log('Wetlands not restored result:', { data: wetlandNotRestoredData?.length || 0, error: wetlandError1 })

      if (wetlandNotRestoredData?.length) {
        // Step 2: Get company details for the wetlands data
        const companyIds = wetlandNotRestoredData.map(item => item.company_id)
        const { data: wetlandCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(wetlandCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'most_wetland_not_restored',
          title: 'Mest Lavbundsjorde Ikke Genoprettet',
          category: 'environment',
          description: 'Virksomheder med mest lavbundsjorde-areal der har behov for genopretning i 2025',
          unit: 'hektar',
          company_count: wetlandNotRestoredCount || 0,
          items: wetlandNotRestoredData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.action_required_hectares,
              formatted_value: `${item.action_required_hectares.toFixed(1)} ha`,
              year: item.year
            }
          })
        })
      }

      // 15. Most Wetlands Restored (Water Covered)
      console.log('🔍 Querying wetlands restored data...')
      const { data: wetlandRestoredData, error: wetlandError2 } = await supabase
        .from('wetlands_environmental_status')
        .select(`
          company_id,
          water_covered_hectares,
          year
        `)
        .eq('year', 2025)
        .gt('water_covered_hectares', 0)
        .order('water_covered_hectares', { ascending: false })
        .limit(limit)

      const { count: wetlandRestoredCount } = await supabase
        .from('wetlands_environmental_status')
        .select('company_id', { count: 'exact' })
        .eq('year', 2025)
        .gt('water_covered_hectares', 0)

      console.log('Wetlands restored result:', { data: wetlandRestoredData?.length || 0, error: wetlandError2 })

      if (wetlandRestoredData?.length) {
        // Step 2: Get company details for the wetlands restored data
        const companyIds = wetlandRestoredData.map(item => item.company_id)
        const { data: wetlandRestoredCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(wetlandRestoredCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'most_wetland_restored',
          title: 'Mest Lavbundsjorde Genoprettet',
          category: 'environment',
          description: 'Virksomheder med mest lavbundsjorde-areal der er genoprettet til naturlig vandstand i 2025',
          unit: 'hektar',
          company_count: wetlandRestoredCount || 0,
          items: wetlandRestoredData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.water_covered_hectares,
              formatted_value: `${item.water_covered_hectares.toFixed(1)} ha`,
              year: item.year
            }
          })
        })
      }
    }

    // Animal/Pig Focus Rankings
    if (!category || category === 'all' || category === 'animal') {
      // 16. Largest Pig Production (species_code 15 = pigs)
      const { data: pigData } = await supabase
        .from('site_species_production_ranked')
        .select(`
          chr,
          species_name,
          municipality,
          total_animals,
          rank_dk_species_production,
          year
        `)
        .eq('species_code', '15')
        .eq('year', 2025)
        .gt('total_animals', 0) // Only include sites with actual animals
        .order('rank_dk_species_production', { ascending: true })
        .limit(limit)

      const { count: pigCount } = await supabase
        .from('site_species_production_ranked')
        .select('chr', { count: 'exact' })
        .eq('species_code', '15')
        .eq('year', 2025)
        .gt('total_animals', 0)

      if (pigData) {
        // Get CVR numbers for CHR sites
        const chrList = pigData.map(item => item.chr)
        const { data: chrToCvr } = await supabase
          .from('production_sites')
          .select(`
            chr,
            companies!inner(id, company_name, cvr_number)
          `)
          .in('chr', chrList)

                const chrMap = new Map(chrToCvr?.map((item: any) => [
          item.chr.toString(),
          {
            cvr_number: item.companies?.cvr_number?.toString() || '',
            company_id: item.companies?.id || '',
            company_name: item.companies?.company_name || ''
          }
        ]) || [])

        rankings.push({
          id: 'largest_pig_production',
          title: 'Størst Svineproduktion',
          category: 'animal',
          description: 'Produktionssteder med den største svineproduktion i 2025',
          unit: 'svin',
          company_count: pigCount || 0,
          items: pigData
            .map((item) => {
              const companyInfo = chrMap.get(item.chr)
              return companyInfo ? {
                company_id: companyInfo.company_id,
                cvr_number: companyInfo.cvr_number,
                company_name: companyInfo.company_name,
                municipality: item.municipality,
                rank: item.rank_dk_species_production,
                value: item.total_animals,
                formatted_value: `${item.total_animals.toLocaleString()} svin`,
                year: item.year
              } : null
            })
            .filter(item => item !== null)
        })
      }

      // 17. Largest Cattle Production (species_code 12 = cattle)
      const { data: cattleData } = await supabase
        .from('site_species_production_ranked')
        .select(`
          chr,
          species_name,
          municipality,
          total_animals,
          rank_dk_species_production,
          year
        `)
        .eq('species_code', '12')
        .eq('year', 2025)
        .gt('total_animals', 0) // Only include sites with actual animals
        .order('rank_dk_species_production', { ascending: true })
        .limit(limit)

      const { count: cattleCount } = await supabase
        .from('site_species_production_ranked')
        .select('chr', { count: 'exact' })
        .eq('species_code', '12')
        .eq('year', 2025)
        .gt('total_animals', 0)

      if (cattleData) {
        const chrList = cattleData.map(item => item.chr)
        const { data: chrToCvr } = await supabase
          .from('production_sites')
          .select(`
            chr,
            companies!inner(id, company_name, cvr_number)
          `)
          .in('chr', chrList)

                const chrMap = new Map(chrToCvr?.map((item: any) => [
          item.chr.toString(),
          {
            cvr_number: item.companies?.cvr_number?.toString() || '',
            company_id: item.companies?.id || '',
            company_name: item.companies?.company_name || ''
          }
        ]) || [])

        rankings.push({
          id: 'largest_cattle_production',
          title: 'Størst Kvægproduktion',
          category: 'animal',
          description: 'Produktionssteder med den største kvægproduktion i 2024',
          unit: 'kvæg',
          company_count: cattleCount || 0,
          items: cattleData
            .map((item) => {
              const companyInfo = chrMap.get(item.chr)
              return companyInfo ? {
                company_id: companyInfo.company_id,
                cvr_number: companyInfo.cvr_number,
                company_name: companyInfo.company_name,
                municipality: item.municipality,
                rank: item.rank_dk_species_production,
                value: item.total_animals,
                formatted_value: `${item.total_animals.toLocaleString()} kvæg`,
                year: item.year
              } : null
            })
            .filter(item => item !== null)
        })
      }

      // 18. Highest Antibiotic Usage
      const { data: antibioticData } = await supabase
        .from('animal_welfare_summary')
        .select(`
          company_id,
          total_ddd_usage,
          year
        `)
        .eq('year', 2025)
        .gt('total_ddd_usage', 0)
        .order('total_ddd_usage', { ascending: false })
        .limit(limit)

      const { count: antibioticCount } = await supabase
        .from('antibiotic_usage_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2025)
        .gt('total_ddd_usage', 0)

      if (antibioticData?.length) {
        // Step 2: Get company details for the antibiotic data
        const companyIds = antibioticData.map(item => item.company_id)
        const { data: antibioticCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(antibioticCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'highest_antibiotic_usage',
          title: 'Højest Antibiotikaforbrug',
          category: 'animal',
          description: 'Virksomheder med det højeste antibiotikaforbrug i 2025',
          unit: 'DDD',
          company_count: antibioticCount || 0,
          items: antibioticData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.total_ddd_usage,
              formatted_value: `${item.total_ddd_usage.toLocaleString()} DDD`,
              year: item.year
            }
          })
        })
      }

      // 19. Most Production Sites
      const { data: sitesData } = await supabase
        .from('animal_welfare_summary')
        .select(`
          company_id,
          site_count,
          year
        `)
        .eq('year', 2025)
        .gt('site_count', 0) // Only include companies with actual production sites
        .order('site_count', { ascending: false })
        .limit(limit)

      const { count: sitesCount } = await supabase
        .from('animal_welfare_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2025)
        .gt('site_count', 0)

      if (sitesData?.length) {
        // Step 2: Get company details for the sites data
        const companyIds = sitesData.map(item => item.company_id)
        const { data: sitesCompanies } = await supabase
          .from('companies')
          .select('id, cvr_number, company_name, municipality')
          .in('id', companyIds)

        // Create company lookup map
        const companyMap = new Map(sitesCompanies?.map((c: any) => [c.id, c]) || [])

        rankings.push({
          id: 'most_production_sites',
          title: 'Flest Produktionssteder',
          category: 'animal',
          description: 'Virksomheder med flest dyreproduktionssteder i 2025',
          unit: 'steder',
          company_count: sitesCount || 0,
          items: sitesData.map((item, index) => {
            const company = companyMap.get(item.company_id)
            return {
              company_id: item.company_id,
              cvr_number: company?.cvr_number?.toString() || 'N/A',
              company_name: company?.company_name || 'Ukendt virksomhed',
              municipality: company?.municipality || 'Ukendt kommune',
              rank: index + 1,
              value: item.site_count,
              formatted_value: `${item.site_count} steder`,
              year: item.year
            }
          })
        })
      }

      // 20. Most Transported Pigs
      const { data: transportData } = await supabase
        .from('animal_transport_weekly_summary')
        .select(`
          company_id,
          animal_count,
          companies!inner(cvr_number, company_name, municipality)
        `)
        .gte('transport_date_week_start', '2025-01-01')
        .lt('transport_date_week_start', '2026-01-01')

      const { count: transportCount } = await supabase
        .from('animal_transport_weekly_summary')
        .select('company_id', { count: 'exact', head: true })
        .gte('transport_date_week_start', '2025-01-01')
        .lt('transport_date_week_start', '2026-01-01')

      if (transportData) {
        // Aggregate by company
        const companyTransports = new Map()
        transportData.forEach(item => {
          const key = item.company_id
          if (!companyTransports.has(key)) {
            companyTransports.set(key, {
              company_id: item.company_id,
              cvr_number: item.companies.cvr_number,
              company_name: item.companies.company_name,
              municipality: item.companies.municipality,
              total_animals: 0
            })
          }
          companyTransports.get(key).total_animals += item.animal_count
        })

        const sortedTransports = Array.from(companyTransports.values())
          .filter(company => company.total_animals > 0) // Only include companies with actual transports
          .sort((a, b) => b.total_animals - a.total_animals)
          .slice(0, limit)

        rankings.push({
          id: 'most_transported_pigs',
          title: 'Flest Transporterede Svin',
          category: 'animal',
          description: 'Virksomheder med flest transporterede svin i 2025',
          unit: 'svin',
          company_count: transportCount || 0,
          items: sortedTransports.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.cvr_number.toString(),
            company_name: item.company_name,
            municipality: item.municipality,
            rank: index + 1,
            value: item.total_animals,
            formatted_value: `${item.total_animals.toLocaleString()} svin`,
            year: 2025
          }))
        })
      }
    }

    // Worker Rankings
    if (!category || category === 'all' || category === 'worker') {
      // 21. Most Employees (Worker data)
      const { data: workerEmployeeData } = await supabase
        .from('worker_yearly_summary')
        .select(`
          company_id,
          average_employee_count,
          year,
          companies!inner(cvr_number, company_name, municipality)
        `)
        .eq('year', 2024)
        .not('average_employee_count', 'is', null)
        .gt('average_employee_count', 0) // Only include companies with actual employees
        .order('average_employee_count', { ascending: false })
        .limit(limit)

      const { count: workerEmployeeCount } = await supabase
        .from('worker_yearly_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2024)
        .not('average_employee_count', 'is', null)
        .gt('average_employee_count', 0)

      if (workerEmployeeData) {
        rankings.push({
          id: 'most_employees_worker',
          title: 'Flest Ansatte (Arbejdsmarkedsdata)',
          category: 'worker',
          description: 'Virksomheder med flest ansatte ifølge arbejdsmarkedsdata 2024',
          unit: 'ansatte',
          company_count: workerEmployeeCount || 0,
          items: workerEmployeeData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.companies?.cvr_number?.toString() || 'N/A',
            company_name: item.companies?.company_name || 'Ukendt virksomhed',
            municipality: item.companies?.municipality || 'Ukendt kommune',
            rank: index + 1,
            value: item.average_employee_count,
            formatted_value: `${item.average_employee_count} ansatte`,
            year: item.year
          }))
        })
      }

      // 22. Most Work Permits (using 2023 data as 2024 has no permits)
      const { data: visaData } = await supabase
        .from('worker_yearly_summary')
        .select(`
          company_id,
          active_visa_count,
          year,
          companies!inner(cvr_number, company_name, municipality)
        `)
        .eq('year', 2023)
        .gt('active_visa_count', 0)
        .order('active_visa_count', { ascending: false })
        .limit(limit)

      const { count: visaCount } = await supabase
        .from('worker_yearly_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2023)
        .gt('active_visa_count', 0)

      if (visaData) {
        rankings.push({
          id: 'most_foreign_workers',
          title: 'Flest Arbejdstilladelser',
          category: 'worker',
          description: 'Virksomheder med flest aktive arbejdstilladelser i 2023',
          unit: 'tilladelser',
          company_count: visaCount || 0,
          items: visaData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.companies?.cvr_number?.toString() || 'N/A',
            company_name: item.companies?.company_name || 'Ukendt virksomhed',
            municipality: item.companies?.municipality || 'Ukendt kommune',
            rank: index + 1,
            value: item.active_visa_count,
            formatted_value: `${item.active_visa_count} tilladelser`,
            year: item.year
          }))
        })
      }

      // 23. Most Work Injuries (text ranges like "1-5", "6", etc.)
      const { data: injuryData } = await supabase
        .from('worker_yearly_summary')
        .select(`
          company_id,
          injury_count_reported,
          year,
          companies!inner(cvr_number, company_name, municipality)
        `)
        .eq('year', 2024)
        .not('injury_count_reported', 'is', null)
        .neq('injury_count_reported', '')
        .neq('injury_count_reported', '0')
        .order('injury_count_reported', { ascending: false })
        .limit(limit)

      const { count: injuryCount } = await supabase
        .from('worker_yearly_summary')
        .select('company_id', { count: 'exact' })
        .eq('year', 2024)
        .not('injury_count_reported', 'is', null)
        .neq('injury_count_reported', '')
        .neq('injury_count_reported', '0')

      if (injuryData) {
        rankings.push({
          id: 'most_work_injuries',
          title: 'Flest Arbejdsulykker',
          category: 'worker',
          description: 'Virksomheder med rapporterede arbejdsulykker i 2024',
          unit: 'ulykker',
          company_count: injuryCount || 0,
          items: injuryData.map((item, index) => ({
            company_id: item.company_id,
            cvr_number: item.companies?.cvr_number?.toString() || 'N/A',
            company_name: item.companies?.company_name || 'Ukendt virksomhed',
            municipality: item.companies?.municipality || 'Ukendt kommune',
            rank: index + 1,
            value: item.injury_count_reported,
            formatted_value: `${item.injury_count_reported} ulykker`,
            year: item.year
          }))
        })
      }
    }

    // Filter to specific ranking if requested
    const filteredRankings = rankingId
      ? rankings.filter(ranking => ranking.id === rankingId)
      : rankings

    const response: HomepageRankingsResponse = {
      rankings: filteredRankings,
      metadata: {
        generated_at: new Date().toISOString(),
        total_tables: filteredRankings.length
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
    console.error('Error in homepage-rankings:', error)
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

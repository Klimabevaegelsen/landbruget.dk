import { serve } from "https://deno.land/std@0.168.0/http/server.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
}

interface HomepageStatistics {
  stat_type: string
  total_data_points: number
  total_companies: number
  last_updated: string
}

serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders })
  }

  try {
    // Create Supabase client
    const supabaseClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_ANON_KEY') ?? '',
    )

    // Query the homepage statistics materialized view
    const { data: stats, error } = await supabaseClient
      .from('homepage_statistics')
      .select('*')
      .eq('stat_type', 'homepage_stats')
      .single()

    if (error) {
      console.error('Database error:', error)
      return new Response(
        JSON.stringify({
          error: 'Failed to fetch homepage statistics',
          details: error.message
        }),
        {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 500,
        },
      )
    }

    if (!stats) {
      return new Response(
        JSON.stringify({
          error: 'No homepage statistics found',
          message: 'The homepage_statistics materialized view may need to be refreshed'
        }),
        {
          headers: { ...corsHeaders, 'Content-Type': 'application/json' },
          status: 404,
        },
      )
    }

    // Format the response
    const response = {
      total_data_points: stats.total_data_points,
      total_companies: stats.total_companies,
      last_updated: stats.last_updated,
      formatted: {
        data_points: stats.total_data_points.toLocaleString('da-DK'),
        companies: stats.total_companies.toLocaleString('da-DK')
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
    console.error('Unexpected error:', error)
    return new Response(
      JSON.stringify({
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error'
      }),
      {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' },
        status: 500,
      },
    )
  }
})

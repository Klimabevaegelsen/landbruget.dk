import { serve } from 'std/http/server';
import { createClient, SupabaseClient } from '@supabase/supabase-js';

// Define type for basic company info
type BasicCompanyInfo = {
  id: string;
  municipality: string;
  cvr_number: string;
  company_name?: string;
  address?: string;
  address_geom?: any;
};

// --- Helper: Get Basic Company Details (Lookup by ID - UUID) ---
async function getBasicCompanyDetails(supabase: SupabaseClient, companyId: string): Promise<BasicCompanyInfo | null> {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (!uuidRegex.test(companyId)) {
    console.warn(`Received invalid format for company ID: ${companyId}`);
    return null;
  }

  const { data, error } = await supabase
    .from('companies')
    .select('id, municipality, cvr_number, company_name, address, address_geom')
    .eq('id', companyId)
    .maybeSingle();

  if (error) {
    console.error(`Error fetching basic company details for ID ${companyId}:`, error);
    throw new Error(`Database error fetching basic company details.`);
  }

  return data || null;
}

// --- Main Request Handler ---
serve(async (req) => {
  // CORS preflight
  if (req.method === 'OPTIONS') {
    return new Response('ok', {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type'
      }
    });
  }

  // Only allow GET requests
  if (req.method !== 'GET') {
    return new Response(JSON.stringify({
      error: 'Method not allowed'
    }), {
      status: 405,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }

  const url = new URL(req.url);
  const companyIdParam = url.searchParams.get('id');

  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*'
  };

  if (!companyIdParam) {
    return new Response(JSON.stringify({
      error: 'Company ID (UUID) query parameter is required'
    }), {
      status: 400,
      headers
    });
  }

  try {
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const supabaseKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

    if (!supabaseUrl || !supabaseKey) {
      throw new Error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.');
    }

    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: {
        fetch: fetch.bind(globalThis)
      }
    });

    const companyInfo = await getBasicCompanyDetails(supabase, companyIdParam);

    if (!companyInfo) {
      const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
      const errorMsg = uuidRegex.test(companyIdParam)
        ? `Company with ID ${companyIdParam} not found`
        : `Invalid Company ID format provided`;

      return new Response(JSON.stringify({
        error: errorMsg
      }), {
        status: 404,
        headers
      });
    }

    // Construct Basic Response
    const responseBody = {
      metadata: {
        api_version: "1.0.0",
        generated_at: new Date().toISOString(),
        company_id: companyInfo.id,
        company_cvr: companyInfo.cvr_number,
        municipality: companyInfo.municipality
      },
      company: companyInfo
    };

    return new Response(JSON.stringify(responseBody, null, 2), {
      headers
    });
  } catch (error) {
    const err = error as Error;
    console.error('Critical error in basic company edge function:', err);

    const errorHeaders = {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*'
    };

    return new Response(JSON.stringify({
      error: `Internal Server Error: ${err.message}`
    }), {
      status: 500,
      headers: errorHeaders
    });
  }
});

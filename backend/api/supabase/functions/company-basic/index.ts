import { serve } from 'std/http/server';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { withSecurity, validateCompanyId, createErrorResponse, createSuccessResponse } from '../_shared/security.ts';

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
serve(withSecurity(async (req, rateLimitInfo) => {
  // Only allow GET requests
  if (req.method !== 'GET') {
    return createErrorResponse('Method not allowed', 405, rateLimitInfo);
  }

  const url = new URL(req.url);
  const companyIdParam = url.searchParams.get('id');

  // Validate company ID
  const validation = validateCompanyId(companyIdParam);
  if (!validation.valid) {
    return createErrorResponse(validation.error!, 400, rateLimitInfo);
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

    const companyInfo = await getBasicCompanyDetails(supabase, companyIdParam!);

    if (!companyInfo) {
      return createErrorResponse(`Company with ID ${companyIdParam} not found`, 404, rateLimitInfo);
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

    return createSuccessResponse(responseBody, rateLimitInfo);
  } catch (error) {
    const err = error as Error;
    console.error('Critical error in basic company edge function:', err);
    return createErrorResponse(`Internal Server Error: ${err.message}`, 500, rateLimitInfo);
  }
}));

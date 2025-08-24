import { serve } from 'std/http/server.ts';
import { createClient } from '@supabase/supabase-js';

// --- Types ---
interface SearchResult {
  id: string;
  name: string;
  cvr: string;
  address: string;
  type: string;
}

interface SearchResponse {
  results: SearchResult[];
  total: number;
  query: string;
  searchType?: string;
}

// --- Helper: Validate Search Query ---
function validateSearchQuery(query: string): { isValid: boolean; error?: string } {
  if (!query || typeof query !== 'string') {
    return { isValid: false, error: 'Search query is required' };
  }

  const trimmedQuery = query.trim();
  if (trimmedQuery.length < 2) {
    return { isValid: false, error: 'Search query must be at least 2 characters' };
  }

  if (trimmedQuery.length > 100) {
    return { isValid: false, error: 'Search query must be less than 100 characters' };
  }

  return { isValid: true };
}

// --- Helper: Detect Search Type ---
function detectSearchType(query: string): string {
  const trimmedQuery = query.trim();

  // Check if it's a CVR number (8 digits)
  if (/^\d{8}$/.test(trimmedQuery)) {
    return 'cvr';
  }

  // Check if it contains mostly numbers (could be partial CVR)
  if (/^\d+$/.test(trimmedQuery) && trimmedQuery.length <= 8) {
    return 'cvr_partial';
  }

  // Default to company name search
  return 'company_name';
}

// --- Helper: Build Search Query ---
function buildSearchQuery(supabase: any, query: string, searchType: string, limit: number = 20) {
  const trimmedQuery = query.trim();

  let dbQuery = supabase
    .from('companies')
    .select('id, company_name, cvr_number, address, city, municipality')
    .limit(limit);

  switch (searchType) {
    case 'cvr':
      // Exact CVR match
      dbQuery = dbQuery.eq('cvr_number', parseInt(trimmedQuery));
      break;

    case 'cvr_partial':
      // Partial CVR match (starts with)
      dbQuery = dbQuery.like('cvr_number', `${trimmedQuery}%`);
      break;

    case 'company_name':
    default:
      // Company name search (case-insensitive, contains)
      dbQuery = dbQuery.ilike('company_name', `%${trimmedQuery}%`);
      break;
  }

  return dbQuery;
}

// --- Helper: Format Search Results ---
function formatSearchResults(data: any[], query: string): SearchResult[] {
  return data.map(company => ({
    id: company.id,
    name: company.company_name,
    cvr: company.cvr_number?.toString() || '',
    address: [company.address, company.city, company.municipality]
      .filter(Boolean)
      .join(', '),
    type: 'company'
  }));
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

  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*'
  };

  try {
    // Only allow GET requests
    if (req.method !== 'GET') {
      return new Response(JSON.stringify({
        error: 'Method not allowed'
      }), {
        status: 405,
        headers
      });
    }

    // Get search parameters
    const url = new URL(req.url);
    const query = url.searchParams.get('q');
    const searchType = url.searchParams.get('type') || 'auto';
    const limitParam = url.searchParams.get('limit');
    const limit = limitParam ? Math.min(parseInt(limitParam), 50) : 20; // Max 50 results

    // Validate search query
    const validation = validateSearchQuery(query || '');
    if (!validation.isValid) {
      return new Response(JSON.stringify({
        error: validation.error,
        results: [],
        total: 0,
        query: query || ''
      }), {
        status: 400,
        headers
      });
    }

    // Initialize Supabase client
    const supabaseUrl = Deno.env.get('SUPABASE_URL');
    const supabaseServiceKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');

    if (!supabaseUrl || !supabaseServiceKey) {
      throw new Error('Missing Supabase configuration');
    }

    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Detect search type if auto
    const finalSearchType = searchType === 'auto' ? detectSearchType(query!) : searchType;

    // Build and execute search query
    const searchQuery = buildSearchQuery(supabase, query!, finalSearchType, limit);
    const { data, error, count } = await searchQuery;

    if (error) {
      console.error('Database search error:', error);
      return new Response(JSON.stringify({
        error: 'Search failed',
        results: [],
        total: 0,
        query: query || ''
      }), {
        status: 500,
        headers
      });
    }

    // Format and return results
    const results = formatSearchResults(data || [], query!);
    const response: SearchResponse = {
      results,
      total: results.length,
      query: query!,
      searchType: finalSearchType
    };

    return new Response(JSON.stringify(response), {
      status: 200,
      headers
    });

  } catch (error) {
    console.error('Search endpoint error:', error);
    return new Response(JSON.stringify({
      error: 'Internal server error',
      results: [],
      total: 0,
      query: ''
    }), {
      status: 500,
      headers
    });
  }
});

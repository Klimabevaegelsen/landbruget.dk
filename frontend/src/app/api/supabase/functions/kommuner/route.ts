import { NextRequest, NextResponse } from 'next/server';
import {
  getWeeklyCacheHeaders,
  getMillisecondsUntilNextTuesday,
} from '@/lib/cache-utils';

// Revalidate this route every Tuesday (server-side caching)
export const revalidate = Math.floor(getMillisecondsUntilNextTuesday() / 1000);

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);

    // Extract query parameters
    const category = searchParams.get('category') || 'all';
    const year = searchParams.get('year') || '2024';
    const limit = searchParams.get('limit') || '50';
    const sort_by = searchParams.get('sort_by') || 'total_area_ha';
    const sort_direction = searchParams.get('sort_direction') || 'desc';

    // Build the Supabase Edge Function URL
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

    if (!supabaseUrl || !supabaseAnonKey) {
      throw new Error('Missing Supabase configuration');
    }

    const edgeFunctionUrl = new URL(
      `${supabaseUrl}/functions/v1/municipality-rankings`
    );
    edgeFunctionUrl.searchParams.set('category', category);
    edgeFunctionUrl.searchParams.set('year', year);
    edgeFunctionUrl.searchParams.set('limit', limit);
    edgeFunctionUrl.searchParams.set('sort_by', sort_by);
    edgeFunctionUrl.searchParams.set('sort_direction', sort_direction);

    // Call the Supabase Edge Function
    const response = await fetch(edgeFunctionUrl.toString(), {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${supabaseAnonKey}`,
        'Content-Type': 'application/json',
        apikey: supabaseAnonKey,
      },
      // Add timeout to prevent hanging requests
      signal: AbortSignal.timeout(30000), // 30 second timeout
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error(
        'Supabase Edge Function error:',
        response.status,
        errorText
      );

      return NextResponse.json(
        {
          error: 'Failed to fetch municipality rankings',
          details: errorText,
          status: response.status,
        },
        { status: response.status }
      );
    }

    const data = await response.json();

    // Return the data with proper CORS headers
    return NextResponse.json(data, {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        ...getWeeklyCacheHeaders(),
      },
    });
  } catch (error) {
    console.error('Municipality rankings API error:', error);

    return NextResponse.json(
      {
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error',
        timestamp: new Date().toISOString(),
      },
      { status: 500 }
    );
  }
}

export async function OPTIONS() {
  return NextResponse.json(
    {},
    {
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      },
    }
  );
}

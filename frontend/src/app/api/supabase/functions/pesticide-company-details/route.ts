import { NextRequest, NextResponse } from 'next/server';

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_ANON_KEY = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

export async function GET(request: NextRequest) {
  try {
    if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
      throw new Error('Missing Supabase configuration');
    }

    // Extract search params from the request
    const { searchParams } = new URL(request.url);

    // Build the Supabase function URL
    const functionUrl = new URL(
      '/functions/v1/pesticide-company-details',
      SUPABASE_URL
    );

    // Forward all search parameters
    for (const [key, value] of searchParams.entries()) {
      functionUrl.searchParams.set(key, value);
    }

    // Make the request to Supabase function
    const response = await fetch(functionUrl.toString(), {
      method: 'GET',
      headers: {
        Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
        apikey: SUPABASE_ANON_KEY,
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(
        `Supabase function error: ${response.status} ${response.statusText}`
      );
    }

    const data = await response.json();

    return NextResponse.json(data, {
      headers: {
        'Cache-Control': 'public, max-age=1800, stale-while-revalidate=3600', // 30 min fresh, 1 hour stale
      },
    });
  } catch (error) {
    console.error('Error in pesticide-company-details proxy:', error);

    return NextResponse.json(
      {
        error: 'Failed to fetch pesticide company details',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

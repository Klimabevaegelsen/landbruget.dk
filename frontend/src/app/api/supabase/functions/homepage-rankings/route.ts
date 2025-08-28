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
    const category = searchParams.get('category') || 'all';
    const limit = searchParams.get('limit') || '20';

    // Build the Supabase function URL
    const functionUrl = new URL(
      '/functions/v1/homepage-rankings',
      SUPABASE_URL
    );
    functionUrl.searchParams.set('category', category);
    functionUrl.searchParams.set('limit', limit);

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
        'Cache-Control': 'public, s-maxage=300, stale-while-revalidate=600', // Cache for 5 minutes
      },
    });
  } catch (error) {
    console.error('Error in homepage-rankings proxy:', error);

    return NextResponse.json(
      {
        error: 'Failed to fetch rankings',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

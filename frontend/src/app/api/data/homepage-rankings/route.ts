import { NextRequest, NextResponse } from 'next/server';
import { getCachedHomepageRankings } from '@/lib/server-cache';

// This handler reads request-specific URL params, so it must stay dynamic.
export const dynamic = 'force-dynamic';

// Revalidate this route every 7 days (server-side caching)
// Data updates weekly on Tuesdays - use POST /api/revalidate-cache after data updates
export const revalidate = 604800; // 7 days in seconds

// Static cache headers for 7-day caching strategy
const CACHE_HEADERS = {
  'Cache-Control': 'public, max-age=604800, stale-while-revalidate=604800',
  'CDN-Cache-Control': 'public, max-age=604800',
  'Vercel-CDN-Cache-Control': 'public, max-age=604800',
};

export async function GET(request: NextRequest) {
  try {
    // Extract search params from the request
    const { searchParams } = new URL(request.url);
    const category = searchParams.get('category') || 'all';
    const limit = searchParams.get('limit') || '20';
    const rankingId = searchParams.get('rankingId') || '';

    // Use server-side cached data (revalidates automatically every 7 days)
    const data = await getCachedHomepageRankings(category, limit, rankingId);

    return NextResponse.json(data, {
      headers: CACHE_HEADERS,
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

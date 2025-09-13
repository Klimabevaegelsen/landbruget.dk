import { NextRequest, NextResponse } from 'next/server';
import { getWeeklyCacheHeaders } from '@/lib/cache-utils';
import { getCachedHomepageRankings } from '@/lib/server-cache';

// Revalidate this route every 7 days (server-side caching)
// Data updates weekly on Tuesdays - use POST /api/revalidate-cache after data updates
export const revalidate = 604800; // 7 days in seconds

export async function GET(request: NextRequest) {
  try {
    // Extract search params from the request
    const { searchParams } = new URL(request.url);
    const category = searchParams.get('category') || 'all';
    const limit = searchParams.get('limit') || '20';

    // Use server-side cached data (revalidates automatically on Tuesday)
    const data = await getCachedHomepageRankings(category, limit);

    return NextResponse.json(data, {
      headers: getWeeklyCacheHeaders(),
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

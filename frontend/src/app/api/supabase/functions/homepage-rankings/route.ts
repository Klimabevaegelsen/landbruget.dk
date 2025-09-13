import { NextRequest, NextResponse } from 'next/server';
import { getWeeklyCacheHeaders } from '@/lib/cache-utils';
import { getCachedHomepageRankings } from '@/lib/server-cache';

// Revalidate this route every 4 hours (server-side caching)
// This ensures fresh data while being conservative enough to catch Tuesday updates
export const revalidate = 14400; // 4 hours in seconds

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

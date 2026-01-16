import { NextResponse } from 'next/server';
import { getCachedHomepageStatistics } from '@/lib/server-cache';

// Revalidate this route every 7 days (server-side caching)
// Data updates weekly on Tuesdays - use POST /api/revalidate-cache after data updates
export const revalidate = 604800; // 7 days in seconds

// Static cache headers for 7-day caching strategy
const CACHE_HEADERS = {
  'Cache-Control': 'public, max-age=604800, stale-while-revalidate=604800',
  'CDN-Cache-Control': 'public, max-age=604800',
  'Vercel-CDN-Cache-Control': 'public, max-age=604800',
};

export async function GET() {
  try {
    // Use server-side cached data (revalidates automatically every 7 days)
    const data = await getCachedHomepageStatistics();

    return NextResponse.json(data, {
      headers: CACHE_HEADERS,
    });
  } catch (error) {
    console.error('API route error:', error);

    // Return fallback data based on our database analysis
    return NextResponse.json(
      {
        total_data_points: 29104178,
        total_companies: 46126,
        last_updated: new Date().toISOString(),
        formatted: {
          data_points: '29.104.178',
          companies: '46.126',
        },
        fallback: true,
      },
      {
        headers: CACHE_HEADERS,
      }
    );
  }
}

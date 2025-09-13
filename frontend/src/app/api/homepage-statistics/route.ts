import { NextResponse } from 'next/server';
import {
  getWeeklyCacheHeaders,
  getMillisecondsUntilNextTuesday,
} from '@/lib/cache-utils';
import { getCachedHomepageStatistics } from '@/lib/server-cache';

// Revalidate this route every Tuesday (server-side caching)
export const revalidate = Math.floor(getMillisecondsUntilNextTuesday() / 1000);

export async function GET() {
  try {
    // Use server-side cached data (revalidates automatically on Tuesday)
    const data = await getCachedHomepageStatistics();

    return NextResponse.json(data, {
      headers: getWeeklyCacheHeaders(),
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
        headers: getWeeklyCacheHeaders(),
      }
    );
  }
}

import { NextRequest, NextResponse } from 'next/server';
import { getCachedPesticideCompanyDetails } from '@/lib/server-cache';

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

    // Convert search params to record for caching
    const params: Record<string, string> = {};
    for (const [key, value] of searchParams.entries()) {
      params[key] = value;
    }

    // Use server-side cached data (revalidates automatically every 7 days)
    const data = await getCachedPesticideCompanyDetails(params);

    return NextResponse.json(data, {
      headers: CACHE_HEADERS,
    });
  } catch (error) {
    console.error('Error in pesticide-company-details proxy:', error);

    const message = error instanceof Error ? error.message : 'Unknown error';
    const isMissingCompany = message.includes('(404)');

    return NextResponse.json(
      {
        error: isMissingCompany
          ? 'No pesticide company details found'
          : 'Failed to fetch pesticide company details',
        message: isMissingCompany
          ? 'No pesticide details are available for this company in the current exported dataset.'
          : message,
      },
      { status: isMissingCompany ? 404 : 500 }
    );
  }
}

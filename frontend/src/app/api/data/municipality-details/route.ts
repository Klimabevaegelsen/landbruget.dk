import { NextRequest, NextResponse } from 'next/server';
import { getCachedMunicipalityDetails } from '@/lib/server-cache';

export const dynamic = 'force-dynamic';
export const revalidate = 604800;

const CACHE_HEADERS = {
  'Cache-Control': 'public, max-age=604800, stale-while-revalidate=604800',
  'CDN-Cache-Control': 'public, max-age=604800',
  'Vercel-CDN-Cache-Control': 'public, max-age=604800',
};

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const municipality = searchParams.get('municipality');
    const category = searchParams.get('category') || 'land_use';

    if (!municipality) {
      return NextResponse.json(
        { error: 'municipality parameter required' },
        { status: 400 }
      );
    }

    const data = await getCachedMunicipalityDetails(municipality, category);

    return NextResponse.json(data, {
      headers: CACHE_HEADERS,
    });
  } catch (error) {
    console.error('Municipality details API error:', error);
    return NextResponse.json(
      {
        error: 'Internal server error',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

import { NextRequest, NextResponse } from 'next/server';
import {
  getCachedDriftExposureIndex,
  getCachedDriftExposureTile,
} from '@/lib/server-cache';

export const dynamic = 'force-dynamic';
export const revalidate = 604800;

const CACHE_HEADERS = {
  'Cache-Control': 'public, max-age=604800, stale-while-revalidate=604800',
  'CDN-Cache-Control': 'public, max-age=604800',
  'Vercel-CDN-Cache-Control': 'public, max-age=604800',
};

const EMPTY_DRIFT_EXPOSURE_INDEX = {
  pesticide_year: null,
  national_avg_drift_dose_kg: null,
  building_count: 0,
  tile_zoom: 12,
  tile_count: 0,
};

export async function GET(request: NextRequest) {
  const params = new URL(request.url).searchParams;
  const z = params.get('z');
  const x = params.get('x');
  const y = params.get('y');

  if (!z && !x && !y) {
    try {
      const data = await getCachedDriftExposureIndex();
      return NextResponse.json(data, { headers: CACHE_HEADERS });
    } catch {
      return NextResponse.json(EMPTY_DRIFT_EXPOSURE_INDEX, {
        headers: CACHE_HEADERS,
      });
    }
  }

  if (!z || !x || !y) {
    return NextResponse.json(
      { error: 'missing tile coordinates' },
      { status: 400 }
    );
  }

  if (!/^\d+$/.test(z) || !/^\d+$/.test(x) || !/^\d+$/.test(y)) {
    return NextResponse.json(
      { error: 'invalid tile coordinates' },
      { status: 400 }
    );
  }

  try {
    const data = await getCachedDriftExposureTile(
      Number(z),
      Number(x),
      Number(y)
    );
    return NextResponse.json(data, { headers: CACHE_HEADERS });
  } catch {
    return NextResponse.json([], { headers: CACHE_HEADERS });
  }
}

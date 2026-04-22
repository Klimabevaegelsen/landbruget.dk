import { NextRequest, NextResponse } from 'next/server';
import {
  getCachedDriftExposureIndex,
  getCachedDriftExposureKommune,
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
  unmatched_count: 0,
  kommunekoder: [],
};

export async function GET(request: NextRequest) {
  const kommunekode = new URL(request.url).searchParams.get('kommunekode');

  if (!kommunekode) {
    try {
      const data = await getCachedDriftExposureIndex();
      return NextResponse.json(data, { headers: CACHE_HEADERS });
    } catch {
      return NextResponse.json(EMPTY_DRIFT_EXPOSURE_INDEX, {
        headers: CACHE_HEADERS,
      });
    }
  }

  if (!/^\d{3,4}$/.test(kommunekode)) {
    return NextResponse.json({ error: 'invalid kommunekode' }, { status: 400 });
  }

  try {
    const data = await getCachedDriftExposureKommune(kommunekode);
    return NextResponse.json(data, { headers: CACHE_HEADERS });
  } catch {
    return NextResponse.json([], { headers: CACHE_HEADERS });
  }
}

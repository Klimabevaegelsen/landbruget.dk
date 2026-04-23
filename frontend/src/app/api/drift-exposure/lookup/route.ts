import { NextRequest, NextResponse } from 'next/server';
import {
  DEFAULT_DRIFT_TILE_ZOOM,
  haversineMeters,
  lngLatToTile,
  MATCH_TOLERANCE_M,
  surroundingTiles,
} from '@/lib/drift-exposure-tiles';
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

function parseCoordinate(value: string | null): number | null {
  if (value === null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export async function GET(request: NextRequest) {
  const params = new URL(request.url).searchParams;
  const lat = parseCoordinate(params.get('lat'));
  const lng = parseCoordinate(params.get('lng'));

  if (lat === null || lng === null) {
    return NextResponse.json(
      { error: 'invalid coordinates' },
      { status: 400, headers: CACHE_HEADERS }
    );
  }

  let nationalAvgDriftDoseKg: number | null = null;
  let tileZoom = DEFAULT_DRIFT_TILE_ZOOM;

  try {
    const index = await getCachedDriftExposureIndex();
    nationalAvgDriftDoseKg = index.national_avg_drift_dose_kg;
    tileZoom = index.tile_zoom ?? DEFAULT_DRIFT_TILE_ZOOM;
  } catch {}

  const centerTile = lngLatToTile(lat, lng, tileZoom);
  const tileResponses = await Promise.all(
    surroundingTiles(centerTile.x, centerTile.y, centerTile.z).map(
      async (tile) => {
        try {
          return await getCachedDriftExposureTile(tile.z, tile.x, tile.y);
        } catch {
          return [];
        }
      }
    )
  );

  const seen = new Set<string>();
  const buildings: Awaited<ReturnType<typeof getCachedDriftExposureTile>> = [];
  for (const tileBuildings of tileResponses) {
    for (const building of tileBuildings) {
      if (seen.has(building.uid)) continue;
      seen.add(building.uid);
      buildings.push(building);
    }
  }

  let nearest: (typeof buildings)[number] | null = null;
  let nearestDist = Infinity;

  for (const building of buildings) {
    const distance = haversineMeters(lat, lng, building.lat, building.lng);
    if (distance < nearestDist) {
      nearest = building;
      nearestDist = distance;
    }
  }

  if (!nearest || nearestDist > MATCH_TOLERANCE_M) {
    return NextResponse.json(null, { headers: CACHE_HEADERS });
  }

  return NextResponse.json(
    {
      exposure_percentile: nearest.pct,
      drift_dose_kg: nearest.dose,
      national_avg_drift_dose_kg: nationalAvgDriftDoseKg,
      building_distance_m: Math.round(nearestDist),
    },
    { headers: CACHE_HEADERS }
  );
}

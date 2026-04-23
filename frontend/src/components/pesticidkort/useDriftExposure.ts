'use client';

import { useEffect, useState } from 'react';
import {
  DEFAULT_DRIFT_TILE_ZOOM,
  haversineMeters,
  lngLatToTile,
  MATCH_TOLERANCE_M,
  surroundingTiles,
} from '@/lib/drift-exposure-tiles';

export interface DriftExposureMatch {
  exposure_percentile: number;
  drift_dose_kg: number;
  national_avg_drift_dose_kg: number | null;
  building_distance_m: number;
}

interface DriftExposureIndexResponse {
  pesticide_year: number | null;
  national_avg_drift_dose_kg: number | null;
  tile_zoom: number;
}

interface DriftExposureBuildingResponse {
  uid: string;
  lat: number;
  lng: number;
  pct: number;
  dose: number;
}

/**
 * Look up drift-exposure percentile for a coordinate by loading the address's
 * slippy-map tile plus adjacent tiles, then finding the nearest BBR building.
 * Returns null when no nearby building shard exists or no building is within
 * {@link MATCH_TOLERANCE_M}.
 */
export function useDriftExposure(
  lat: number | undefined,
  lng: number | undefined
): {
  match: DriftExposureMatch | null;
  status: 'idle' | 'loading' | 'ready' | 'no_match';
} {
  const [status, setStatus] = useState<
    'idle' | 'loading' | 'ready' | 'no_match'
  >('idle');
  const [match, setMatch] = useState<DriftExposureMatch | null>(null);

  useEffect(() => {
    if (lat === undefined || lng === undefined) {
      setStatus('idle');
      setMatch(null);
      return;
    }
    let cancelled = false;
    setStatus('loading');
    setMatch(null);

    (async () => {
      const indexRes = await fetch('/api/drift-exposure');
      if (cancelled) return;
      const index = indexRes.ok
        ? ((await indexRes.json()) as DriftExposureIndexResponse)
        : null;
      if (cancelled) return;
      const tileZoom = index?.tile_zoom ?? DEFAULT_DRIFT_TILE_ZOOM;
      const centerTile = lngLatToTile(lat, lng, tileZoom);
      const tileResponses = await Promise.all(
        surroundingTiles(centerTile.x, centerTile.y, centerTile.z).map(
          async (tile) => {
            const response = await fetch(
              `/api/drift-exposure?z=${tile.z}&x=${tile.x}&y=${tile.y}`
            );
            if (!response.ok) return [];
            return (await response.json()) as DriftExposureBuildingResponse[];
          }
        )
      );
      if (cancelled) return;
      const seen = new Set<string>();
      const buildings: DriftExposureBuildingResponse[] = [];
      for (const tileBuildings of tileResponses) {
        for (const building of tileBuildings) {
          if (seen.has(building.uid)) continue;
          seen.add(building.uid);
          buildings.push(building);
        }
      }

      let nearest: DriftExposureBuildingResponse | null = null;
      let nearestDist = Infinity;
      for (const b of buildings) {
        const d = haversineMeters(lat, lng, b.lat, b.lng);
        if (d < nearestDist) {
          nearest = b;
          nearestDist = d;
        }
      }

      if (!nearest || nearestDist > MATCH_TOLERANCE_M) {
        setStatus('no_match');
        return;
      }

      setMatch({
        exposure_percentile: nearest.pct,
        drift_dose_kg: nearest.dose,
        national_avg_drift_dose_kg: index?.national_avg_drift_dose_kg ?? null,
        building_distance_m: Math.round(nearestDist),
      });
      setStatus('ready');
    })().catch(() => {
      if (!cancelled) setStatus('no_match');
    });

    return () => {
      cancelled = true;
    };
  }, [lat, lng]);

  return { match, status };
}

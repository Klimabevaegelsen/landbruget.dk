'use client';

import { useEffect, useState } from 'react';

export interface DriftExposureMatch {
  exposure_percentile: number;
  drift_dose_kg: number;
  national_avg_drift_dose_kg: number | null;
  building_distance_m: number;
}

interface DriftExposureIndexResponse {
  pesticide_year: number | null;
  national_avg_drift_dose_kg: number | null;
}

interface DriftExposureBuildingResponse {
  uid: string;
  lat: number;
  lng: number;
  pct: number;
  dose: number;
}

// Match tolerance: DAWA coordinates are building-centroid snapshots; drift
// exposure uses BBR centroids. 60 m covers most cases without matching a
// neighbor in dense residential areas.
const MATCH_TOLERANCE_M = 60;

function haversineMeters(
  aLat: number,
  aLng: number,
  bLat: number,
  bLng: number
): number {
  const R = 6371000;
  const toRad = (d: number) => (d * Math.PI) / 180;
  const dLat = toRad(bLat - aLat);
  const dLng = toRad(bLng - aLng);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

async function reverseLookupKommune(
  lat: number,
  lng: number
): Promise<string | null> {
  try {
    const res = await fetch(
      `https://api.dataforsyningen.dk/kommuner/reverse?x=${lng}&y=${lat}&format=json`
    );
    if (!res.ok) return null;
    const data = (await res.json()) as { kode?: string };
    return data.kode ?? null;
  } catch {
    return null;
  }
}

/**
 * Look up drift-exposure percentile for a coordinate by finding the nearest
 * BBR building in the address's kommune. Returns null when the coordinate
 * falls outside any kommune, the kommune has no drift data, or no building
 * is within {@link MATCH_TOLERANCE_M}.
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
      const kommunekode = await reverseLookupKommune(lat, lng);
      if (cancelled) return;
      if (!kommunekode) {
        setStatus('no_match');
        return;
      }

      const [buildingsRes, indexRes] = await Promise.all([
        fetch(`/api/drift-exposure?kommunekode=${kommunekode}`),
        fetch('/api/drift-exposure'),
      ]);
      if (cancelled) return;
      if (!buildingsRes.ok) {
        setStatus('no_match');
        return;
      }
      const buildings =
        (await buildingsRes.json()) as DriftExposureBuildingResponse[];
      const index = indexRes.ok
        ? ((await indexRes.json()) as DriftExposureIndexResponse)
        : null;
      if (cancelled) return;

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

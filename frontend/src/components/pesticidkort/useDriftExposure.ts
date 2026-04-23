'use client';

import { useEffect, useState } from 'react';

export interface DriftExposureMatch {
  exposure_percentile: number;
  drift_dose_kg: number;
  national_avg_drift_dose_kg: number | null;
  building_distance_m: number;
}

/**
 * Look up the final drift-exposure match for a coordinate via a single server
 * endpoint that resolves nearby tiles and nearest-building matching.
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
      const params = new URLSearchParams({
        lat: lat.toString(),
        lng: lng.toString(),
      });
      const response = await fetch(`/api/drift-exposure/lookup?${params}`);
      if (cancelled) return;
      if (!response.ok) {
        setStatus('no_match');
        return;
      }
      const driftMatch = (await response.json()) as DriftExposureMatch | null;
      if (cancelled) return;
      if (!driftMatch) {
        setStatus('no_match');
        return;
      }

      setMatch(driftMatch);
      setStatus('ready');
    })().catch(() => {
      if (!cancelled) {
        setStatus('no_match');
      }
    });

    return () => {
      cancelled = true;
    };
  }, [lat, lng]);

  return { match, status };
}

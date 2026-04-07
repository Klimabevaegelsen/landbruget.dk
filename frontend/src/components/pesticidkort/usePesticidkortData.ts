import { useRef, useEffect, useState, useCallback } from 'react';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import {
  featureToFieldSummary,
  haversineDistance,
} from '@/components/pesticidkort/map-utils';
import { computeCentroid } from '@/utils/geo';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import type { MapRef } from '@vis.gl/react-maplibre';

interface UsePesticidkortDataOptions {
  lat: number;
  lng: number;
  radiusM: number;
  year: number;
  mapRef: React.RefObject<MapRef | null>;
  onFieldsLoaded: (fields: NearbyFieldSummary[]) => void;
}

export function usePesticidkortData({
  lat,
  lng,
  radiusM,
  year,
  mapRef,
  onFieldsLoaded,
}: UsePesticidkortDataOptions) {
  const [pmtilesUrl, setPmtilesUrl] = useState<string | null>(null);
  const [overviewUrl, setOverviewUrl] = useState<string | null>(null);
  const [buildingsUrl, setBuildingsUrl] = useState<string | null>(null);
  const fieldsQueriedRef = useRef(false);

  useEffect(() => {
    pmtilesCacheService.getFieldAnalysisUrls(year).then((urls) => {
      setPmtilesUrl(urls.fields);
      setOverviewUrl(urls.overview);
      setBuildingsUrl(urls.buildings);
    });
  }, [year]);

  useEffect(() => {
    fieldsQueriedRef.current = false;
  }, [lat, lng, radiusM, pmtilesUrl]);

  const queryNearbyFields = useCallback(() => {
    if (!mapRef.current || fieldsQueriedRef.current) return;
    const map = mapRef.current.getMap();
    if (!map.getLayer('fields-fill')) return;
    fieldsQueriedRef.current = true;

    const features = map.queryRenderedFeatures(undefined, {
      layers: ['fields-fill'],
    });
    const seen = new Set<string>();
    const fields: NearbyFieldSummary[] = [];

    for (const feat of features) {
      const uuid = String(feat.properties.field_uuid ?? '');
      if (!uuid || seen.has(uuid)) continue;
      seen.add(uuid);
      const centroid = computeCentroid(feat.geometry);
      if (!centroid) continue;
      const dist = haversineDistance(lat, lng, centroid.lat, centroid.lng);
      if (dist > radiusM) continue;
      fields.push(
        featureToFieldSummary(
          feat.properties as Record<string, unknown>,
          lat,
          lng,
          centroid.lat,
          centroid.lng
        )
      );
    }
    fields.sort((a, b) => a.distance_m - b.distance_m);
    onFieldsLoaded(fields);
  }, [lat, lng, radiusM, mapRef, onFieldsLoaded]);

  return {
    pmtilesUrl,
    overviewUrl,
    buildingsUrl,
    queryNearbyFields,
    fieldsQueriedRef,
  };
}

'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { NavigationControl, Marker, MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import { ProximityRings } from '@/components/pesticidkort/ProximityRings';
import { addFieldLayers } from '@/components/pesticidkort/map-layers';
import {
  featureToFieldSummary,
  haversineDistance,
} from '@/components/pesticidkort/map-utils';
import {
  highlightField,
  flyToField,
} from '@/components/pesticidkort/map-highlight';
import { registerPmtilesProtocol } from '@/components/pesticidkort/pmtiles-protocol';
import { computeCentroid } from '@/utils/geo';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import type { MapInstance } from '@/components/field-analysis/map-constants';

interface PesticidkortMapProps {
  lat: number;
  lng: number;
  radiusM: number;
  year: number;
  selectedFieldUuid?: string | null;
  onFieldsLoaded: (fields: NearbyFieldSummary[]) => void;
}

export function PesticidkortMap({
  lat,
  lng,
  radiusM,
  year,
  selectedFieldUuid,
  onFieldsLoaded,
}: PesticidkortMapProps) {
  const { mapStyle } = useMapTheme();
  const mapRef = useRef<MapRef>(null);
  const [isReady, setIsReady] = useState(false);
  const [pmtilesUrl, setPmtilesUrl] = useState<string | null>(null);
  const fieldsQueriedRef = useRef(false);

  useEffect(() => {
    pmtilesCacheService
      .getFieldAnalysisUrls(year)
      .then((urls) => setPmtilesUrl(urls.fields));
  }, [year]);

  useEffect(() => {
    registerPmtilesProtocol();
  }, []);

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
  }, [lat, lng, radiusM, onFieldsLoaded]);

  const handleMapLoad = useCallback(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    const map = mapRef.current.getMap();
    addFieldLayers(map as unknown as MapInstance, pmtilesUrl);
    setIsReady(true);
    map.on('idle', () => {
      if (!fieldsQueriedRef.current) queryNearbyFields();
    });
  }, [pmtilesUrl, queryNearbyFields]);

  // When year changes, update the source URL on the existing map
  useEffect(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    const map = mapRef.current.getMap();
    const src = map.getSource('fields');
    if (src && 'setUrl' in src) {
      (src as unknown as { setUrl: (url: string) => void }).setUrl(
        `pmtiles://${pmtilesUrl}`
      );
    }
    fieldsQueriedRef.current = false;
  }, [pmtilesUrl]);

  useEffect(() => {
    fieldsQueriedRef.current = false;
  }, [lat, lng, radiusM]);

  useEffect(() => {
    if (!isReady) return;
    highlightField(mapRef.current, selectedFieldUuid ?? null);
    if (selectedFieldUuid) flyToField(mapRef.current, selectedFieldUuid);
  }, [selectedFieldUuid, isReady]);

  if (!pmtilesUrl) {
    return (
      <div className="bg-muted flex h-full items-center justify-center">
        <p className="text-muted-foreground text-sm">Indlæser kort...</p>
      </div>
    );
  }

  return (
    <Map
      ref={mapRef}
      initialViewState={{ longitude: lng, latitude: lat, zoom: 13.5 }}
      mapStyle={mapStyle}
      onLoad={handleMapLoad}
      attributionControl={false}
      data-testid="pesticidkort-map"
    >
      <NavigationControl position="top-right" />
      {isReady && <ProximityRings lat={lat} lng={lng} radiusM={radiusM} />}
      <Marker longitude={lng} latitude={lat} anchor="center">
        <div className="bg-primary border-background h-4 w-4 rounded-full border-2 shadow-md" />
      </Marker>
    </Map>
  );
}

'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { NavigationControl, MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import { addFieldLayers } from '@/components/pesticidkort/map-layers';
import { registerPmtilesProtocol } from '@/components/pesticidkort/pmtiles-protocol';
import type { MapInstance } from '@/components/field-analysis/map-constants';

const DENMARK_CENTER = { longitude: 10.4, latitude: 56.0 };
const DENMARK_ZOOM = 7;

interface ExploreMapInnerProps {
  year: number;
}

export function ExploreMapInner({ year }: ExploreMapInnerProps) {
  const { mapStyle } = useMapTheme();
  const mapRef = useRef<MapRef>(null);
  const [pmtilesUrl, setPmtilesUrl] = useState<string | null>(null);

  useEffect(() => {
    pmtilesCacheService
      .getFieldAnalysisUrls(year)
      .then((urls) => setPmtilesUrl(urls.fields));
  }, [year]);

  useEffect(() => {
    registerPmtilesProtocol();
  }, []);

  const handleMapLoad = useCallback(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    const map = mapRef.current.getMap();
    addFieldLayers(map as unknown as MapInstance, pmtilesUrl);
  }, [pmtilesUrl]);

  useEffect(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    const map = mapRef.current.getMap();
    const src = map.getSource('fields');
    if (src && 'setUrl' in src) {
      (src as unknown as { setUrl: (url: string) => void }).setUrl(
        `pmtiles://${pmtilesUrl}`
      );
    }
  }, [pmtilesUrl]);

  if (!pmtilesUrl) return null;

  return (
    <Map
      ref={mapRef}
      initialViewState={{
        longitude: DENMARK_CENTER.longitude,
        latitude: DENMARK_CENTER.latitude,
        zoom: DENMARK_ZOOM,
      }}
      mapStyle={mapStyle}
      onLoad={handleMapLoad}
      attributionControl={false}
      data-testid="explore-map"
    >
      <NavigationControl position="top-right" />
    </Map>
  );
}

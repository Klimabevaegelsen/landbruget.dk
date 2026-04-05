'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { type MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import { addFieldLayers } from '@/components/pesticidkort/map-layers';
import { registerPmtilesProtocol } from '@/components/pesticidkort/pmtiles-protocol';
import { EXAMPLE } from '@/components/methodology/scrolly-example-data';

const C = EXAMPLE.center;
const UUIDS = EXAMPLE.fields.map((f) => f.uuid);

const VIEWS = {
  overview: { zoom: 6.5, lng: 10.5, lat: 56.0 },
  zoom: { zoom: 12.5, lng: C[0], lat: C[1] },
  fields: { zoom: 14, lng: C[0], lat: C[1] },
  match: { zoom: 14.5, lng: C[0], lat: C[1] },
  result: { zoom: 13.5, lng: C[0], lat: C[1] },
} as const;

export type MapStep = keyof typeof VIEWS;

interface MethodologyMapProps {
  step: MapStep;
}

export function MethodologyMap({ step }: MethodologyMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [ready, setReady] = useState(false);
  const { mapStyle } = useMapTheme();

  const handleLoad = useCallback(async () => {
    const map = mapRef.current?.getMap();
    if (!map) return;
    await registerPmtilesProtocol();
    const urls = await pmtilesCacheService.getFieldAnalysisUrls(2023);
    addFieldLayers(map as never, urls.fields);

    // Smooth transitions for field layers
    map.setPaintProperty('fields-fill', 'fill-opacity-transition', {
      duration: 600,
      delay: 0,
    });
    map.setPaintProperty('fields-outline', 'line-opacity-transition', {
      duration: 600,
      delay: 0,
    });

    // Highlight layer for the example fields
    map.addLayer({
      id: 'method-highlight',
      source: 'fields',
      'source-layer': 'field_analysis',
      type: 'line',
      paint: {
        'line-color': '#f59e0b',
        'line-width': 3,
        'line-opacity': 0,
        'line-opacity-transition': { duration: 500, delay: 0 },
        'line-width-transition': { duration: 400, delay: 0 },
      },
      filter: ['in', 'field_uuid', ...UUIDS],
    });

    setReady(true);
  }, []);

  useEffect(() => {
    if (!ready) return;
    const map = mapRef.current?.getMap();
    if (!map) return;

    const v = VIEWS[step];
    map.flyTo({
      center: [v.lng, v.lat],
      zoom: v.zoom,
      duration: 1600,
      essential: true,
      curve: 1.2,
    });

    const showHighlight =
      step === 'fields' || step === 'match' || step === 'result';

    try {
      const burdenColor =
        step === 'result'
          ? [
              'interpolate',
              ['linear'],
              ['coalesce', ['get', 'total_pesticide_belastning'], 0],
              0,
              '#6abf69',
              2,
              '#d4c54a',
              5,
              '#d89135',
              10,
              '#c4512c',
            ]
          : '#6abf69';

      map.setPaintProperty('fields-fill', 'fill-color', burdenColor);
      map.setPaintProperty(
        'fields-fill',
        'fill-opacity',
        step === 'overview' ? 0.15 : 0.7
      );
      map.setPaintProperty(
        'fields-outline',
        'line-opacity',
        step === 'overview' ? 0.1 : 0.5
      );
      map.setPaintProperty(
        'method-highlight',
        'line-opacity',
        showHighlight ? 1 : 0
      );
      map.setPaintProperty(
        'method-highlight',
        'line-width',
        step === 'result' ? 2 : 3
      );
    } catch {
      /* layers not yet added */
    }
  }, [step, ready]);

  return (
    <Map
      ref={mapRef}
      initialViewState={{ longitude: 10.5, latitude: 56.0, zoom: 6.5 }}
      mapStyle={mapStyle}
      onLoad={handleLoad}
      attributionControl={false}
      dragRotate={false}
      data-testid="methodology-map"
    />
  );
}

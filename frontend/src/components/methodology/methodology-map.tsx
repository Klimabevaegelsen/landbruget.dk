'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { type MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import { addFieldLayers } from '@/components/pesticidkort/map-layers';
import { registerPmtilesProtocol } from '@/components/pesticidkort/pmtiles-protocol';
import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import {
  VIEWS,
  type DisaggStepId,
} from '@/components/methodology/scrolly-disagg-views';
import { DisaggMapAnnotations } from '@/components/methodology/disagg-map-annotations';

const UUIDS = EXAMPLE.fields.map((f) => f.uuid);

const HIGHLIGHT_STEPS = new Set<DisaggStepId>([
  'fields',
  'match',
  'result',
  'summary',
]);
const DIM_STEPS = new Set<DisaggStepId>(['context', 'location']);

interface MethodologyMapProps {
  step: DisaggStepId;
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

    map.on('sourcedata', (e) => {
      if (e.sourceId !== 'fields' || !map.isSourceLoaded('fields')) return;
      if (map.getLayer('method-highlight')) {
        setReady(true);
        return;
      }
      map.setPaintProperty('fields-fill', 'fill-opacity-transition', {
        duration: 600,
        delay: 0,
      });
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
        },
        filter: ['in', 'field_uuid', ...UUIDS],
      });
      setReady(true);
    });
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
    try {
      const isResult = step === 'result' || step === 'summary';
      const burdenColor = isResult
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
      const dim = DIM_STEPS.has(step);
      map.setPaintProperty('fields-fill', 'fill-color', burdenColor);
      map.setPaintProperty('fields-fill', 'fill-opacity', dim ? 0.15 : 0.7);
      map.setPaintProperty('fields-outline', 'line-opacity', dim ? 0.1 : 0.5);
      map.setPaintProperty(
        'method-highlight',
        'line-opacity',
        HIGHLIGHT_STEPS.has(step) ? 1 : 0
      );
    } catch (err) {
      console.warn('[MethodologyMap] Paint update failed:', err);
    }
  }, [step, ready]);

  return (
    <Map
      ref={mapRef}
      initialViewState={{
        longitude: VIEWS.context.lng,
        latitude: VIEWS.context.lat,
        zoom: VIEWS.context.zoom,
      }}
      mapStyle={mapStyle}
      onLoad={handleLoad}
      attributionControl={false}
      dragRotate={false}
      data-testid="methodology-map"
    >
      <DisaggMapAnnotations step={step} />
    </Map>
  );
}

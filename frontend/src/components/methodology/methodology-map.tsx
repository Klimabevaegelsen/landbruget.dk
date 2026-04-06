'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { Source, Layer, type MapRef } from '@vis.gl/react-maplibre';
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
import {
  addHighlightLayers,
  updateStepPaint,
} from '@/components/methodology/disagg-map-layers';
import {
  HEATMAP_POINTS,
  HADERSLEV_POLYGON,
} from '@/components/methodology/disagg-map-geodata';

const UUIDS = EXAMPLE.fields.map((f) => f.uuid);

interface MethodologyMapProps {
  step: DisaggStepId;
}

export function MethodologyMap({ step }: MethodologyMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [pmtilesUrl, setPmtilesUrl] = useState<string | null>(null);
  const [ready, setReady] = useState(false);
  const { mapStyle } = useMapTheme();

  useEffect(() => {
    registerPmtilesProtocol();
    pmtilesCacheService
      .getFieldAnalysisUrls(2023)
      .then((urls) => setPmtilesUrl(urls.fields))
      .catch(() => setPmtilesUrl(null));
  }, []);

  const handleLoad = useCallback(() => {
    const map = mapRef.current?.getMap();
    if (!map || !pmtilesUrl) return;
    addFieldLayers(map as never, pmtilesUrl);
    addHighlightLayers(map, UUIDS);
    map.once('idle', () => setReady(true));
    setTimeout(() => setReady(true), 5000);
  }, [pmtilesUrl]);

  useEffect(() => {
    if (!ready) return;
    const map = mapRef.current?.getMap();
    if (!map) return;
    const v = VIEWS[step];
    if (v.bounds) {
      map.fitBounds(v.bounds, { padding: 60, duration: 1600 });
    } else {
      map.flyTo({
        center: [v.lng, v.lat],
        zoom: v.zoom,
        pitch: v.pitch ?? 0,
        bearing: v.bearing ?? 0,
        duration: step === 'scale' ? 2000 : 1600,
        essential: true,
        curve: 1.2,
      });
    }
    try {
      updateStepPaint(map, step);
    } catch (err) {
      console.warn('[MethodologyMap] Paint update failed:', err);
    }
  }, [step, ready]);

  if (!pmtilesUrl) return null;

  const showHeatmap = step === 'context';
  const showMunicipality = step === 'location';

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
      {showHeatmap && (
        <Source id="heatmap-pts" type="geojson" data={HEATMAP_POINTS}>
          <Layer
            id="method-heatmap"
            type="heatmap"
            paint={{
              'heatmap-intensity': 0.6,
              'heatmap-radius': 30,
              'heatmap-opacity': 0.5,
            }}
          />
        </Source>
      )}
      {showMunicipality && (
        <Source id="haderslev-poly" type="geojson" data={HADERSLEV_POLYGON}>
          <Layer
            id="method-municipality-fill"
            type="fill"
            paint={{ 'fill-color': '#f59e0b', 'fill-opacity': 0.08 }}
          />
          <Layer
            id="method-municipality-line"
            type="line"
            paint={{ 'line-color': '#f59e0b', 'line-width': 2 }}
          />
        </Source>
      )}
      <DisaggMapAnnotations step={step} />
    </Map>
  );
}

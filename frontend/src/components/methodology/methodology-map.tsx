'use client';

import { useRef, useEffect, useState, useCallback, useMemo } from 'react';
import Map, { Source, Layer, type MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import {
  addFieldLayers,
  addOverviewLayers,
} from '@/components/pesticidkort/map-layers';
import { registerPmtilesProtocol } from '@/components/pesticidkort/pmtiles-protocol';
import { EXAMPLE } from '@/components/methodology/scrolly-example-data';
import {
  VIEWS,
  type DisaggStepId,
} from '@/components/methodology/scrolly-disagg-views';
import { DisaggMapAnnotations } from '@/components/methodology/disagg-map-annotations';
import { addHighlightLayers } from '@/components/methodology/disagg-map-layers';
import {
  HEATMAP_POINTS,
  HADERSLEV_POLYGON,
} from '@/components/methodology/disagg-map-geodata';
import { animateToStep } from '@/components/methodology/methodology-map-nav';

const UUIDS = EXAMPLE.fields.map((f) => f.uuid);

interface MethodologyMapProps {
  step: DisaggStepId;
}

export function MethodologyMap({ step }: MethodologyMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [pmtilesUrl, setPmtilesUrl] = useState<string | null>(null);
  const [overviewUrl, setOverviewUrl] = useState<string | null>(null);
  const [ready, setReady] = useState(false);
  const { mapStyle } = useMapTheme();

  useEffect(() => {
    registerPmtilesProtocol();
    pmtilesCacheService
      .getFieldAnalysisUrls(2023)
      .then((urls) => {
        setPmtilesUrl(urls.fields);
        setOverviewUrl(urls.overview);
      })
      .catch(() => setPmtilesUrl(null));
  }, []);

  const handleLoad = useCallback(() => {
    const map = mapRef.current?.getMap();
    if (!map || !pmtilesUrl) return;
    addFieldLayers(map as never, pmtilesUrl);
    if (overviewUrl) addOverviewLayers(map as never, overviewUrl);
    addHighlightLayers(map, UUIDS);
    map.once('idle', () => setReady(true));
    setTimeout(() => setReady(true), 5000);
  }, [pmtilesUrl, overviewUrl]);

  const isMobile = useMemo(
    () => typeof window !== 'undefined' && window.innerWidth < 1024,
    []
  );

  useEffect(() => {
    if (!ready) return;
    animateToStep(mapRef, step, isMobile);
  }, [step, ready, isMobile]);

  if (!pmtilesUrl) {
    return (
      <div className="bg-muted flex h-full w-full items-center justify-center">
        <div className="border-primary h-8 w-8 animate-spin rounded-full border-2 border-t-transparent" />
      </div>
    );
  }

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
              'heatmap-weight': [
                'interpolate',
                ['linear'],
                ['get', 'weight'],
                0,
                0,
                55000,
                1,
              ],
              'heatmap-intensity': 0.8,
              'heatmap-radius': 35,
              'heatmap-opacity': 0.6,
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

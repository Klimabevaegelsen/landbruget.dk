'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { Marker, MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import { addFieldLayers } from '@/components/pesticidkort/map-layers';
import { registerPmtilesProtocol } from '@/components/pesticidkort/pmtiles-protocol';
import { ProximityRings } from '@/components/pesticidkort/ProximityRings';
import type { StoryChapter } from '@/components/pesticidkort/story-chapters';
import type { MapInstance } from '@/components/field-analysis/map-constants';

interface StoryMapInnerProps {
  lat: number;
  lng: number;
  radiusM: number;
  year: number;
  chapter: StoryChapter;
}

export function StoryMapInner({
  lat,
  lng,
  radiusM,
  year,
  chapter,
}: StoryMapInnerProps) {
  const { mapStyle } = useMapTheme();
  const mapRef = useRef<MapRef>(null);
  const [pmtilesUrl, setPmtilesUrl] = useState<string | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    registerPmtilesProtocol();
  }, []);

  useEffect(() => {
    pmtilesCacheService
      .getFieldAnalysisUrls(year)
      .then((urls) => setPmtilesUrl(urls.fields));
  }, [year]);

  const handleLoad = useCallback(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    addFieldLayers(
      mapRef.current.getMap() as unknown as MapInstance,
      pmtilesUrl
    );
    setReady(true);
  }, [pmtilesUrl]);

  useEffect(() => {
    if (!mapRef.current || !ready) return;
    const map = mapRef.current.getMap();
    map.flyTo({ center: [lng, lat], zoom: chapter.mapZoom, duration: 1200 });
    applyPaintMode(map, chapter.paintMode);
  }, [chapter, lat, lng, ready]);

  const showRings = chapter.showLayers.includes('proximity-rings');
  if (!pmtilesUrl) return null;

  return (
    <Map
      ref={mapRef}
      initialViewState={{ longitude: lng, latitude: lat, zoom: 14 }}
      mapStyle={mapStyle}
      onLoad={handleLoad}
      attributionControl={false}
      interactive={false}
      data-testid="story-map"
    >
      {ready && showRings && (
        <ProximityRings lat={lat} lng={lng} radiusM={radiusM} />
      )}
      <Marker longitude={lng} latitude={lat} anchor="center">
        <div className="bg-primary border-background h-4 w-4 rounded-full border-2 shadow-md" />
      </Marker>
    </Map>
  );
}

function applyPaintMode(
  map: ReturnType<MapRef['getMap']>,
  mode: StoryChapter['paintMode']
) {
  if (!map.getLayer('fields-fill')) return;
  if (mode === 'pfas') {
    map.setPaintProperty('fields-fill', 'fill-color', [
      'case',
      ['>', ['coalesce', ['get', 'pfas_applications'], 0], 0],
      'oklch(65% 0.15 45)',
      'oklch(85% 0.03 250)',
    ]);
    map.setPaintProperty('fields-fill', 'fill-opacity', 0.8);
  } else if (mode === 'burden') {
    map.setPaintProperty('fields-fill', 'fill-color', [
      'interpolate',
      ['linear'],
      ['coalesce', ['get', 'total_pesticide_belastning'], 0],
      0,
      'oklch(75% 0.12 142)',
      2,
      'oklch(80% 0.14 100)',
      5,
      'oklch(70% 0.16 65)',
      10,
      'oklch(60% 0.18 25)',
    ]);
    map.setPaintProperty('fields-fill', 'fill-opacity', 0.7);
  } else {
    map.setPaintProperty('fields-fill', 'fill-color', 'oklch(55% 0.15 142)');
    map.setPaintProperty('fields-fill', 'fill-opacity', 0.5);
  }
}

'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { Marker, MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import {
  addFieldLayers,
  addOverviewLayers,
} from '@/components/pesticidkort/map-layers';
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
  const [overviewUrl, setOverviewUrl] = useState<string | null>(null);
  const [ready, setReady] = useState(false);

  useEffect(() => {
    registerPmtilesProtocol();
  }, []);

  useEffect(() => {
    pmtilesCacheService.getFieldAnalysisUrls(year).then((urls) => {
      setPmtilesUrl(urls.fields);
      setOverviewUrl(urls.overview);
    });
  }, [year]);

  const handleLoad = useCallback(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    const map = mapRef.current.getMap() as unknown as MapInstance;
    addFieldLayers(map, pmtilesUrl);
    if (overviewUrl) addOverviewLayers(map, overviewUrl);
    setReady(true);
  }, [pmtilesUrl, overviewUrl]);

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
  const layers = ['fields-fill', 'fields-overview-fill'].filter((id) =>
    map.getLayer(id)
  );
  if (layers.length === 0) return;

  let color: unknown;
  let opacity: number;
  if (mode === 'pfas') {
    color = [
      'case',
      ['>', ['coalesce', ['get', 'pfas_applications'], 0], 0],
      '#d06a3a',
      '#dfe6f2',
    ];
    opacity = 0.8;
  } else if (mode === 'burden') {
    color = [
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
    ];
    opacity = 0.7;
  } else {
    color = '#3a9d5d';
    opacity = 0.5;
  }
  for (const id of layers) {
    map.setPaintProperty(id, 'fill-color', color);
    map.setPaintProperty(id, 'fill-opacity', opacity);
  }
}

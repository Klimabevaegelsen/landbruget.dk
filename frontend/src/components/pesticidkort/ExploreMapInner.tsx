'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { NavigationControl, MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';
import {
  addFieldLayers,
  addOverviewLayers,
  applyChemicalFilter,
} from '@/components/pesticidkort/map-layers';
import type { ChemicalFilter } from '@/components/pesticidkort/map-layers';
import { registerPmtilesProtocol } from '@/components/pesticidkort/pmtiles-protocol';
import { setupFieldClickHandlers } from '@/components/pesticidkort/map-events';
import { highlightField } from '@/components/pesticidkort/map-highlight';
import { FieldHoverTooltip } from '@/components/pesticidkort/FieldHoverTooltip';
import { useFieldHover } from '@/components/pesticidkort/useFieldHover';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import type { MapInstance } from '@/components/field-analysis/map-constants';

const DENMARK_CENTER = { longitude: 10.4, latitude: 56.0 };
const DENMARK_ZOOM = 7;

interface ExploreMapInnerProps {
  year: number;
  activeFilter?: ChemicalFilter;
  onZoomChange?: (zoom: number) => void;
  onFieldClick?: (fieldUuid: string, fieldData: NearbyFieldSummary) => void;
  selectedFieldUuid?: string | null;
}

export function ExploreMapInner({
  year,
  activeFilter = 'none',
  onZoomChange,
  onFieldClick,
  selectedFieldUuid,
}: ExploreMapInnerProps) {
  const { mapStyle } = useMapTheme();
  const mapRef = useRef<MapRef>(null);
  const [pmtilesUrl, setPmtilesUrl] = useState<string | null>(null);
  const [overviewUrl, setOverviewUrl] = useState<string | null>(null);
  const [mapReady, setMapReady] = useState(false);
  const { hoverData, attachHover } = useFieldHover();
  const onFieldClickRef = useRef(onFieldClick);
  onFieldClickRef.current = onFieldClick;

  useEffect(() => {
    pmtilesCacheService.getFieldAnalysisUrls(year).then((urls) => {
      setPmtilesUrl(urls.fields);
      setOverviewUrl(urls.overview);
    });
  }, [year]);

  useEffect(() => {
    registerPmtilesProtocol();
  }, []);

  const handleMapLoad = useCallback(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    const map = mapRef.current.getMap();
    addFieldLayers(map as unknown as MapInstance, pmtilesUrl);
    if (overviewUrl)
      addOverviewLayers(map as unknown as MapInstance, overviewUrl);
    setMapReady(true);

    setupFieldClickHandlers(map, 0, 0, onFieldClickRef);
    attachHover(map);
  }, [pmtilesUrl, overviewUrl, attachHover]);

  useEffect(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    const map = mapRef.current.getMap();
    const setSourceUrl = (name: string, url: string) => {
      const src = map.getSource(name);
      if (src && 'setUrl' in src) {
        (src as unknown as { setUrl: (u: string) => void }).setUrl(
          `pmtiles://${url}`
        );
      }
    };
    setSourceUrl('fields', pmtilesUrl);
    if (overviewUrl) setSourceUrl('fields-overview', overviewUrl);
  }, [pmtilesUrl, overviewUrl]);

  useEffect(() => {
    if (!mapRef.current || !mapReady) return;
    const map = mapRef.current.getMap() as unknown as MapInstance;
    applyChemicalFilter(map, activeFilter);
  }, [activeFilter, mapReady]);

  useEffect(() => {
    if (!mapReady) return;
    highlightField(mapRef.current, selectedFieldUuid ?? null);
  }, [selectedFieldUuid, mapReady]);

  const handleZoom = useCallback(() => {
    if (!mapRef.current || !onZoomChange) return;
    onZoomChange(mapRef.current.getMap().getZoom());
  }, [onZoomChange]);

  if (!pmtilesUrl) return null;

  return (
    <>
      <div className="pesticidkort-map-shell pesticidkort-map-shell--explore h-full w-full">
        <Map
          ref={mapRef}
          initialViewState={{
            longitude: DENMARK_CENTER.longitude,
            latitude: DENMARK_CENTER.latitude,
            zoom: DENMARK_ZOOM,
          }}
          mapStyle={mapStyle}
          onLoad={handleMapLoad}
          onZoom={handleZoom}
          attributionControl={false}
          data-testid="explore-map"
        >
          <NavigationControl position="top-left" />
        </Map>
      </div>
      {hoverData && <FieldHoverTooltip {...hoverData} />}
    </>
  );
}

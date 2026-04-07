'use client';

import { useRef, useEffect, useState, useCallback } from 'react';
import Map, { NavigationControl, Marker, MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { ProximityRings } from '@/components/pesticidkort/ProximityRings';
import {
  addFieldLayers,
  addOverviewLayers,
} from '@/components/pesticidkort/map-layers';
import { addBuildingLayers } from '@/components/pesticidkort/map-layers-buildings';
import {
  highlightField,
  flyToField,
} from '@/components/pesticidkort/map-highlight';
import { registerPmtilesProtocol } from '@/components/pesticidkort/pmtiles-protocol';
import { usePesticidkortData } from '@/components/pesticidkort/usePesticidkortData';
import { setupFieldClickHandlers } from '@/components/pesticidkort/map-events';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import type { MapInstance } from '@/components/field-analysis/map-constants';

interface PesticidkortMapProps {
  lat: number;
  lng: number;
  radiusM: number;
  year: number;
  selectedFieldUuid?: string | null;
  onFieldsLoaded: (fields: NearbyFieldSummary[]) => void;
  onFieldClick?: (fieldUuid: string, fieldData: NearbyFieldSummary) => void;
}

export function PesticidkortMap({
  lat,
  lng,
  radiusM,
  year,
  selectedFieldUuid,
  onFieldsLoaded,
  onFieldClick,
}: PesticidkortMapProps) {
  const { mapStyle } = useMapTheme();
  const mapRef = useRef<MapRef>(null);
  const [isReady, setIsReady] = useState(false);
  const onFieldClickRef = useRef(onFieldClick);
  onFieldClickRef.current = onFieldClick;

  const {
    pmtilesUrl,
    overviewUrl,
    buildingsUrl,
    queryNearbyFields,
    fieldsQueriedRef,
  } = usePesticidkortData({
    lat,
    lng,
    radiusM,
    year,
    mapRef,
    onFieldsLoaded,
  });

  useEffect(() => {
    registerPmtilesProtocol();
  }, []);

  const handleMapLoad = useCallback(() => {
    if (!mapRef.current || !pmtilesUrl) return;
    const map = mapRef.current.getMap() as unknown as MapInstance;
    addFieldLayers(map, pmtilesUrl);
    if (overviewUrl) addOverviewLayers(map, overviewUrl);
    if (buildingsUrl) addBuildingLayers(map, buildingsUrl);
    setIsReady(true);

    const rawMap = mapRef.current.getMap();
    rawMap.on('idle', () => {
      if (!fieldsQueriedRef.current) queryNearbyFields();
    });
    setupFieldClickHandlers(rawMap, lat, lng, onFieldClickRef);
  }, [
    pmtilesUrl,
    overviewUrl,
    buildingsUrl,
    queryNearbyFields,
    fieldsQueriedRef,
    lat,
    lng,
  ]);

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
    fieldsQueriedRef.current = false;
  }, [pmtilesUrl, overviewUrl, fieldsQueriedRef]);

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

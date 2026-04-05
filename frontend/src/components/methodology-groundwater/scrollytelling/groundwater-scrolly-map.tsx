'use client';

import { useRef, useEffect, useState, useCallback, useMemo } from 'react';
import Map, { Marker, type MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import {
  ESPE_CATCHMENT,
  ESPE_FIELDS,
  VEJEN_CATCHMENT,
  VEJEN_FIELDS,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-geo-data';
import {
  ESPE_WELL,
  VEJEN_WELL,
  VIEWS,
  type ScrollyStepId,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';
import { CatchmentLayers } from '@/components/methodology-groundwater/scrollytelling/catchment-layers';

function buildGeo(
  catchment: typeof ESPE_CATCHMENT | typeof VEJEN_CATCHMENT,
  fields: typeof ESPE_FIELDS | typeof VEJEN_FIELDS
): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        properties: { kind: 'catchment' },
        geometry: catchment as unknown as GeoJSON.Geometry,
      },
      ...fields.features.map((f) => ({
        type: 'Feature' as const,
        properties: { ...f.properties, kind: 'field' },
        geometry: f.geometry as unknown as GeoJSON.Geometry,
      })),
    ],
  };
}

interface GroundwaterScrollyMapProps {
  step: ScrollyStepId;
}

export function GroundwaterScrollyMap({ step }: GroundwaterScrollyMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [ready, setReady] = useState(false);
  const { mapStyle } = useMapTheme();

  const showVejen = step === 'metabolite' || step === 'conclusion';
  const showEspe = !showVejen || step === 'conclusion';
  const showEspeWell =
    step === 'detection' || step === 'doseresponse' || step === 'conclusion';
  const showVejenWell = step === 'metabolite' || step === 'conclusion';

  const espeGeo = useMemo(() => buildGeo(ESPE_CATCHMENT, ESPE_FIELDS), []);
  const vejenGeo = useMemo(() => buildGeo(VEJEN_CATCHMENT, VEJEN_FIELDS), []);

  const handleLoad = useCallback(() => setReady(true), []);

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
  }, [step, ready]);

  return (
    <Map
      ref={mapRef}
      initialViewState={{ longitude: 10.46, latitude: 55.202, zoom: 13 }}
      mapStyle={mapStyle}
      onLoad={handleLoad}
      attributionControl={false}
      dragRotate={false}
      data-testid="groundwater-scrolly-map"
    >
      {showEspe && (
        <CatchmentLayers
          id="espe"
          data={espeGeo}
          catchmentColor="#3b82f6"
          fieldColor="#ef4444"
        />
      )}
      {showVejen && (
        <CatchmentLayers
          id="vejen"
          data={vejenGeo}
          catchmentColor="#8b5cf6"
          fieldColor="#f59e0b"
        />
      )}
      {showEspeWell && (
        <Marker
          longitude={ESPE_WELL.lng}
          latitude={ESPE_WELL.lat}
          anchor="center"
        >
          <div
            className="bg-destructive h-4 w-4 animate-pulse rounded-full border-2 border-white shadow-md"
            data-testid="espe-well-marker"
          />
        </Marker>
      )}
      {showVejenWell && (
        <Marker
          longitude={VEJEN_WELL.lng}
          latitude={VEJEN_WELL.lat}
          anchor="center"
        >
          <div
            className="h-4 w-4 animate-pulse rounded-full border-2 border-white bg-amber-500 shadow-md"
            data-testid="vejen-well-marker"
          />
        </Marker>
      )}
    </Map>
  );
}

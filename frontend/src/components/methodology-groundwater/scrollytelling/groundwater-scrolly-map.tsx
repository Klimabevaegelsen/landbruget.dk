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
  ESPE_STEPS,
  VEJEN_STEPS,
  ESPE_WELL_STEPS,
  VEJEN_WELL_STEPS,
  PITCHED_STEPS,
  FLY_TO_OVERRIDES,
  type ScrollyStepId,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';
import { CatchmentLayers } from '@/components/methodology-groundwater/scrollytelling/catchment-layers';
import { GwTerrainExtrusion } from '@/components/methodology-groundwater/scrollytelling/gw-terrain-extrusion';
import { GwMapAnnotations } from '@/components/methodology-groundwater/scrollytelling/gw-map-annotations';
import { GwVadoseAnimation } from '@/components/methodology-groundwater/scrollytelling/gw-vadose-animation';

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
  showVadose?: boolean;
}

export function GroundwaterScrollyMap({
  step,
  showVadose = false,
}: GroundwaterScrollyMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [ready, setReady] = useState(false);
  const { mapStyle, getMapStyle } = useMapTheme();
  const isDarkIntro = step === 'intro';
  const activeStyle = isDarkIntro ? getMapStyle('dark') : mapStyle;

  const espeGeo = useMemo(() => buildGeo(ESPE_CATCHMENT, ESPE_FIELDS), []);
  const vejenGeo = useMemo(() => buildGeo(VEJEN_CATCHMENT, VEJEN_FIELDS), []);

  const handleLoad = useCallback(() => setReady(true), []);

  useEffect(() => {
    if (!ready) return;
    const map = mapRef.current?.getMap();
    if (!map) return;
    const v = VIEWS[step];
    const overrides = FLY_TO_OVERRIDES[step];
    map.flyTo({
      center: [v.lng, v.lat],
      zoom: v.zoom,
      pitch: v.pitch ?? 0,
      bearing: v.bearing ?? 0,
      duration: overrides?.duration ?? 1600,
      essential: true,
      curve: overrides?.curve ?? 1.2,
    });
    if (PITCHED_STEPS.has(step)) {
      map.dragRotate.enable();
    } else {
      map.dragRotate.disable();
    }
  }, [step, ready]);

  return (
    <Map
      ref={mapRef}
      initialViewState={{
        longitude: VIEWS.intro.lng,
        latitude: VIEWS.intro.lat,
        zoom: VIEWS.intro.zoom,
        pitch: VIEWS.intro.pitch ?? 0,
        bearing: VIEWS.intro.bearing ?? 0,
      }}
      mapStyle={activeStyle}
      onLoad={handleLoad}
      attributionControl={false}
      dragRotate={false}
      data-testid="groundwater-scrolly-map"
    >
      {ESPE_STEPS.has(step) && (
        <CatchmentLayers
          id="espe"
          data={espeGeo}
          catchmentColor="#3b82f6"
          fieldColor="#ef4444"
        />
      )}
      {VEJEN_STEPS.has(step) && (
        <CatchmentLayers
          id="vejen"
          data={vejenGeo}
          catchmentColor="#8b5cf6"
          fieldColor="#f59e0b"
        />
      )}
      <GwTerrainExtrusion step={step} />
      {ESPE_WELL_STEPS.has(step) && (
        <Marker
          longitude={ESPE_WELL.lng}
          latitude={ESPE_WELL.lat}
          anchor="center"
        >
          <div
            className="bg-destructive border-background h-4 w-4 animate-pulse rounded-full border-2 shadow-md"
            data-testid="espe-well-marker"
          />
        </Marker>
      )}
      {VEJEN_WELL_STEPS.has(step) && (
        <Marker
          longitude={VEJEN_WELL.lng}
          latitude={VEJEN_WELL.lat}
          anchor="center"
        >
          <div
            className="border-background bg-warning h-4 w-4 animate-pulse rounded-full border-2 shadow-md"
            data-testid="vejen-well-marker"
          />
        </Marker>
      )}
      <GwVadoseAnimation
        variant={
          step === 'mcpa' || step === 'fortidslevn' ? 'mcpa' : 'bentazon'
        }
        visible={showVadose}
      />
      <GwMapAnnotations step={step} />
    </Map>
  );
}

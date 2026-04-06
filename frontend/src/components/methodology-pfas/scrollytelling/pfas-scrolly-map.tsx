'use client';

import { useRef, useEffect, useState, useCallback, useMemo } from 'react';
import Map, { Marker, type MapRef } from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import {
  SKRYDSTRUP_CATCHMENT,
  HADERSLEV_FIELDS,
} from '@/components/methodology-pfas/scrollytelling/scrolly-geo-data';
import {
  WELLS,
  VIEWS,
  type PfasScrollyStepId,
} from '@/components/methodology-pfas/scrollytelling/scrolly-constants';
import { CatchmentLayers } from '@/components/methodology-groundwater/scrollytelling/catchment-layers';
import { buildGeo, buildUnmonitoredGeo } from './pfas-geo-builders';
import { CatchmentChoropleth } from './catchment-choropleth';
import { PfasMapAnnotations } from './pfas-map-annotations';

interface PfasScrollyMapProps {
  step: PfasScrollyStepId;
}

const CATCHMENT_STEPS = new Set<PfasScrollyStepId>([
  'field',
  'product',
  'molecule',
  'tfa',
  'detection',
]);
const WELL_STEPS = new Set<PfasScrollyStepId>(['tfa', 'detection']);
const NATIONAL_STEPS = new Set<PfasScrollyStepId>([
  'everywhere',
  'blindspot',
  'conclusion',
]);

export function PfasScrollyMap({ step }: PfasScrollyMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [ready, setReady] = useState(false);
  const { mapStyle } = useMapTheme();

  const showCatchment = CATCHMENT_STEPS.has(step);
  const showWells = WELL_STEPS.has(step);
  const showNational = NATIONAL_STEPS.has(step);
  const showUnmonitored = step === 'blindspot';

  const skrydstrupGeo = useMemo(
    () => buildGeo(SKRYDSTRUP_CATCHMENT, HADERSLEV_FIELDS),
    []
  );
  const unmonitoredGeo = useMemo(() => buildUnmonitoredGeo(), []);

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
      initialViewState={{
        longitude: VIEWS.intro.lng,
        latitude: VIEWS.intro.lat,
        zoom: VIEWS.intro.zoom,
      }}
      mapStyle={mapStyle}
      onLoad={handleLoad}
      attributionControl={false}
      dragRotate={false}
      data-testid="pfas-scrolly-map"
    >
      {showCatchment && (
        <CatchmentLayers
          id="skrydstrup"
          data={skrydstrupGeo}
          catchmentColor="#8b5cf6"
          fieldColor="#22c55e"
        />
      )}
      {showUnmonitored && (
        <CatchmentLayers
          id="sonder-felding"
          data={unmonitoredGeo}
          catchmentColor="#94a3b8"
          fieldColor="#94a3b8"
        />
      )}
      <CatchmentChoropleth visible={showNational} />
      {showWells &&
        WELLS.map((w) => (
          <Marker
            key={w.dgu}
            longitude={w.lng}
            latitude={w.lat}
            anchor="center"
          >
            <div
              className="bg-destructive border-background h-3.5 w-3.5 animate-pulse rounded-full border-2 shadow-md"
              data-testid={`well-marker-${w.dgu.replace(/\s/g, '')}`}
            />
          </Marker>
        ))}
      <PfasMapAnnotations step={step} />
    </Map>
  );
}

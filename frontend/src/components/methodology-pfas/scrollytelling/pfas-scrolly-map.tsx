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
  SKRYDSTRUP_WELL,
  type PfasScrollyStepId,
} from '@/components/methodology-pfas/scrollytelling/scrolly-constants';
import { CatchmentLayers } from '@/components/methodology-groundwater/scrollytelling/catchment-layers';
import { buildGeo, buildUnmonitoredGeo } from './pfas-geo-builders';
import { CatchmentChoropleth } from './catchment-choropleth';
import { PfasMapAnnotations } from './pfas-map-annotations';
import { useZoomLoop } from './pfas-zoom-loop';

interface PfasScrollyMapProps {
  step: PfasScrollyStepId;
}

const CATCHMENT_STEPS = new Set<PfasScrollyStepId>([
  'byggeklodser',
  'evighed',
  'glasset',
]);
const WELL_STEPS = new Set<PfasScrollyStepId>(['evighed', 'glasset']);
const NATIONAL_STEPS = new Set<PfasScrollyStepId>([
  'overalt',
  'blindvinkel',
  'spoergsmaalet',
]);

export function PfasScrollyMap({ step }: PfasScrollyMapProps) {
  const mapRef = useRef<MapRef>(null);
  const [ready, setReady] = useState(false);
  const { mapStyle } = useMapTheme();

  const showCatchment = CATCHMENT_STEPS.has(step);
  const showWells = WELL_STEPS.has(step);
  const showNational = NATIONAL_STEPS.has(step);
  const showUnmonitored = step === 'blindvinkel';

  const skrydstrupGeo = useMemo(
    () => buildGeo(SKRYDSTRUP_CATCHMENT, HADERSLEV_FIELDS),
    []
  );
  const unmonitoredGeo = useMemo(() => buildUnmonitoredGeo(), []);

  const handleLoad = useCallback(() => setReady(true), []);

  useZoomLoop(mapRef, step === 'spoergsmaalet' && ready);

  useEffect(() => {
    if (!ready) return;
    const map = mapRef.current?.getMap();
    if (!map) return;
    if (step === 'spoergsmaalet') return;
    const v = VIEWS[step];
    map.flyTo({
      center: [v.lng, v.lat],
      zoom: v.zoom,
      duration: 1600,
      essential: true,
      curve: 1.2,
    });
  }, [step, ready]);

  const isPrimaryWell = (dgu: string) => dgu === SKRYDSTRUP_WELL.dgu;

  return (
    <Map
      ref={mapRef}
      initialViewState={{
        longitude: VIEWS.skjult.lng,
        latitude: VIEWS.skjult.lat,
        zoom: VIEWS.skjult.zoom,
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
      <CatchmentChoropleth visible={showNational} voidMode={showUnmonitored} />
      {showWells &&
        WELLS.map((w) => (
          <Marker
            key={w.dgu}
            longitude={w.lng}
            latitude={w.lat}
            anchor="center"
          >
            <div
              className={`bg-destructive border-background rounded-full border-2 shadow-md ${
                isPrimaryWell(w.dgu) && step === 'glasset'
                  ? 'h-5 w-5 animate-pulse ring-2 ring-red-400/50 ring-offset-1'
                  : 'h-3.5 w-3.5 animate-pulse'
              }`}
              data-testid={`well-marker-${w.dgu.replace(/\s/g, '')}`}
            />
          </Marker>
        ))}
      <PfasMapAnnotations step={step} />
    </Map>
  );
}

'use client';

import { Layer, Source } from '@vis.gl/react-maplibre';
import { useMemo } from 'react';
import {
  ESPE_STEPS,
  VEJEN_STEPS,
  PITCHED_STEPS,
  type ScrollyStepId,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-constants';
import {
  ESPE_CATCHMENT,
  VEJEN_CATCHMENT,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-geo-data';

interface GwTerrainExtrusionProps {
  step: ScrollyStepId;
}

/**
 * Fill-extrusion over the real catchment polygon to give a "lifted
 * terrain / cutaway" impression when the camera is pitched.
 * Switches between Espe and Vejen catchments based on the active step.
 */
export function GwTerrainExtrusion({ step }: GwTerrainExtrusionProps) {
  const pitched = PITCHED_STEPS.has(step);
  const isEspe = ESPE_STEPS.has(step);
  const isVejen = VEJEN_STEPS.has(step);
  const visible = pitched && (isEspe || isVejen);

  const data = useMemo<GeoJSON.FeatureCollection>(
    () => ({
      type: 'FeatureCollection',
      features: [
        {
          type: 'Feature',
          properties: { kind: 'terrain', area: 'espe' },
          geometry: ESPE_CATCHMENT as unknown as GeoJSON.Geometry,
        },
        {
          type: 'Feature',
          properties: { kind: 'terrain', area: 'vejen' },
          geometry: VEJEN_CATCHMENT as unknown as GeoJSON.Geometry,
        },
      ],
    }),
    []
  );

  if (!visible) return null;

  const activeArea = isVejen && !isEspe ? 'vejen' : 'espe';

  return (
    <Source id="terrain-extrusion" type="geojson" data={data}>
      <Layer
        id="terrain-extrusion-layer"
        type="fill-extrusion"
        filter={['==', ['get', 'area'], activeArea]}
        paint={{
          'fill-extrusion-color': '#8b7355',
          'fill-extrusion-opacity': 0.12,
          'fill-extrusion-height': 150,
          'fill-extrusion-base': 0,
        }}
      />
    </Source>
  );
}

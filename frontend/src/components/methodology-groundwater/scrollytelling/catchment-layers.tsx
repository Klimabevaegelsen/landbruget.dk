'use client';

import { Source, Layer } from '@vis.gl/react-maplibre';

interface CatchmentLayersProps {
  id: string;
  data: GeoJSON.FeatureCollection;
  catchmentColor: string;
  fieldColor: string;
}

export function CatchmentLayers({
  id,
  data,
  catchmentColor,
  fieldColor,
}: CatchmentLayersProps) {
  return (
    <Source id={id} type="geojson" data={data}>
      <Layer
        id={`${id}-catchment-fill`}
        type="fill"
        filter={['==', ['get', 'kind'], 'catchment']}
        paint={{
          'fill-color': catchmentColor,
          'fill-opacity': 0.08,
          'fill-opacity-transition': { duration: 600, delay: 0 },
        }}
      />
      <Layer
        id={`${id}-catchment-line`}
        type="line"
        filter={['==', ['get', 'kind'], 'catchment']}
        paint={{
          'line-color': catchmentColor,
          'line-width': 2,
          'line-dasharray': [4, 3],
          'line-opacity': 0.6,
        }}
      />
      <Layer
        id={`${id}-fields-fill`}
        type="fill"
        filter={['==', ['get', 'kind'], 'field']}
        paint={{
          'fill-color': fieldColor,
          'fill-opacity': 0.5,
          'fill-opacity-transition': { duration: 500, delay: 0 },
        }}
      />
      <Layer
        id={`${id}-fields-line`}
        type="line"
        filter={['==', ['get', 'kind'], 'field']}
        paint={{ 'line-color': fieldColor, 'line-width': 1.5 }}
      />
    </Source>
  );
}

'use client';

import { useEffect, useState, useMemo } from 'react';
import { Source, Layer } from '@vis.gl/react-maplibre';

interface Centroid {
  id: string;
  lng: number;
  lat: number;
  w: number;
}

interface CatchmentChoroplethProps {
  visible: boolean;
}

function toGeoJSON(centroids: Centroid[]): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: centroids.map((c) => ({
      type: 'Feature' as const,
      properties: { monitored: c.w > 0 },
      geometry: { type: 'Point' as const, coordinates: [c.lng, c.lat] },
    })),
  };
}

export function CatchmentChoropleth({ visible }: CatchmentChoroplethProps) {
  const [centroids, setCentroids] = useState<Centroid[] | null>(null);

  useEffect(() => {
    if (!visible || centroids) return;
    fetch('/data/pfas-centroids.json')
      .then((r) => r.json())
      .then(setCentroids)
      .catch(() => {});
  }, [visible, centroids]);

  const data = useMemo(
    () => (centroids ? toGeoJSON(centroids) : null),
    [centroids]
  );

  if (!visible || !data) return null;

  return (
    <Source id="pfas-centroids" type="geojson" data={data}>
      <Layer
        id="pfas-centroids-unmonitored"
        type="circle"
        filter={['==', ['get', 'monitored'], false]}
        paint={{
          'circle-radius': 3,
          'circle-color': '#94a3b8',
          'circle-opacity': 0.5,
        }}
      />
      <Layer
        id="pfas-centroids-monitored"
        type="circle"
        filter={['==', ['get', 'monitored'], true]}
        paint={{
          'circle-radius': 3.5,
          'circle-color': '#ef4444',
          'circle-opacity': 0.7,
        }}
      />
    </Source>
  );
}

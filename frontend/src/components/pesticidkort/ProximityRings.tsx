import { Source, Layer } from '@vis.gl/react-maplibre';
import { useMemo, useEffect, useState } from 'react';
import { resolvePesticidkortColor } from '@/components/pesticidkort/color-theme';

interface ProximityRingsProps {
  lat: number;
  lng: number;
  radiusM: number;
}

function createCircleGeoJSON(
  lat: number,
  lng: number,
  radiusM: number,
  steps = 64
): GeoJSON.Feature<GeoJSON.Polygon> {
  const coords: [number, number][] = [];
  for (let i = 0; i <= steps; i++) {
    const angle = (i / steps) * 2 * Math.PI;
    const dLat = (radiusM * Math.cos(angle)) / 111320;
    const dLng =
      (radiusM * Math.sin(angle)) / (111320 * Math.cos((lat * Math.PI) / 180));
    coords.push([lng + dLng, lat + dLat]);
  }
  return {
    type: 'Feature',
    properties: {},
    geometry: { type: 'Polygon', coordinates: [coords] },
  };
}

export function ProximityRings({ lat, lng, radiusM }: ProximityRingsProps) {
  const [isVisible, setIsVisible] = useState(false);
  const ringColor = resolvePesticidkortColor('ring');
  const geojson = useMemo(
    () => ({
      type: 'FeatureCollection' as const,
      features: [
        createCircleGeoJSON(lat, lng, radiusM),
        createCircleGeoJSON(lat, lng, radiusM / 2),
      ],
    }),
    [lat, lng, radiusM]
  );

  useEffect(() => {
    setIsVisible(false);
    const frameId = requestAnimationFrame(() => setIsVisible(true));
    return () => cancelAnimationFrame(frameId);
  }, [lat, lng, radiusM]);

  return (
    <Source id="proximity-rings" type="geojson" data={geojson}>
      <Layer
        id="proximity-rings-fill"
        type="fill"
        paint={{
          'fill-color': ringColor,
          'fill-opacity': isVisible ? 0.04 : 0,
          'fill-opacity-transition': { duration: 650, delay: 50 },
        }}
      />
      <Layer
        id="proximity-rings-line"
        type="line"
        paint={{
          'line-color': ringColor,
          'line-width': 1.5,
          'line-dasharray': [4, 3],
          'line-opacity': isVisible ? 0.5 : 0,
          'line-opacity-transition': { duration: 700, delay: 120 },
        }}
      />
    </Source>
  );
}

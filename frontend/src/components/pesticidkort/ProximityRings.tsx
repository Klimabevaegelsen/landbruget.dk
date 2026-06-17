import { Source, Layer, Marker } from '@vis.gl/react-maplibre';
import { useMemo, useEffect, useState } from 'react';
import { resolvePesticidkortColor } from '@/components/pesticidkort/color-theme';
import { getExposureRings } from '@/components/pesticidkort/exposure-utils';
import {
  latitudeDeltaForMeters,
  longitudeDeltaForMeters,
  offsetLatLngByMeters,
} from '@/components/pesticidkort/map-utils';

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
    const dLat = latitudeDeltaForMeters(radiusM * Math.cos(angle));
    const dLng = longitudeDeltaForMeters(lat, radiusM * Math.sin(angle));
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
  const rings = useMemo(() => getExposureRings(radiusM), [radiusM]);
  const geojson = useMemo(
    () => ({
      type: 'FeatureCollection' as const,
      features: rings.map(({ radiusM }) =>
        createCircleGeoJSON(lat, lng, radiusM)
      ),
    }),
    [lat, lng, rings]
  );

  useEffect(() => {
    setIsVisible(false);
    const frameId = requestAnimationFrame(() => setIsVisible(true));
    return () => cancelAnimationFrame(frameId);
  }, [lat, lng, radiusM]);

  return (
    <>
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
      {rings.map(({ id, radiusM, label }) => {
        const labelPosition = offsetLatLngByMeters(lat, lng, radiusM);
        return (
          <Marker
            key={id}
            latitude={labelPosition.lat}
            longitude={labelPosition.lng}
            anchor="bottom"
          >
            <div
              data-testid={`ring-label-${id}`}
              className="border-border bg-background/90 text-foreground rounded-full border px-1.5 py-0.5 text-[10px] font-medium shadow-sm backdrop-blur-sm"
            >
              {label}
            </div>
          </Marker>
        );
      })}
    </>
  );
}

import {
  SKRYDSTRUP_CATCHMENT,
  SONDER_FELDING_CATCHMENT,
  SKRYDSTRUP_FIELDS,
  SONDER_FELDING_FIELDS,
} from '@/components/methodology-pfas/scrollytelling/scrolly-geo-data';

export function buildGeo(
  catchment: typeof SKRYDSTRUP_CATCHMENT,
  fields?: typeof SKRYDSTRUP_FIELDS
): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        properties: { kind: 'catchment' },
        geometry: catchment as unknown as GeoJSON.Geometry,
      },
      ...(fields?.features.map((f) => ({
        type: 'Feature' as const,
        properties: { ...f.properties, kind: 'field' },
        geometry: f.geometry as unknown as GeoJSON.Geometry,
      })) ?? []),
    ],
  };
}

export function buildUnmonitoredGeo(): GeoJSON.FeatureCollection {
  return {
    type: 'FeatureCollection',
    features: [
      {
        type: 'Feature',
        properties: { kind: 'catchment' },
        geometry: SONDER_FELDING_CATCHMENT as unknown as GeoJSON.Geometry,
      },
      ...SONDER_FELDING_FIELDS.features.map((f) => ({
        type: 'Feature' as const,
        properties: { ...f.properties, kind: 'field' },
        geometry: f.geometry as unknown as GeoJSON.Geometry,
      })),
    ],
  };
}

import {
  ESPE_CATCHMENT,
  ESPE_FIELDS,
  VEJEN_CATCHMENT,
  VEJEN_FIELDS,
} from '@/components/methodology-groundwater/scrollytelling/scrolly-geo-data';

export function buildGeo(
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

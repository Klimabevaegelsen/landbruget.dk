import type { NearbyFieldSummary } from '@/components/pesticidkort/types';

/** Convert PMTiles feature properties to NearbyFieldSummary */
export function featureToFieldSummary(
  props: Record<string, unknown>,
  userLat: number,
  userLng: number,
  centroidLat: number,
  centroidLng: number
): NearbyFieldSummary {
  return {
    field_uuid: String(props.field_uuid ?? ''),
    crop_name: String(props.crop_name ?? 'Ukendt'),
    area_hectares: Number(props.area_hectares ?? 0),
    distance_m: haversineDistance(userLat, userLng, centroidLat, centroidLng),
    is_organic: props.is_organic === true || props.is_organic === 'true',
    kommune: String(props.kommune ?? ''),
    total_pesticide_belastning: Number(props.total_pesticide_belastning ?? 0),
    total_pesticide_applications: Number(
      props.total_pesticide_applications ?? 0
    ),
    pfas_applications: Number(props.pfas_applications ?? 0),
    pfas_products_detail: optionalString(props.pfas_products_detail),
    diquat_applications: Number(props.diquat_applications ?? 0),
    diquat_products_detail: optionalString(props.diquat_products_detail),
    glyphosate_applications: Number(props.glyphosate_applications ?? 0),
    glyphosate_products_detail: optionalString(
      props.glyphosate_products_detail
    ),
    other_applications: Number(props.other_applications ?? 0),
    other_products_detail: optionalString(props.other_products_detail),
    residential_buildings_proximity: optionalString(
      props.residential_buildings_proximity
    ),
    educational_facilities_proximity: optionalString(
      props.educational_facilities_proximity
    ),
    water_distance_proximity: optionalString(props.water_distance_proximity),
    bnbo_area_hectares: Number(props.bnbo_area_hectares ?? 0),
    total_pfas_active_ingredient_kg: Number(
      props.total_pfas_active_ingredient_kg ?? 0
    ),
    total_glyphosate_active_ingredient_kg: Number(
      props.total_glyphosate_active_ingredient_kg ?? 0
    ),
  };
}

function optionalString(val: unknown): string | undefined {
  if (val === null || val === undefined || val === '') return undefined;
  return String(val);
}

/** Haversine distance in meters between two lat/lng points */
export function haversineDistance(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number
): number {
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLng / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function toRad(deg: number): number {
  return (deg * Math.PI) / 180;
}

/**
 * Approximate bounding box for a radius around a point.
 * Returns [west, south, east, north] in degrees.
 */
export function radiusToBbox(
  lat: number,
  lng: number,
  radiusM: number
): [number, number, number, number] {
  const latDelta = radiusM / 111320;
  const lngDelta = radiusM / (111320 * Math.cos(toRad(lat)));
  return [lng - lngDelta, lat - latDelta, lng + lngDelta, lat + latDelta];
}

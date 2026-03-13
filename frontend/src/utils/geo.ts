import type { Geometry } from 'geojson';

/**
 * Compute an approximate centroid from a GeoJSON Polygon or MultiPolygon geometry.
 * Uses the mean of the outer ring vertices (excluding the duplicate closing vertex
 * per RFC 7946 §3.1.6).
 *
 * Returns null for unsupported geometry types or empty coordinate arrays.
 */
export function computeCentroid(
  geometry: Geometry
): { lat: number; lng: number } | null {
  const coords =
    geometry.type === 'Polygon'
      ? geometry.coordinates[0]
      : geometry.type === 'MultiPolygon'
        ? geometry.coordinates[0][0]
        : null;

  // A valid ring has at least 4 positions (3 unique + closing duplicate)
  if (!coords || coords.length < 4) {
    return null;
  }

  // Exclude the closing vertex (duplicate of first) per GeoJSON spec
  const uniqueCount = coords.length - 1;
  let sumLng = 0;
  let sumLat = 0;

  for (let i = 0; i < uniqueCount; i++) {
    sumLng += coords[i][0];
    sumLat += coords[i][1];
  }

  return {
    lat: sumLat / uniqueCount,
    lng: sumLng / uniqueCount,
  };
}

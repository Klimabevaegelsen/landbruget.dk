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

const DEG_TO_RAD = Math.PI / 180;
const EARTH_RADIUS_M = 6371000;

/**
 * Minimum haversine distance in meters from a point to the nearest edge of a
 * GeoJSON Polygon or MultiPolygon. For MultiPolygon, only the first polygon's
 * outer ring is checked (consistent with computeCentroid).
 *
 * Returns Infinity for unsupported geometry types.
 */
export function pointToPolygonEdgeDistance(
  lat: number,
  lng: number,
  geometry: Geometry
): number {
  const rings: number[][][] | null =
    geometry.type === 'Polygon'
      ? geometry.coordinates
      : geometry.type === 'MultiPolygon'
        ? geometry.coordinates[0]
        : null;

  if (!rings || rings.length === 0) return Infinity;

  let minDist = Infinity;

  for (const ring of rings) {
    if (ring.length < 2) continue;
    for (let i = 0; i < ring.length - 1; i++) {
      const dist = pointToSegmentDistance(
        lat,
        lng,
        ring[i][1],
        ring[i][0],
        ring[i + 1][1],
        ring[i + 1][0]
      );
      if (dist < minDist) minDist = dist;
    }
  }

  return minDist;
}

/**
 * Approximate distance from a point to a line segment on a sphere.
 * Projects the point onto the segment (clamped to [0,1]) using a local
 * flat-earth approximation, then computes haversine to the projected point.
 */
function pointToSegmentDistance(
  pLat: number,
  pLng: number,
  aLat: number,
  aLng: number,
  bLat: number,
  bLng: number
): number {
  const cosLat = Math.cos(pLat * DEG_TO_RAD);
  const dx = (bLng - aLng) * cosLat;
  const dy = bLat - aLat;
  const px = (pLng - aLng) * cosLat;
  const py = pLat - aLat;

  const dot = px * dx + py * dy;
  const lenSq = dx * dx + dy * dy;
  const t = lenSq === 0 ? 0 : Math.max(0, Math.min(1, dot / lenSq));

  const nearLat = aLat + t * (bLat - aLat);
  const nearLng = aLng + t * (bLng - aLng);

  return haversineDist(pLat, pLng, nearLat, nearLng);
}

function haversineDist(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number
): number {
  const dLat = (lat2 - lat1) * DEG_TO_RAD;
  const dLng = (lng2 - lng1) * DEG_TO_RAD;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * DEG_TO_RAD) *
      Math.cos(lat2 * DEG_TO_RAD) *
      Math.sin(dLng / 2) ** 2;
  return EARTH_RADIUS_M * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

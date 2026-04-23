/* oxlint-disable landbruget/require-test-coverage */
export const DEFAULT_DRIFT_TILE_ZOOM = 12;
export const MATCH_TOLERANCE_M = 60;

const TILE_FETCH_RADIUS = 1;
const toRad = (d: number) => (d * Math.PI) / 180;

export function haversineMeters(
  aLat: number,
  aLng: number,
  bLat: number,
  bLng: number
): number {
  const R = 6371000;
  const dLat = toRad(bLat - aLat);
  const dLng = toRad(bLng - aLng);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLng / 2) ** 2;
  return 2 * R * Math.asin(Math.sqrt(h));
}

export function lngLatToTile(lat: number, lng: number, zoom: number) {
  const n = 2 ** zoom;
  const x = Math.floor(((lng + 180) / 360) * n);
  const latRad = toRad(lat);
  const y = Math.floor(
    ((1 - Math.log(Math.tan(latRad) + 1 / Math.cos(latRad)) / Math.PI) / 2) * n
  );
  return { x, y, z: zoom };
}

export function surroundingTiles(x: number, y: number, z: number) {
  const tiles: { x: number; y: number; z: number }[] = [];
  const maxIndex = 2 ** z - 1;
  for (let dx = -TILE_FETCH_RADIUS; dx <= TILE_FETCH_RADIUS; dx++) {
    for (let dy = -TILE_FETCH_RADIUS; dy <= TILE_FETCH_RADIUS; dy++) {
      const tileX = x + dx;
      const tileY = y + dy;
      if (tileX < 0 || tileX > maxIndex || tileY < 0 || tileY > maxIndex) {
        continue;
      }
      tiles.push({ x: tileX, y: tileY, z });
    }
  }
  return tiles;
}

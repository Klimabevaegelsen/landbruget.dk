/**
 * Coordinate conversion utilities for field analysis
 * Converts between WGS84 (lat/lng) and UTM Zone 32N coordinates
 */

// UTM Zone 32N parameters for Denmark (commented out as they're not used in the current implementation)
// const UTM_ZONE = 32;
// const UTM_HEMISPHERE = 'N';

/**
 * Convert WGS84 coordinates to UTM Zone 32N
 * Uses a simplified conversion suitable for Denmark
 * For more precise conversion, consider using a dedicated projection library
 */
export function wgs84ToUtm32(
  lat: number,
  lng: number
): { x: number; y: number } {
  // Convert degrees to radians
  const latRad = (lat * Math.PI) / 180;
  const lngRad = (lng * Math.PI) / 180;

  // UTM Zone 32N central meridian (9° E)
  const centralMeridian = (9 * Math.PI) / 180;
  const lngDiff = lngRad - centralMeridian;

  // WGS84 ellipsoid parameters
  const a = 6378137.0; // Semi-major axis
  const f = 1 / 298.257223563; // Flattening
  const e2 = 2 * f - f * f; // First eccentricity squared
  const e2p = e2 / (1 - e2); // Second eccentricity squared

  // UTM scale factor
  const k0 = 0.9996;

  // Calculate various terms
  const N = a / Math.sqrt(1 - e2 * Math.sin(latRad) * Math.sin(latRad));
  const T = Math.tan(latRad) * Math.tan(latRad);
  const C = e2p * Math.cos(latRad) * Math.cos(latRad);
  const A = Math.cos(latRad) * lngDiff;

  // Calculate M (meridional arc)
  // const e1 = (1 - Math.sqrt(1 - e2)) / (1 + Math.sqrt(1 - e2)); // Not used in current calculation
  const M =
    a *
    ((1 - e2 / 4 - (3 * e2 * e2) / 64 - (5 * e2 * e2 * e2) / 256) * latRad -
      ((3 * e2) / 8 + (3 * e2 * e2) / 32 + (45 * e2 * e2 * e2) / 1024) *
        Math.sin(2 * latRad) +
      ((15 * e2 * e2) / 256 + (45 * e2 * e2 * e2) / 1024) *
        Math.sin(4 * latRad) -
      ((35 * e2 * e2 * e2) / 3072) * Math.sin(6 * latRad));

  // Calculate UTM coordinates
  const x =
    k0 *
      N *
      (A +
        ((1 - T + C) * A * A * A) / 6 +
        ((5 - 18 * T + T * T + 72 * C - 58 * e2p) * A * A * A * A * A) / 120) +
    500000; // False easting

  const y =
    k0 *
    (M +
      N *
        Math.tan(latRad) *
        ((A * A) / 2 +
          ((5 - T + 9 * C + 4 * C * C) * A * A * A * A) / 24 +
          ((61 - 58 * T + T * T + 600 * C - 330 * e2p) *
            A *
            A *
            A *
            A *
            A *
            A) /
            720));

  return { x: Math.round(x * 100) / 100, y: Math.round(y * 100) / 100 };
}

/**
 * Format WGS84 coordinates for display
 */
export function formatWgs84Coordinates(lat: number, lng: number): string {
  const latFormatted = Math.abs(lat).toFixed(6);
  const lngFormatted = Math.abs(lng).toFixed(6);
  const latDir = lat >= 0 ? 'N' : 'S';
  const lngDir = lng >= 0 ? 'E' : 'W';

  return `${latFormatted}°${latDir}, ${lngFormatted}°${lngDir}`;
}

/**
 * Generate Skråfoto URL from WGS84 coordinates
 */
export function generateSkraafotoUrl(lat: number, lng: number): string {
  const utm = wgs84ToUtm32(lat, lng);
  return `https://skraafoto.dataforsyningen.dk/?center=${utm.x},${utm.y}`;
}

/**
 * Generate Google Maps URL from WGS84 coordinates
 */
export function generateGoogleMapsUrl(lat: number, lng: number): string {
  return `https://www.google.com/maps?q=${lat},${lng}`;
}

/**
 * Copy coordinates to clipboard
 */
export async function copyCoordinatesToClipboard(
  lat: number,
  lng: number
): Promise<boolean> {
  try {
    const formatted = formatWgs84Coordinates(lat, lng);
    await navigator.clipboard.writeText(formatted);
    return true;
  } catch (error) {
    console.error('Failed to copy coordinates to clipboard:', error);
    return false;
  }
}

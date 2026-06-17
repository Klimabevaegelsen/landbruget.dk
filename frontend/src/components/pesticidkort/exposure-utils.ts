export const NEAR_EXPOSURE_RADIUS_M = 100;
export const DEFAULT_EXPOSURE_RADIUS_M = 1000;

export interface ExposureRing {
  id: string;
  radiusM: number;
  label: string;
}

export function formatRadiusLabel(radiusM: number): string {
  if (radiusM >= 1000) {
    const kilometers = radiusM / 1000;
    const precision = Number.isInteger(kilometers) ? 0 : 1;
    return `${kilometers.toFixed(precision).replace('.', ',')} km`;
  }
  return `${Math.round(radiusM)} m`;
}

export function getExposureRings(radiusM: number): ExposureRing[] {
  return uniqueRadii([NEAR_EXPOSURE_RADIUS_M, radiusM]).map((radius) => ({
    id: `${radius}m`,
    radiusM: radius,
    label: formatRadiusLabel(radius),
  }));
}

function uniqueRadii(radii: number[]): number[] {
  const seen = new Set<number>();
  const unique: number[] = [];
  for (const radius of radii) {
    if (seen.has(radius)) continue;
    seen.add(radius);
    unique.push(radius);
  }
  return unique;
}

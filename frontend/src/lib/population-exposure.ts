/* oxlint-disable landbruget/require-test-coverage */

import type {
  PopulationExposureDefinition,
  PopulationExposureSummary,
} from '@/lib/server-cache';

export interface PopulationHeadline {
  persons: number;
  bandM: number;
  pesticideYear: number;
}

export function getHeadline(
  summary: PopulationExposureSummary | null,
  definition: PopulationExposureDefinition = 'treated_field',
  bandM = 100
): PopulationHeadline | null {
  if (
    !summary ||
    summary.schema_version !== 1 ||
    typeof summary.generated_at !== 'string' ||
    !Number.isInteger(summary.pesticide_year) ||
    !Number.isInteger(summary.field_year) ||
    !Array.isArray(summary.bands_m) ||
    !summary.bands_m.includes(bandM) ||
    !Number.isFinite(bandM) ||
    bandM <= 0
  ) {
    return null;
  }

  const bands = summary.national?.[definition];
  if (!Array.isArray(bands)) return null;

  const band = bands.find((item) => item?.band_m === bandM);
  if (
    !band ||
    typeof band.persons !== 'number' ||
    !Number.isFinite(band.persons) ||
    band.persons <= 0
  ) {
    return null;
  }

  return {
    persons: band.persons,
    bandM,
    pesticideYear: summary.pesticide_year,
  };
}

export function formatApproxPersons(n: number): string {
  const rounded =
    n >= 10_000 ? Math.round(n / 1_000) * 1_000 : Math.round(n / 100) * 100;
  return new Intl.NumberFormat('da-DK', { maximumFractionDigits: 0 }).format(
    rounded
  );
}

export function formatShare(share: number): string {
  if (share > 0 && share < 0.001) return '< 0,1 %';

  return new Intl.NumberFormat('da-DK', {
    style: 'percent',
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  })
    .format(share)
    .replace(/[\u00a0\u202f]/g, ' ');
}

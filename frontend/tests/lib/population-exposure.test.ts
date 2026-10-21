import { describe, expect, it } from 'vitest';
import type { PopulationExposureSummary } from '@/lib/server-cache';
import {
  formatApproxPersons,
  formatShare,
  getHeadline,
} from '@/lib/population-exposure';

function band(bandM: number, persons: number, share: number) {
  return {
    band_m: bandM,
    dwellings: persons / 2,
    persons,
    share_of_population: share,
  };
}

function summary(): PopulationExposureSummary {
  return {
    schema_version: 1,
    generated_at: '2026-10-20T21:00:00Z',
    pesticide_year: 2024,
    field_year: 2025,
    max_distance_m: 1000,
    bands_m: [10, 30, 50, 100, 250, 500, 1000],
    population_source: {
      provider: 'Danmarks Statistik',
      table: 'FOLK1A',
      period: '2026K3',
      total_population: 6031699,
      population_unallocated: 0,
    },
    buildings: {
      residential_buildings: 0,
      dwellings: 0,
      buildings_without_kommune: 0,
      dwellings_without_kommune: 0,
    },
    national: {
      treated_field: [band(10, 1000, 0.001), band(100, 312456, 0.052)],
      conventional_field: [band(10, 2000, 0.002)],
      any_field: [band(10, 3000, 0.003), band(500, 8456, 0.0014)],
    },
    municipalities: [],
  };
}

describe('getHeadline', () => {
  it('returns the treated-field 100 m population and pesticide year by default', () => {
    expect(getHeadline(summary())).toEqual({
      persons: 312456,
      bandM: 100,
      pesticideYear: 2024,
    });
  });

  it('supports a different field definition and distance', () => {
    expect(getHeadline(summary(), 'any_field', 500)).toEqual({
      persons: 8456,
      bandM: 500,
      pesticideYear: 2024,
    });
  });

  it('returns null for a null or invalid summary', () => {
    expect(getHeadline(null)).toBeNull();
    expect(getHeadline({} as PopulationExposureSummary)).toBeNull();

    const unsupportedVersion = summary();
    unsupportedVersion.schema_version = 2;
    expect(getHeadline(unsupportedVersion)).toBeNull();
  });

  it('returns null when the requested band is missing or has no persons', () => {
    const data = summary();
    expect(getHeadline(data, 'treated_field', 25)).toBeNull();
    data.national.treated_field[1].persons = 0;
    expect(getHeadline(data)).toBeNull();
  });
});

describe('formatApproxPersons', () => {
  it('rounds to the nearest thousand at 10,000 or above', () => {
    expect(formatApproxPersons(312456)).toBe('312.000');
  });

  it('rounds to the nearest hundred below 10,000', () => {
    expect(formatApproxPersons(8456)).toBe('8.500');
  });
});

describe('formatShare', () => {
  it('formats Danish percentages with one decimal place', () => {
    expect(formatShare(0.052)).toBe('5,2 %');
  });

  it('uses a less-than label for positive shares below 0.1 percent', () => {
    expect(formatShare(0.0009)).toBe('< 0,1 %');
  });
});

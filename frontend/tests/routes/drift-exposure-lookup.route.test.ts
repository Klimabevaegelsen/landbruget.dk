import { beforeEach, describe, expect, it, vi } from 'vitest';

import { GET } from '@/app/api/drift-exposure/lookup/route';
import {
  getCachedDriftExposureIndex,
  getCachedDriftExposureTile,
} from '@/lib/server-cache';

import { createRouteRequest } from './helpers/request';

vi.mock('@/lib/server-cache', () => ({
  getCachedDriftExposureIndex: vi.fn(),
  getCachedDriftExposureTile: vi.fn(),
}));

const getCachedDriftExposureIndexMock = vi.mocked(getCachedDriftExposureIndex);
const getCachedDriftExposureTileMock = vi.mocked(getCachedDriftExposureTile);

describe('GET /api/drift-exposure/lookup', () => {
  beforeEach(() => {
    getCachedDriftExposureIndexMock.mockReset();
    getCachedDriftExposureTileMock.mockReset();
  });

  it('returns the nearest drift-exposure match for a coordinate', async () => {
    getCachedDriftExposureIndexMock.mockResolvedValue({
      pesticide_year: 2024,
      national_avg_drift_dose_kg: 1.23,
      building_count: 2,
      tile_zoom: 12,
      tile_count: 1,
    });
    getCachedDriftExposureTileMock.mockResolvedValueOnce([
      { uid: 'near', lat: 55.6761, lng: 12.5683, pct: 95, dose: 2.4 },
      { uid: 'far', lat: 55.68, lng: 12.58, pct: 50, dose: 0.8 },
    ]);
    getCachedDriftExposureTileMock.mockResolvedValue([]);

    const response = await GET(
      createRouteRequest(
        'https://landbruget.dk/api/drift-exposure/lookup?lat=55.6761&lng=12.5683'
      )
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toEqual({
      exposure_percentile: 95,
      drift_dose_kg: 2.4,
      national_avg_drift_dose_kg: 1.23,
      building_distance_m: 0,
    });
  });

  it('returns null when no building is within match tolerance', async () => {
    getCachedDriftExposureIndexMock.mockResolvedValue({
      pesticide_year: 2024,
      national_avg_drift_dose_kg: 1.23,
      building_count: 1,
      tile_zoom: 12,
      tile_count: 1,
    });
    getCachedDriftExposureTileMock.mockResolvedValue([
      { uid: 'far', lat: 55.68, lng: 12.58, pct: 50, dose: 0.8 },
    ]);

    const response = await GET(
      createRouteRequest(
        'https://landbruget.dk/api/drift-exposure/lookup?lat=55.6761&lng=12.5683'
      )
    );

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toBeNull();
  });

  it('rejects invalid coordinates', async () => {
    const response = await GET(
      createRouteRequest(
        'https://landbruget.dk/api/drift-exposure/lookup?lat=north&lng=12.5683'
      )
    );

    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toEqual({
      error: 'invalid coordinates',
    });
  });
});

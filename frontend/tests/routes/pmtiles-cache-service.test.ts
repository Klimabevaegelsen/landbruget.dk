import { beforeEach, describe, expect, it, vi } from 'vitest';

import { pmtilesCacheService } from '@/lib/pmtiles-cache-service';

describe('pmtiles cache service', () => {
  beforeEach(() => {
    pmtilesCacheService.clearCache();
    vi.restoreAllMocks();
  });

  it('skips the optional buildings layer when the PMTiles file is missing', async () => {
    const fetchMock = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(null, { status: 404 }));

    const urls = await pmtilesCacheService.getFieldAnalysisUrls(2024);

    expect(urls.fields).toBe(
      '/api/pmtiles/pmtiles/field_analysis_2024.pmtiles'
    );
    expect(urls.buildings).toBeNull();
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/pmtiles/pmtiles/buildings_proximity.pmtiles',
      { method: 'HEAD' }
    );
  });

  it('caches missing optional layer availability checks', async () => {
    const fetchMock = vi
      .spyOn(globalThis, 'fetch')
      .mockResolvedValue(new Response(null, { status: 404 }));

    const first = await pmtilesCacheService.getOptionalPMTilesUrl(
      'pmtiles/buildings_proximity.pmtiles'
    );
    const second = await pmtilesCacheService.getOptionalPMTilesUrl(
      'pmtiles/buildings_proximity.pmtiles'
    );

    expect(first).toBeNull();
    expect(second).toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});

import { PMTILES_BASE_URL } from '@/lib/env';

interface CachedPMTilesUrl {
  url: string;
  timestamp: number;
  etag?: string;
}

interface CachedPMTilesAvailability {
  available: boolean;
  timestamp: number;
}

interface PMTilesPreloadOptions {
  preloadAdjacentYears?: boolean;
  preloadCommonLayers?: boolean;
}

class PMTilesCacheService {
  private cache = new Map<string, CachedPMTilesUrl>();
  private availabilityCache = new Map<string, CachedPMTilesAvailability>();
  private readonly CACHE_DURATION = 7 * 24 * 60 * 60 * 1000; // 1 week in milliseconds
  private readonly USE_PROXY = true; // Use proxy to avoid CORS issues
  private readonly PMTILES_BASE_URL = PMTILES_BASE_URL.replace(/\/$/, '');

  /**
   * Get a PMTiles URL with caching optimization
   */
  async getPMTilesUrl(filename: string): Promise<string> {
    const cacheKey = filename;
    const cached = this.cache.get(cacheKey);

    // Return cached URL if still valid
    if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
      return cached.url;
    }

    // Determine URL based on proxy setting
    const url = this.USE_PROXY
      ? `/api/pmtiles/${filename}` // Same-origin caching proxy
      : `${this.PMTILES_BASE_URL}/pmtiles/${filename}`; // Direct R2 URL

    // Cache the URL
    this.cache.set(cacheKey, {
      url,
      timestamp: Date.now(),
    });

    return url;
  }

  /**
   * Get all PMTiles URLs for a specific year
   */
  async getFieldAnalysisUrls(year: number): Promise<{
    fields: string;
    overview: string;
    bnbo: string;
    wetlands: string;
    water_projects: string;
    buildings: string | null;
  }> {
    const [fields, overview, bnbo, wetlands, water_projects, buildings] =
      await Promise.all([
        this.getPMTilesUrl(`pmtiles/field_analysis_${year}.pmtiles`),
        this.getPMTilesUrl(`pmtiles/field_analysis_overview_${year}.pmtiles`),
        this.getPMTilesUrl('pmtiles/bnbo_areas.pmtiles'),
        this.getPMTilesUrl('pmtiles/wetlands_all.pmtiles'),
        this.getPMTilesUrl('pmtiles/water_projects.pmtiles'),
        this.getOptionalPMTilesUrl('pmtiles/buildings_proximity.pmtiles'),
      ]);

    return { fields, overview, bnbo, wetlands, water_projects, buildings };
  }

  /**
   * Get a PMTiles URL for an optional layer and return null when the file is
   * missing so map consumers can skip the source entirely.
   */
  async getOptionalPMTilesUrl(filename: string): Promise<string | null> {
    const isAvailable = await this.checkPMTilesAvailability(filename);
    if (!isAvailable) {
      return null;
    }

    return this.getPMTilesUrl(filename);
  }

  /**
   * Preload PMTiles directory headers for better performance.
   * Creates PMTiles instances and registers them with Protocol via
   * protocol.add(), so the cached headers are reused when MapLibre
   * later requests tiles. Falls back to standalone instances if
   * Protocol isn't registered yet.
   */
  async preloadPMTiles(filenames: string[]): Promise<void> {
    if (typeof window === 'undefined') {
      return; // Skip on server-side
    }

    const [{ PMTiles }, { getPmtilesProtocol, getSharedPmtilesCache }] =
      await Promise.all([
        import('pmtiles'),
        import('@/components/pesticidkort/pmtiles-protocol'),
      ]);

    const protocol = getPmtilesProtocol();
    const cache = getSharedPmtilesCache();

    const preloadPromises = filenames.map(async (filename) => {
      try {
        const url = await this.getPMTilesUrl(filename);
        const p = cache ? new PMTiles(url, cache) : new PMTiles(url);
        if (protocol) {
          protocol.add(p);
        }
        await p.getHeader();
      } catch {
        // Preload failures are non-critical
      }
    });

    await Promise.allSettled(preloadPromises);
  }

  /**
   * Preload adjacent years for smooth year switching
   */
  async preloadAdjacentYears(
    currentYear: number,
    availableYears: number[]
  ): Promise<void> {
    const adjacentYears = [currentYear - 1, currentYear + 1].filter((year) =>
      availableYears.includes(year)
    );

    if (adjacentYears.length === 0) {
      return;
    }

    const filenames = adjacentYears.map(
      (year) => `field_analysis_${year}.pmtiles`
    );

    await this.preloadPMTiles(filenames);
  }

  /**
   * Preload commonly used background layers
   */
  async preloadCommonLayers(): Promise<void> {
    const commonLayers = ['bnbo_areas.pmtiles', 'buildings_proximity.pmtiles'];

    await this.preloadPMTiles(commonLayers);
  }

  /**
   * Check if a PMTiles file is available
   */
  async checkPMTilesAvailability(filename: string): Promise<boolean> {
    const cached = this.availabilityCache.get(filename);

    if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
      return cached.available;
    }

    try {
      const url = await this.getPMTilesUrl(filename);
      const response = await fetch(url, { method: 'HEAD' });
      const available = response.ok;

      this.availabilityCache.set(filename, {
        available,
        timestamp: Date.now(),
      });

      return available;
    } catch {
      this.availabilityCache.set(filename, {
        available: false,
        timestamp: Date.now(),
      });
      return false;
    }
  }

  /**
   * Get cache statistics
   */
  getCacheStats(): {
    size: number;
    entries: string[];
    oldestEntry: string | null;
    newestEntry: string | null;
  } {
    const entries = Array.from(this.cache.keys());
    const timestamps = Array.from(this.cache.values()).map((v) => v.timestamp);

    return {
      size: this.cache.size,
      entries,
      oldestEntry:
        entries.length > 0
          ? entries[timestamps.indexOf(Math.min(...timestamps))]
          : null,
      newestEntry:
        entries.length > 0
          ? entries[timestamps.indexOf(Math.max(...timestamps))]
          : null,
    };
  }

  /**
   * Clear the cache
   */
  clearCache(): void {
    this.cache.clear();
    this.availabilityCache.clear();
  }

  /**
   * Clear expired entries from cache
   */
  cleanExpiredEntries(): number {
    const now = Date.now();
    let removedCount = 0;

    for (const [key, value] of this.cache.entries()) {
      if (now - value.timestamp > this.CACHE_DURATION) {
        this.cache.delete(key);
        removedCount++;
      }
    }

    for (const [key, value] of this.availabilityCache.entries()) {
      if (now - value.timestamp > this.CACHE_DURATION) {
        this.availabilityCache.delete(key);
      }
    }

    return removedCount;
  }

  /**
   * Enable or disable proxy usage
   */
  setProxyEnabled(enabled: boolean): void {
    if (enabled !== this.USE_PROXY) {
      this.clearCache();
    }
    // Note: USE_PROXY is readonly, but we could make it configurable if needed
  }
}

// Export singleton instance
export const pmtilesCacheService = new PMTilesCacheService();

// Make pmtilesCacheService available globally for debugging
if (typeof window !== 'undefined') {
  (
    window as typeof window & { pmtilesCacheService: PMTilesCacheService }
  ).pmtilesCacheService = pmtilesCacheService;
}

// Clean expired entries periodically (every 30 minutes)
if (typeof window !== 'undefined') {
  setInterval(
    () => {
      pmtilesCacheService.cleanExpiredEntries();
    },
    30 * 60 * 1000
  );
}

// Export types for use in components
export type { PMTilesPreloadOptions };

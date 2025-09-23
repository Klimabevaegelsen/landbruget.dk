interface CachedPMTilesUrl {
  url: string;
  timestamp: number;
  etag?: string;
}

interface PMTilesPreloadOptions {
  preloadAdjacentYears?: boolean;
  preloadCommonLayers?: boolean;
}

class PMTilesCacheService {
  private cache = new Map<string, CachedPMTilesUrl>();
  private readonly CACHE_DURATION = 7 * 24 * 60 * 60 * 1000; // 1 week in milliseconds
  private readonly USE_PROXY = true; // Use proxy to avoid CORS issues
  private readonly PROXY_BASE_URL = 'https://www.landbruget.dk'; // Enforce www for consistency

  /**
   * Get a PMTiles URL with caching optimization
   */
  async getPMTilesUrl(filename: string): Promise<string> {
    const cacheKey = filename;
    const cached = this.cache.get(cacheKey);

    // Return cached URL if still valid
    if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
      console.log(`🎯 PMTiles cache hit: ${filename}`);
      return cached.url;
    }

    // Determine URL based on proxy setting
    const url = this.USE_PROXY
      ? `${this.PROXY_BASE_URL}/api/pmtiles/${filename}?v=${Date.now()}` // Use our caching proxy
      : `https://data.pesticidkortet.dk/pmtiles/${filename}`; // Direct R2 URL

    // Cache the URL
    this.cache.set(cacheKey, {
      url,
      timestamp: Date.now(),
    });

    console.log(
      `🗺️ PMTiles URL generated: ${filename} -> ${this.USE_PROXY ? 'PROXY' : 'DIRECT'}`
    );
    return url;
  }

  /**
   * Get all PMTiles URLs for a specific year
   */
  async getFieldAnalysisUrls(year: number): Promise<{
    fields: string;
    bnbo: string;
    wetlands: string;
    water_projects: string;
    buildings: string;
  }> {
    const [fields, bnbo, wetlands, water_projects, buildings] =
      await Promise.all([
        this.getPMTilesUrl(`pmtiles/field_analysis_${year}.pmtiles`),
        this.getPMTilesUrl('pmtiles/bnbo_areas.pmtiles'), // BNBO areas from environmental generator
        this.getPMTilesUrl('pmtiles/wetlands_all.pmtiles'), // Match actual R2 upload names
        this.getPMTilesUrl('pmtiles/water_projects.pmtiles'), // Match actual R2 upload names
        this.getPMTilesUrl('pmtiles/buildings_proximity.pmtiles'), // Match actual R2 upload names
      ]);

    return { fields, bnbo, wetlands, water_projects, buildings };
  }

  /**
   * Preload PMTiles files for better performance
   */
  async preloadPMTiles(filenames: string[]): Promise<void> {
    if (typeof window === 'undefined') {
      console.log('⚠️ PMTiles preload skipped (server-side)');
      return; // Skip on server-side
    }

    console.log(`🚀 Preloading ${filenames.length} PMTiles files...`);

    const preloadPromises = filenames.map(async (filename) => {
      try {
        const url = await this.getPMTilesUrl(filename);

        // Use link preload for better browser caching
        const link = document.createElement('link');
        link.rel = 'preload';
        link.href = url;
        // PMTiles are binary data, don't specify 'as' attribute
        link.crossOrigin = 'anonymous';

        // Add to document head
        document.head.appendChild(link);

        // Optional: Remove link after a delay to clean up DOM
        setTimeout(() => {
          if (link.parentNode) {
            link.parentNode.removeChild(link);
          }
        }, 30000); // Remove after 30 seconds

        console.log(`✅ Preloaded: ${filename}`);
      } catch (error) {
        console.warn(`⚠️ Failed to preload: ${filename}`, error);
      }
    });

    await Promise.allSettled(preloadPromises);
    console.log(`🎯 PMTiles preloading completed`);
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
      console.log('🔄 No adjacent years to preload');
      return;
    }

    const filenames = adjacentYears.map(
      (year) => `field_analysis_${year}.pmtiles`
    );

    console.log(`📅 Preloading adjacent years: ${adjacentYears.join(', ')}`);
    await this.preloadPMTiles(filenames);
  }

  /**
   * Preload commonly used background layers
   */
  async preloadCommonLayers(): Promise<void> {
    const commonLayers = ['bnbo_areas.pmtiles', 'buildings_proximity.pmtiles'];

    console.log('🏗️ Preloading common background layers');
    await this.preloadPMTiles(commonLayers);
  }

  /**
   * Check if a PMTiles file is available
   */
  async checkPMTilesAvailability(filename: string): Promise<boolean> {
    try {
      const url = await this.getPMTilesUrl(filename);
      const response = await fetch(url, { method: 'HEAD' });
      return response.ok;
    } catch (error) {
      console.warn(`❌ PMTiles availability check failed: ${filename}`, error);
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
    console.log('🧹 PMTiles cache cleared');
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

    if (removedCount > 0) {
      console.log(`🧹 Cleaned ${removedCount} expired PMTiles cache entries`);
    }

    return removedCount;
  }

  /**
   * Enable or disable proxy usage
   */
  setProxyEnabled(enabled: boolean): void {
    if (enabled !== this.USE_PROXY) {
      console.log(
        `🔄 PMTiles proxy ${enabled ? 'enabled' : 'disabled'} - clearing cache`
      );
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

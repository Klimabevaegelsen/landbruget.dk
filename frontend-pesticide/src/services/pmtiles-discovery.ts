// PMTiles Discovery Service - Browser Compatible Version
// This service handles discovery of PMTiles files from R2 storage

export type YearSelection = number | 'total';

interface DataAvailability {
  years: number[];
  resolutions: number[];
  latestYear: number;
  latestResolution: number;
}

interface PMTilesUrls {
  basemap: string;
  h3: Record<string, string>; // year_resolution -> url
  kommune: Record<string, string>; // year -> url
  bnbo: string;
}

interface PMTilesManifest {
  years: number[];
  resolutions: number[];
  latestYear: number;
  latestResolution: number;
  h3: Record<string, string>;
  kommune: Record<string, string>;
  basemap?: string;
  bnbo?: string;
}

class PMTilesDiscoveryService {
  private cache: Map<string, unknown> = new Map();
  private readonly baseUrl = 'https://api.landbruget.dk';
  private readonly manifestUrl = `${this.baseUrl}/pmtiles/pmtiles-manifest.json`;

  private async getManifest(): Promise<PMTilesManifest> {
    const cacheKey = 'pmtiles_manifest';
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) as PMTilesManifest;
    }

    const response = await fetch(this.manifestUrl);
    if (!response.ok) {
      throw new Error(`Manifest fetch failed: HTTP ${response.status}`);
    }

    const manifest = (await response.json()) as PMTilesManifest;
    this.cache.set(cacheKey, manifest);
    return manifest;
  }

  // Discover available data by checking R2 bucket structure
  async getDataAvailability(): Promise<DataAvailability> {
    const cacheKey = 'data_availability';

    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) as DataAvailability;
    }

    try {
      const manifest = await this.getManifest();
      const availability: DataAvailability = {
        years: manifest.years,
        resolutions: manifest.resolutions,
        latestYear: manifest.latestYear,
        latestResolution: manifest.latestResolution,
      };

      this.cache.set(cacheKey, availability);
      return availability;
    } catch (error) {
      console.warn(
        'Failed to discover data availability, using fallback:',
        error
      );
      // Fallback to known structure
      const fallback: DataAvailability = {
        years: [2023],
        resolutions: [8, 10], // Only res8 and res10 for H3
        latestYear: 2023,
        latestResolution: 10,
      };
      return fallback;
    }
  }

  async discoverBasemapTiles(): Promise<string> {
    try {
      const manifest = await this.getManifest();
      return (
        manifest.basemap || `${this.baseUrl}/pmtiles/protomaps_denmark.pmtiles`
      );
    } catch {
      return `${this.baseUrl}/pmtiles/protomaps_denmark.pmtiles`;
    }
  }

  async discoverLatestBNBOTiles(): Promise<string> {
    try {
      const manifest = await this.getManifest();
      return manifest.bnbo || `${this.baseUrl}/pmtiles/bnbo_areas.pmtiles`;
    } catch {
      return `${this.baseUrl}/pmtiles/bnbo_areas.pmtiles`;
    }
  }

  async discoverLatestH3Tiles(
    year: YearSelection,
    resolution: number
  ): Promise<string> {
    const cacheKey = `h3_${year}_${resolution}`;

    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) as string;
    }

    try {
      const manifest = await this.getManifest();
      const key = `${year}_${resolution}`;
      const url = manifest.h3[key];
      if (!url) {
        throw new Error(`No manifest entry for H3 ${key}`);
      }
      this.cache.set(cacheKey, url);
      return url;
    } catch (error) {
      console.error(
        `Failed to discover H3 tiles for ${year} res${resolution}:`,
        error
      );
      throw error;
    }
  }

  async discoverLatestKommuneTiles(year: YearSelection): Promise<string> {
    const cacheKey = `kommune_${year}`;

    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) as string;
    }

    try {
      const manifest = await this.getManifest();
      const key = `${year}`;
      const url = manifest.kommune[key];
      if (!url) {
        throw new Error(`No manifest entry for kommune ${key}`);
      }
      this.cache.set(cacheKey, url);
      return url;
    } catch (error) {
      console.error(`Failed to discover kommune tiles for ${year}:`, error);
      throw error;
    }
  }

  async getAvailableYears(): Promise<number[]> {
    const availability = await this.getDataAvailability();
    return availability.years;
  }

  async getAvailableResolutions(): Promise<number[]> {
    const availability = await this.getDataAvailability();
    return availability.resolutions;
  }

  // Helper method to get PMTiles URL for a specific type
  async getPMTilesUrl(type: 'basemap' | 'bnbo'): Promise<string> {
    switch (type) {
      case 'basemap':
        return this.discoverBasemapTiles();
      case 'bnbo':
        return this.discoverLatestBNBOTiles();
      default:
        throw new Error(`Unknown PMTiles type: ${type}`);
    }
  }

  // Get all URLs for a specific year
  async getYearUrls(year: YearSelection): Promise<PMTilesUrls> {
    const [basemap, bnbo] = await Promise.all([
      this.discoverBasemapTiles(),
      this.discoverLatestBNBOTiles(),
    ]);

    // Get H3 URLs for all resolutions
    const h3Urls: Record<string, string> = {};
    const resolutions = await this.getAvailableResolutions();

    for (const resolution of resolutions) {
      const key = `${year}_${resolution}`;
      try {
        h3Urls[key] = await this.discoverLatestH3Tiles(year, resolution);
      } catch (error) {
        console.warn(
          `Failed to get H3 URL for ${year} res${resolution}:`,
          error
        );
      }
    }

    // Get kommune URL
    const kommuneUrls: Record<string, string> = {};
    try {
      kommuneUrls[year.toString()] =
        await this.discoverLatestKommuneTiles(year);
    } catch (error) {
      console.warn(`Failed to get kommune URL for ${year}:`, error);
    }

    return {
      basemap,
      h3: h3Urls,
      kommune: kommuneUrls,
      bnbo,
    };
  }

  // Clear cache to force re-discovery
  clearCache(): void {
    this.cache.clear();
  }

  // Test if a URL is accessible
  async testUrl(url: string): Promise<boolean> {
    try {
      const response = await fetch(url, { method: 'HEAD' });
      return response.ok;
    } catch (error) {
      console.warn(`URL test failed for ${url}:`, error);
      return false;
    }
  }

  // Get URLs directly without validation
  async discoverAndValidateUrls(
    year: YearSelection,
    resolution: number
  ): Promise<{
    h3: string | null;
    kommune: string | null;
    basemap: string | null;
    bnbo: string | null;
  }> {
    try {
      const [basemapUrl, bnboUrl, h3Url, kommuneUrl] = await Promise.all([
        this.discoverBasemapTiles(),
        this.discoverLatestBNBOTiles(),
        this.discoverLatestH3Tiles(year, resolution),
        this.discoverLatestKommuneTiles(year),
      ]);

      return {
        basemap: basemapUrl,
        bnbo: bnboUrl,
        h3: h3Url,
        kommune: kommuneUrl,
      };
    } catch (error) {
      console.error('URL discovery failed:', error);
      return {
        h3: null,
        kommune: null,
        basemap: null,
        bnbo: null,
      };
    }
  }
}

// Export singleton instance
export const pmtilesDiscovery = new PMTilesDiscoveryService();

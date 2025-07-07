// PMTiles Discovery Service - Browser Compatible Version
// This service handles discovery of PMTiles files from GCS bucket

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

class PMTilesDiscoveryService {
  private cache: Map<string, unknown> = new Map();
  private readonly baseUrl = 'https://storage.googleapis.com/landbrugsdata-raw-data';
  
  // Discover available data by checking GCS bucket structure
  async getDataAvailability(): Promise<DataAvailability> {
    const cacheKey = 'data_availability';
    
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }
    
    try {
      // For now, return known structure - in practice, this would query GCS API
      const availability: DataAvailability = {
        years: [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023],
        resolutions: [7, 8, 9, 10],
        latestYear: 2023,
        latestResolution: 10
      };
      
      this.cache.set(cacheKey, availability);
      return availability;
    } catch (error) {
      console.warn('Failed to discover data availability, using fallback:', error);
      // Fallback to known structure
      const fallback: DataAvailability = {
        years: [2023],
        resolutions: [7, 8, 9, 10],
        latestYear: 2023,
        latestResolution: 10
      };
      return fallback;
    }
  }
  
  async discoverBasemapTiles(): Promise<string> {
    return `${this.baseUrl}/pmtiles/protomaps_denmark.pmtiles`;
  }
  
  async discoverLatestBNBOTiles(): Promise<string> {
    return `${this.baseUrl}/pmtiles/bnbo_areas.pmtiles`;
  }

  async discoverLatestH3Tiles(year: YearSelection, resolution: number): Promise<string> {
    const cacheKey = `h3_${year}_${resolution}`;
    
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }
    
    try {
      const pattern = `gold/pmtiles/h3_pfas_${year}_res${resolution}`;
      const latestTimestamp = await this._discoverLatestTimestamp(pattern);
      
      const url = `${this.baseUrl}/${pattern}/${latestTimestamp}/h3_pfas_${year}_res${resolution}.pmtiles`;
      this.cache.set(cacheKey, url);
      return url;
    } catch (error) {
      console.error(`Failed to discover H3 tiles for ${year} res${resolution}:`, error);
      throw error;
    }
  }

  async discoverLatestKommuneTiles(year: YearSelection): Promise<string> {
    const cacheKey = `kommune_${year}`;
    
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }
    
    try {
      const pattern = `gold/pmtiles/kommune_pfas_${year}`;
      const latestTimestamp = await this._discoverLatestTimestamp(pattern);
      
      const url = `${this.baseUrl}/${pattern}/${latestTimestamp}/kommune_pfas_${year}.pmtiles`;
      this.cache.set(cacheKey, url);
      return url;
    } catch (error) {
      console.error(`Failed to discover kommune tiles for ${year}:`, error);
      throw error;
    }
  }

  private async _discoverLatestTimestamp(pattern: string): Promise<string> {
    try {
      // Try to list directory contents via GCS JSON API with timeout
      const listUrl = `https://storage.googleapis.com/storage/v1/b/landbrugsdata-raw-data/o?prefix=${pattern}/&delimiter=/`;
      
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 second timeout
      
      const response = await fetch(listUrl, { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      const data = await response.json();
      
      if (data.prefixes && data.prefixes.length > 0) {
        // Extract timestamps from prefixes like "gold/pmtiles/h3_pfas_2023_res10/20250705_181521/"
        const timestamps = data.prefixes
          .map((prefix: string) => {
            const parts = prefix.split('/');
            return parts[parts.length - 2]; // Get the timestamp part
          })
          .filter((ts: string) => /^\d{8}_\d{6}$/.test(ts)) // Validate timestamp format
          .sort()
          .reverse(); // Latest first
        
        if (timestamps.length > 0) {
          return timestamps[0];
        }
      }
      
      throw new Error('No timestamps found');
    } catch (error) {
      console.warn(`Failed to discover timestamp for ${pattern}:`, error);
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
      this.discoverLatestBNBOTiles()
    ]);
    
    // Get H3 URLs for all resolutions
    const h3Urls: Record<string, string> = {};
    const resolutions = await this.getAvailableResolutions();
    
    for (const resolution of resolutions) {
      const key = `${year}_${resolution}`;
      try {
        h3Urls[key] = await this.discoverLatestH3Tiles(year, resolution);
      } catch (error) {
        console.warn(`Failed to get H3 URL for ${year} res${resolution}:`, error);
      }
    }
    
    // Get kommune URL
    const kommuneUrls: Record<string, string> = {};
    try {
      kommuneUrls[year.toString()] = await this.discoverLatestKommuneTiles(year);
    } catch (error) {
      console.warn(`Failed to get kommune URL for ${year}:`, error);
    }
    
    return {
      basemap,
      h3: h3Urls,
      kommune: kommuneUrls,
      bnbo
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
  async discoverAndValidateUrls(year: YearSelection, resolution: number): Promise<{
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
        this.discoverLatestKommuneTiles(year)
      ]);

      return {
        basemap: basemapUrl,
        bnbo: bnboUrl,
        h3: h3Url,
        kommune: kommuneUrl
      };
    } catch (error) {
      console.error('URL discovery failed:', error);
      return {
        h3: null,
        kommune: null,
        basemap: null,
        bnbo: null
      };
    }
  }
}

// Export singleton instance
export const pmtilesDiscovery = new PMTilesDiscoveryService(); 
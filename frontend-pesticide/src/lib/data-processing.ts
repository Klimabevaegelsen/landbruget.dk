import { supabase, handleSupabaseError, buildBboxQuery } from './supabase';
import type { 
  H3DataPoint, 
  H3DataFilter, 
  H3ProcessingConfig,
  H3DataQuality 
} from '@/types/h3-data';
import type { BNBOArea, BNBODataFilter } from '@/types/bnbo-data';
import type { BBRBuilding, BBRDataFilter } from '@/types/bbr-data';
import { CACHE_SETTINGS, VISUALIZATION_LIMITS } from './shared-constants';

/**
 * DataManager class - Core data processing and management
 * Handles all database interactions, caching, and data transformations
 */
export class DataManager {
  private cache: Map<string, unknown> = new Map();
  private cacheTimestamps: Map<string, number> = new Map();

  /**
   * Fetch H3 PFAS exposure data with filtering and aggregation
   */
  async fetchH3Data(
    year: number, 
    cumulativeMode: boolean = false,
    filter?: H3DataFilter,
    config?: H3ProcessingConfig
  ): Promise<H3DataPoint[]> {
    const cacheKey = this.buildCacheKey('h3', { year, cumulativeMode, filter, config });
    
    // Check cache first
    const cachedData = this.getCachedData(cacheKey, CACHE_SETTINGS.H3_DATA_TTL);
    if (cachedData) {
      return cachedData;
    }

    try {
      let query = supabase
        .from('h3_pfas_exposure')
        .select(`
          h3_id,
          year,
          total_pesticide_load,
          total_pfas_grams,
          pesticide_application_count,
          field_count,
          agricultural_area_ha,
          avg_field_coverage,
          h3_resolution,
          ST_AsGeoJSON(geometry) as geometry,
          ST_X(h3_centroid) as centroid_lon,
          ST_Y(h3_centroid) as centroid_lat
        `);

      // Apply year filtering
      if (cumulativeMode) {
        query = query.gte('year', 2020).lte('year', year);
      } else {
        query = query.eq('year', year);
      }

      // Apply additional filters
      if (filter) {
        if (filter.minPesticideLoad !== undefined) {
          query = query.gte('total_pesticide_load', filter.minPesticideLoad);
        }
        if (filter.maxPesticideLoad !== undefined) {
          query = query.lte('total_pesticide_load', filter.maxPesticideLoad);
        }
        if (filter.minPfasGrams !== undefined) {
          query = query.gte('total_pfas_grams', filter.minPfasGrams);
        }
        if (filter.maxPfasGrams !== undefined) {
          query = query.lte('total_pfas_grams', filter.maxPfasGrams);
        }
        if (filter.h3Resolution !== undefined) {
          query = query.eq('h3_resolution', filter.h3Resolution);
        }
        if (filter.bbox) {
          // Use PostGIS spatial filtering
          const bboxQuery = buildBboxQuery(filter.bbox);
          query = query.filter('geometry', 'filter', bboxQuery);
        }
      }

      // Limit results for performance
      query = query.limit(VISUALIZATION_LIMITS.MAX_H3_HEXAGONS);

      const { data, error } = await query;

      if (error) {
        handleSupabaseError(error);
      }

      if (!data) {
        return [];
      }

      // Process and transform data
      let processedData: H3DataPoint[];
      
      if (cumulativeMode) {
        const aggregatedData = this.aggregateH3DataByYear(data);
        processedData = this.transformAggregatedH3Data(aggregatedData, config);
      } else {
        processedData = this.transformH3Data(data, config);
      }

      // Cache the results
      this.setCachedData(cacheKey, processedData);
      
      return processedData;

    } catch (error) {
      console.error('Error fetching H3 data:', error);
      throw error;
    }
  }

  /**
   * Fetch BNBO status areas data
   */
  async fetchBNBOData(filter?: BNBODataFilter): Promise<BNBOArea[]> {
    const cacheKey = this.buildCacheKey('bnbo', { filter });
    
    const cachedData = this.getCachedData(cacheKey, CACHE_SETTINGS.BNBO_DATA_TTL);
    if (cachedData) {
      return cachedData;
    }

    try {
      let query = supabase
        .from('bnbo_status_areas')
        .select(`
          id,
          bnbo_id,
          status_code,
          status_description,
          area_ha,
          year,
          created_at,
          ST_AsGeoJSON(geometry) as geometry
        `);

      // Apply filters
      if (filter) {
        if (filter.statusCodes && filter.statusCodes.length > 0) {
          query = query.in('status_code', filter.statusCodes);
        }
        if (filter.minAreaHa !== undefined) {
          query = query.gte('area_ha', filter.minAreaHa);
        }
        if (filter.maxAreaHa !== undefined) {
          query = query.lte('area_ha', filter.maxAreaHa);
        }
        if (filter.year !== undefined) {
          query = query.eq('year', filter.year);
        }
        if (filter.bbox) {
          const bboxQuery = buildBboxQuery(filter.bbox);
          query = query.filter('geometry', 'filter', bboxQuery);
        }
      }

      query = query.limit(VISUALIZATION_LIMITS.MAX_BNBO_POLYGONS);

      const { data, error } = await query;

      if (error) {
        handleSupabaseError(error);
      }

      if (!data) {
        return [];
      }

      const processedData = this.transformBNBOData(data);
      this.setCachedData(cacheKey, processedData);
      
      return processedData;

    } catch (error) {
      console.error('Error fetching BNBO data:', error);
      throw error;
    }
  }

  /**
   * Fetch BBR buildings data
   */
  async fetchBBRData(filter?: BBRDataFilter): Promise<BBRBuilding[]> {
    const cacheKey = this.buildCacheKey('bbr', { filter });
    
    const cachedData = this.getCachedData(cacheKey, CACHE_SETTINGS.BBR_DATA_TTL);
    if (cachedData) {
      return cachedData;
    }

    try {
      let query = supabase
        .from('bbr_buildings')
        .select(`
          id,
          bbr_id,
          building_code,
          building_type,
          construction_year,
          floor_area,
          address,
          created_at,
          ST_AsGeoJSON(geometry) as geometry
        `);

      // Apply filters
      if (filter) {
        if (filter.buildingTypes && filter.buildingTypes.length > 0) {
          query = query.in('building_type', filter.buildingTypes);
        }
        if (filter.minConstructionYear !== undefined) {
          query = query.gte('construction_year', filter.minConstructionYear);
        }
        if (filter.maxConstructionYear !== undefined) {
          query = query.lte('construction_year', filter.maxConstructionYear);
        }
        if (filter.minFloorArea !== undefined) {
          query = query.gte('floor_area', filter.minFloorArea);
        }
        if (filter.maxFloorArea !== undefined) {
          query = query.lte('floor_area', filter.maxFloorArea);
        }
        if (filter.bbox) {
          const bboxQuery = buildBboxQuery(filter.bbox);
          query = query.filter('geometry', 'filter', bboxQuery);
        }
        if (filter.proximityFilter) {
          // Use PostGIS distance query
          const { centerLon, centerLat, radiusKm } = filter.proximityFilter;
          const distanceQuery = `ST_DWithin(geometry, ST_Point(${centerLon}, ${centerLat})::geography, ${radiusKm * 1000})`;
          query = query.filter('geometry', 'filter', distanceQuery);
        }
      }

      query = query.limit(VISUALIZATION_LIMITS.MAX_BBR_POINTS);

      const { data, error } = await query;

      if (error) {
        handleSupabaseError(error);
      }

      if (!data) {
        return [];
      }

      const processedData = this.transformBBRData(data);
      this.setCachedData(cacheKey, processedData);
      
      return processedData;

    } catch (error) {
      console.error('Error fetching BBR data:', error);
      throw error;
    }
  }

  /**
   * Get data quality metrics for H3 data
   */
  async getH3DataQuality(year?: number): Promise<H3DataQuality> {
    const cacheKey = this.buildCacheKey('h3_quality', { year });
    
    const cachedData = this.getCachedData(cacheKey, 3600); // Cache for 1 hour
    if (cachedData) {
      return cachedData;
    }

    try {
      let query = supabase.from('h3_pfas_exposure');
      
      if (year) {
        query = query.eq('year', year);
      }

      const { data, error } = await query.select(`
        year,
        total_pesticide_load,
        total_pfas_grams,
        geometry,
        ST_X(h3_centroid) as centroid_lon,
        ST_Y(h3_centroid) as centroid_lat
      `);

      if (error) {
        handleSupabaseError(error);
      }

      if (!data || data.length === 0) {
        throw new Error('No data available for quality analysis');
      }

      const quality: H3DataQuality = {
        totalRecords: data.length,
        recordsWithGeometry: data.filter(row => row.geometry).length,
        recordsWithPesticideData: data.filter(row => row.total_pesticide_load !== null && row.total_pesticide_load > 0).length,
        recordsWithPfasData: data.filter(row => row.total_pfas_grams !== null && row.total_pfas_grams > 0).length,
        yearRange: {
          min: Math.min(...data.map(row => row.year)),
          max: Math.max(...data.map(row => row.year))
        },
        spatialExtent: {
          minLon: Math.min(...data.map(row => row.centroid_lon)),
          maxLon: Math.max(...data.map(row => row.centroid_lon)),
          minLat: Math.min(...data.map(row => row.centroid_lat)),
          maxLat: Math.max(...data.map(row => row.centroid_lat))
        },
        dataCompleteness: (data.filter(row => 
          row.total_pesticide_load !== null && 
          row.total_pfas_grams !== null && 
          row.geometry
        ).length / data.length) * 100,
        lastUpdated: new Date().toISOString()
      };

      this.setCachedData(cacheKey, quality);
      return quality;

    } catch (error) {
      console.error('Error getting data quality metrics:', error);
      throw error;
    }
  }

  /**
   * Transform raw H3 data from database to frontend format
   */
  private transformH3Data(rawData: Record<string, unknown>[], config?: H3ProcessingConfig): H3DataPoint[] {
    return rawData.map(row => {
      const geometry = typeof row.geometry === 'string' 
        ? JSON.parse(row.geometry) 
        : row.geometry;

      const dataPoint: H3DataPoint = {
        h3_id: row.h3_id,
        year: row.year,
        total_pesticide_load: row.total_pesticide_load || 0,
        total_pfas_grams: row.total_pfas_grams || 0,
        pesticide_application_count: row.pesticide_application_count || 0,
        field_count: row.field_count || 0,
        agricultural_area_ha: row.agricultural_area_ha || 0,
        avg_field_coverage: row.avg_field_coverage || 0,
        geometry: geometry,
        centroid_lon: row.centroid_lon,
        centroid_lat: row.centroid_lat,
        h3_resolution: row.h3_resolution || 10
      };

      // Add calculated intensity fields
      if (config?.includeIntensityCalculations !== false) {
        dataPoint.pfas_intensity = dataPoint.agricultural_area_ha > 0 
          ? dataPoint.total_pfas_grams / dataPoint.agricultural_area_ha 
          : 0;
        dataPoint.pesticide_intensity = dataPoint.agricultural_area_ha > 0 
          ? dataPoint.total_pesticide_load / dataPoint.agricultural_area_ha 
          : 0;
      }

      return dataPoint;
    });
  }

  /**
   * Aggregate H3 data by hexagon for cumulative mode
   */
  private aggregateH3DataByYear(data: Record<string, unknown>[]): Record<string, unknown>[] {
    const aggregated = new Map();

    data.forEach(row => {
      const key = row.h3_id;

      if (!aggregated.has(key)) {
        aggregated.set(key, {
          ...row,
          years: [],
          total_pesticide_load: 0,
          total_pfas_grams: 0,
          pesticide_application_count: 0,
          max_field_count: 0,
          total_agricultural_area_ha: 0,
          total_field_coverage: 0,
          year_count: 0
        });
      }

      const existing = aggregated.get(key);
      existing.years.push(row.year);
      existing.total_pesticide_load += row.total_pesticide_load || 0;
      existing.total_pfas_grams += row.total_pfas_grams || 0;
      existing.pesticide_application_count += row.pesticide_application_count || 0;
      existing.max_field_count = Math.max(existing.max_field_count, row.field_count || 0);
      existing.total_agricultural_area_ha += row.agricultural_area_ha || 0;
      existing.total_field_coverage += row.avg_field_coverage || 0;
      existing.year_count += 1;
    });

    return Array.from(aggregated.values()).map(item => ({
      ...item,
      avg_agricultural_area_ha: item.total_agricultural_area_ha / item.year_count,
      avg_field_coverage: item.total_field_coverage / item.year_count
    }));
  }

  /**
   * Transform aggregated H3 data to frontend format
   */
  private transformAggregatedH3Data(aggregatedData: Record<string, unknown>[]): H3DataPoint[] {
    return aggregatedData.map(row => {
      const geometry = typeof row.geometry === 'string' 
        ? JSON.parse(row.geometry) 
        : row.geometry;

      return {
        h3_id: row.h3_id,
        year: Math.max(...row.years), // Use latest year for display
        total_pesticide_load: row.total_pesticide_load,
        total_pfas_grams: row.total_pfas_grams,
        pesticide_application_count: row.pesticide_application_count,
        field_count: row.max_field_count,
        agricultural_area_ha: row.avg_agricultural_area_ha,
        avg_field_coverage: row.avg_field_coverage,
        geometry: geometry,
        centroid_lon: row.centroid_lon,
        centroid_lat: row.centroid_lat,
        h3_resolution: row.h3_resolution || 10,
        pfas_intensity: row.avg_agricultural_area_ha > 0 
          ? row.total_pfas_grams / row.avg_agricultural_area_ha 
          : 0,
        pesticide_intensity: row.avg_agricultural_area_ha > 0 
          ? row.total_pesticide_load / row.avg_agricultural_area_ha 
          : 0
      };
    });
  }

  /**
   * Transform BNBO data from database to frontend format
   */
  private transformBNBOData(rawData: Record<string, unknown>[]): BNBOArea[] {
    return rawData.map(row => ({
      id: row.id,
      bnbo_id: row.bnbo_id,
      status_code: row.status_code,
      status_description: row.status_description,
      area_ha: row.area_ha || 0,
      geometry: typeof row.geometry === 'string' 
        ? JSON.parse(row.geometry) 
        : row.geometry,
      year: row.year || new Date().getFullYear(),
      created_at: row.created_at
    }));
  }

  /**
   * Transform BBR data from database to frontend format
   */
  private transformBBRData(rawData: Record<string, unknown>[]): BBRBuilding[] {
    return rawData.map(row => ({
      id: row.id,
      bbr_id: row.bbr_id,
      building_code: row.building_code,
      building_type: row.building_type || 'Other',
      construction_year: row.construction_year,
      floor_area: row.floor_area,
      geometry: typeof row.geometry === 'string' 
        ? JSON.parse(row.geometry) 
        : row.geometry,
      address: row.address,
      created_at: row.created_at
    }));
  }

  /**
   * Cache management utilities
   */
  private buildCacheKey(prefix: string, params: Record<string, unknown>): string {
    return `${prefix}_${JSON.stringify(params)}`;
  }

  private getCachedData(key: string, ttlSeconds: number): unknown | null {
    if (!this.cache.has(key)) {
      return null;
    }

    const timestamp = this.cacheTimestamps.get(key);
    if (!timestamp || (Date.now() - timestamp) > (ttlSeconds * 1000)) {
      this.cache.delete(key);
      this.cacheTimestamps.delete(key);
      return null;
    }

    return this.cache.get(key);
  }

  private setCachedData(key: string, data: unknown): void {
    // Implement LRU cache behavior
    if (this.cache.size >= CACHE_SETTINGS.MAX_CACHE_SIZE) {
      const firstKey = this.cache.keys().next().value;
      this.cache.delete(firstKey);
      this.cacheTimestamps.delete(firstKey);
    }

    this.cache.set(key, data);
    this.cacheTimestamps.set(key, Date.now());
  }

  /**
   * Clear all cached data
   */
  clearCache(): void {
    this.cache.clear();
    this.cacheTimestamps.clear();
  }
} 
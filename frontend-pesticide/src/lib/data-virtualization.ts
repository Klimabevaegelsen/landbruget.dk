import type { H3DataPoint } from '@/types/h3-data';
import type { BNBOArea } from '@/types/bnbo-data';
import type { BBRBuilding } from '@/types/bbr-data';
import { VISUALIZATION_LIMITS } from './shared-constants';

// Viewport state interface
export interface ViewState {
  latitude: number;
  longitude: number;
  zoom: number;
  bearing?: number;
  pitch?: number;
}

// Bounding box interface
export interface BoundingBox {
  minLon: number;
  maxLon: number;
  minLat: number;
  maxLat: number;
}

// Layer configuration based on zoom level
export interface LayerConfig {
  showH3: boolean;
  showBNBO: boolean;
  showBBR: boolean;
  h3Aggregation: 'sum' | 'average' | 'individual';
  maxH3Points: number;
  maxBNBOPolygons: number;
  maxBBRPoints: number;
}

/**
 * DataVirtualizer class - Handles performance optimization through data virtualization
 * Filters data based on viewport, zoom level, and performance constraints
 */
export class DataVirtualizer {
  private viewportBounds: BoundingBox | null = null;
  private zoomLevel: number = 7;
  private performanceMode: 'high' | 'medium' | 'low' = 'medium';

  /**
   * Update viewport and recalculate bounds
   */
  updateViewport(viewport: ViewState): void {
    this.zoomLevel = viewport.zoom;
    this.viewportBounds = this.calculateViewportBounds(viewport);
  }

  /**
   * Set performance mode based on device capabilities
   */
  setPerformanceMode(mode: 'high' | 'medium' | 'low'): void {
    this.performanceMode = mode;
  }

  /**
   * Filter H3 data based on viewport and performance constraints
   */
  filterH3Data(data: H3DataPoint[], viewport: ViewState): H3DataPoint[] {
    if (!data || data.length === 0) return [];

    // Update viewport bounds
    this.updateViewport(viewport);
    
    if (!this.viewportBounds) return data;

    // Filter by viewport with buffer
    const buffer = this.calculateBuffer(viewport.zoom);
    const bufferedBounds = this.expandBounds(this.viewportBounds, buffer);
    
    let filteredData = data.filter(item => 
      this.isPointInBounds(item.centroid_lon, item.centroid_lat, bufferedBounds)
    );

    // Apply performance-based filtering
    const maxPoints = this.getMaxPointsForZoom(viewport.zoom, 'h3');
    if (filteredData.length > maxPoints) {
      filteredData = this.prioritizeH3Data(filteredData, maxPoints);
    }

    return filteredData;
  }

  /**
   * Filter BNBO data based on viewport and performance constraints
   */
  filterBNBOData(data: BNBOArea[], viewport: ViewState): BNBOArea[] {
    if (!data || data.length === 0) return [];

    this.updateViewport(viewport);
    
    if (!this.viewportBounds) return data;

    // Filter by viewport intersection
    const buffer = this.calculateBuffer(viewport.zoom);
    const bufferedBounds = this.expandBounds(this.viewportBounds, buffer);
    
    let filteredData = data.filter(area => 
      this.isGeometryInBounds(area.geometry, bufferedBounds)
    );

    // Apply performance-based filtering
    const maxPolygons = this.getMaxPointsForZoom(viewport.zoom, 'bnbo');
    if (filteredData.length > maxPolygons) {
      filteredData = this.prioritizeBNBOData(filteredData, maxPolygons);
    }

    return filteredData;
  }

  /**
   * Filter BBR data based on viewport and performance constraints
   */
  filterBBRData(data: BBRBuilding[], viewport: ViewState): BBRBuilding[] {
    if (!data || data.length === 0) return [];

    this.updateViewport(viewport);
    
    if (!this.viewportBounds) return data;

    // Filter by viewport with buffer
    const buffer = this.calculateBuffer(viewport.zoom);
    const bufferedBounds = this.expandBounds(this.viewportBounds, buffer);
    
    let filteredData = data.filter(building => {
      const coords = building.geometry.coordinates;
      return this.isPointInBounds(coords[0], coords[1], bufferedBounds);
    });

    // Apply performance-based filtering
    const maxPoints = this.getMaxPointsForZoom(viewport.zoom, 'bbr');
    if (filteredData.length > maxPoints) {
      filteredData = this.prioritizeBBRData(filteredData, maxPoints);
    }

    return filteredData;
  }

  /**
   * Get layer configuration based on zoom level
   */
  getLayerConfigForZoom(zoom: number): LayerConfig {
    const performanceMultiplier = this.getPerformanceMultiplier();

    if (zoom < 8) {
      // Country/region level - show aggregated data
      return {
        showH3: true,
        showBNBO: false,
        showBBR: false,
        h3Aggregation: 'sum',
        maxH3Points: Math.floor(2000 * performanceMultiplier),
        maxBNBOPolygons: 0,
        maxBBRPoints: 0
      };
    } else if (zoom < 12) {
      // County level - show H3 + BNBO
      return {
        showH3: true,
        showBNBO: true,
        showBBR: false,
        h3Aggregation: 'average',
        maxH3Points: Math.floor(5000 * performanceMultiplier),
        maxBNBOPolygons: Math.floor(1000 * performanceMultiplier),
        maxBBRPoints: 0
      };
    } else {
      // Local level - show all layers
      return {
        showH3: true,
        showBNBO: true,
        showBBR: true,
        h3Aggregation: 'individual',
        maxH3Points: Math.floor(VISUALIZATION_LIMITS.MAX_H3_HEXAGONS * performanceMultiplier),
        maxBNBOPolygons: Math.floor(VISUALIZATION_LIMITS.MAX_BNBO_POLYGONS * performanceMultiplier),
        maxBBRPoints: Math.floor(VISUALIZATION_LIMITS.MAX_BBR_POINTS * performanceMultiplier)
      };
    }
  }

  /**
   * Calculate viewport bounds from view state
   */
  private calculateViewportBounds(viewport: ViewState): BoundingBox {
    // Approximate bounds calculation based on zoom level
    const latDelta = 180 / Math.pow(2, viewport.zoom);
    const lonDelta = 360 / Math.pow(2, viewport.zoom);

    return {
      minLon: viewport.longitude - lonDelta,
      maxLon: viewport.longitude + lonDelta,
      minLat: viewport.latitude - latDelta,
      maxLat: viewport.latitude + latDelta
    };
  }

  /**
   * Calculate buffer size based on zoom level
   */
  private calculateBuffer(zoom: number): number {
    // Larger buffer at lower zoom levels for smoother panning
    return Math.max(0.1, 2 / Math.pow(2, zoom - 5));
  }

  /**
   * Expand bounds by buffer amount
   */
  private expandBounds(bounds: BoundingBox, buffer: number): BoundingBox {
    return {
      minLon: bounds.minLon - buffer,
      maxLon: bounds.maxLon + buffer,
      minLat: bounds.minLat - buffer,
      maxLat: bounds.maxLat + buffer
    };
  }

  /**
   * Check if point is within bounds
   */
  private isPointInBounds(lon: number, lat: number, bounds: BoundingBox): boolean {
    return lon >= bounds.minLon && 
           lon <= bounds.maxLon && 
           lat >= bounds.minLat && 
           lat <= bounds.maxLat;
  }

  /**
   * Check if geometry intersects with bounds
   */
  private isGeometryInBounds(geometry: GeoJSON.Geometry, bounds: BoundingBox): boolean {
    // Simplified bounds checking - in production would use proper geometry intersection
    if (geometry.type === 'Point') {
      const coords = geometry.coordinates as [number, number];
      return this.isPointInBounds(coords[0], coords[1], bounds);
    }
    
    if (geometry.type === 'Polygon') {
      const coords = geometry.coordinates[0] as [number, number][];
      // Check if any point of the polygon is within bounds
      return coords.some(coord => this.isPointInBounds(coord[0], coord[1], bounds));
    }
    
    if (geometry.type === 'MultiPolygon') {
      const coords = geometry.coordinates as [number, number][][][];
      // Check if any polygon intersects with bounds
      return coords.some(polygon => 
        polygon[0].some(coord => this.isPointInBounds(coord[0], coord[1], bounds))
      );
    }
    
    return true; // Default to include if we can't determine
  }

  /**
   * Get maximum points allowed for zoom level and layer type
   */
  private getMaxPointsForZoom(zoom: number, layerType: 'h3' | 'bnbo' | 'bbr'): number {
    const config = this.getLayerConfigForZoom(zoom);
    
    switch (layerType) {
      case 'h3':
        return config.maxH3Points;
      case 'bnbo':
        return config.maxBNBOPolygons;
      case 'bbr':
        return config.maxBBRPoints;
      default:
        return 1000;
    }
  }

  /**
   * Get performance multiplier based on performance mode
   */
  private getPerformanceMultiplier(): number {
    switch (this.performanceMode) {
      case 'high':
        return 1.5;
      case 'medium':
        return 1.0;
      case 'low':
        return 0.5;
      default:
        return 1.0;
    }
  }

  /**
   * Prioritize H3 data based on importance metrics
   */
  private prioritizeH3Data(data: H3DataPoint[], maxPoints: number): H3DataPoint[] {
    // Sort by combined importance score (PFAS + pesticide load)
    const scored = data.map(item => ({
      ...item,
      importance: (item.total_pfas_grams || 0) + (item.total_pesticide_load || 0)
    }));

    scored.sort((a, b) => b.importance - a.importance);
    
    return scored.slice(0, maxPoints);
  }

  /**
   * Prioritize BNBO data based on importance metrics
   */
  private prioritizeBNBOData(data: BNBOArea[], maxPolygons: number): BNBOArea[] {
    // Sort by area and protection status importance
    const statusPriority = {
      'protected': 5,
      'buffer': 4,
      'agricultural': 3,
      'transition': 2,
      'unprotected': 1
    };

    const scored = data.map(area => ({
      ...area,
      importance: (statusPriority[area.status_code] || 1) * Math.log(area.area_ha + 1)
    }));

    scored.sort((a, b) => b.importance - a.importance);
    
    return scored.slice(0, maxPolygons);
  }

  /**
   * Prioritize BBR data based on importance metrics
   */
  private prioritizeBBRData(data: BBRBuilding[], maxPoints: number): BBRBuilding[] {
    // Sort by building type importance and floor area
    const typePriority = {
      'Agricultural': 5,
      'Industrial': 4,
      'Commercial': 3,
      'Residential': 2,
      'Public': 2,
      'Other': 1
    };

    const scored = data.map(building => ({
      ...building,
      importance: (typePriority[building.building_type] || 1) * Math.log((building.floor_area || 0) + 1)
    }));

    scored.sort((a, b) => b.importance - a.importance);
    
    return scored.slice(0, maxPoints);
  }

  /**
   * Calculate memory usage estimate for dataset
   */
  estimateMemoryUsage(data: unknown[]): number {
    // Rough estimate: 1KB per data point
    return data.length * 1024;
  }

  /**
   * Check if dataset should be virtualized based on size
   */
  shouldVirtualize(data: unknown[], layerType: 'h3' | 'bnbo' | 'bbr'): boolean {
    const thresholds = {
      h3: 5000,
      bnbo: 2000,
      bbr: 10000
    };

    return data.length > thresholds[layerType];
  }
} 
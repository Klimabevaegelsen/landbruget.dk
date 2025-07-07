// Protomaps integration for custom base layer with PMTiles v3.2
import { PMTiles, Protocol } from 'pmtiles';

export interface ProtomapsConfig {
  pmtilesUrl: string;
  styleUrl?: string;
  attribution?: string;
}

export class ProtomapsManager {
  private pmtiles: PMTiles | null = null;
  private config: ProtomapsConfig;
  private isInitialized = false;

  constructor(config: ProtomapsConfig) {
    this.config = {
      attribution: '© Protomaps © OpenStreetMap',
      ...config
    };
  }

  async initialize(): Promise<void> {
    if (this.isInitialized) return;

    try {
      // Initialize PMTiles with custom Denmark tileset
      this.pmtiles = new PMTiles(this.config.pmtilesUrl);
      
      // Register protocol for deck.gl and Kepler.gl
      if (typeof window !== 'undefined') {
        Protocol.add(this.pmtiles);
      }
      
      this.isInitialized = true;
      console.log('Protomaps initialized successfully');
    } catch (error) {
      console.error('Failed to initialize Protomaps:', error);
      throw error;
    }
  }

  getMapStyle(): Record<string, unknown> {
    return {
      version: 8,
      name: 'Denmark Base Map',
      metadata: {
        'mapbox:autocomposite': false,
        'mapbox:type': 'template'
      },
      sources: {
        'protomaps': {
          type: 'vector',
          url: `pmtiles://${this.config.pmtilesUrl}`,
          attribution: this.config.attribution
        }
      },
      sprite: 'https://protomaps.github.io/basemaps-assets/sprites/v2/light',
      glyphs: 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf',
      layers: this.getDenmarkStyleLayers()
    };
  }

  private getDenmarkStyleLayers(): Array<Record<string, unknown>> {
    return [
      // Background
      {
        id: 'background',
        type: 'background',
        paint: {
          'background-color': '#f8f9fa'
        }
      },
      
      // Water bodies
      {
        id: 'water',
        source: 'protomaps',
        'source-layer': 'water',
        type: 'fill',
        paint: {
          'fill-color': '#a8d5e5',
          'fill-opacity': 0.8
        }
      },
      
      // Natural areas
      {
        id: 'natural',
        source: 'protomaps',
        'source-layer': 'natural',
        type: 'fill',
        filter: ['in', 'class', 'wood', 'forest', 'grass', 'scrub'],
        paint: {
          'fill-color': [
            'match',
            ['get', 'class'],
            ['wood', 'forest'], '#d4e5d4',
            ['grass', 'scrub'], '#e8f5e8',
            '#f0f8e8'
          ],
          'fill-opacity': 0.6
        }
      },
      
      // Agricultural areas
      {
        id: 'landuse-agricultural',
        source: 'protomaps',
        'source-layer': 'landuse',
        type: 'fill',
        filter: ['in', 'class', 'farmland', 'crop', 'orchard'],
        paint: {
          'fill-color': '#f0f8e8',
          'fill-opacity': 0.4
        }
      },
      
      // Built-up areas
      {
        id: 'landuse-built',
        source: 'protomaps',
        'source-layer': 'landuse',
        type: 'fill',
        filter: ['in', 'class', 'residential', 'commercial', 'industrial'],
        paint: {
          'fill-color': [
            'match',
            ['get', 'class'],
            'residential', '#f5f5f5',
            'commercial', '#eeeeee',
            'industrial', '#e8e8e8',
            '#f0f0f0'
          ],
          'fill-opacity': 0.5
        }
      },
      
      // Buildings
      {
        id: 'buildings',
        source: 'protomaps',
        'source-layer': 'buildings',
        type: 'fill',
        minzoom: 12,
        paint: {
          'fill-color': '#d6d6d6',
          'fill-opacity': [
            'interpolate',
            ['linear'],
            ['zoom'],
            12, 0,
            13, 0.3,
            16, 0.6
          ]
        }
      },
      
      // Roads - Major highways
      {
        id: 'roads-highway',
        source: 'protomaps',
        'source-layer': 'roads',
        type: 'line',
        filter: ['in', 'class', 'motorway', 'trunk'],
        paint: {
          'line-color': '#ffffff',
          'line-width': [
            'interpolate',
            ['linear'],
            ['zoom'],
            5, 1,
            10, 2,
            15, 8
          ],
          'line-opacity': 0.9
        }
      },
      
      // Roads - Primary roads
      {
        id: 'roads-primary',
        source: 'protomaps',
        'source-layer': 'roads',
        type: 'line',
        filter: ['in', 'class', 'primary', 'secondary'],
        paint: {
          'line-color': '#ffffff',
          'line-width': [
            'interpolate',
            ['linear'],
            ['zoom'],
            7, 0.5,
            10, 1,
            15, 4
          ],
          'line-opacity': 0.8
        }
      },
      
      // Roads - Local roads
      {
        id: 'roads-local',
        source: 'protomaps',
        'source-layer': 'roads',
        type: 'line',
        filter: ['in', 'class', 'tertiary', 'minor', 'service'],
        minzoom: 10,
        paint: {
          'line-color': '#ffffff',
          'line-width': [
            'interpolate',
            ['linear'],
            ['zoom'],
            10, 0.5,
            15, 2
          ],
          'line-opacity': 0.6
        }
      },
      
      // Administrative boundaries
      {
        id: 'admin-boundaries',
        source: 'protomaps',
        'source-layer': 'boundaries',
        type: 'line',
        filter: ['<=', 'admin_level', 6],
        paint: {
          'line-color': '#999999',
          'line-width': [
            'interpolate',
            ['linear'],
            ['zoom'],
            4, 0.5,
            8, 1,
            12, 2
          ],
          'line-dasharray': [2, 2],
          'line-opacity': 0.5
        }
      },
      
      // Place labels - Cities
      {
        id: 'place-city',
        source: 'protomaps',
        'source-layer': 'places',
        type: 'symbol',
        filter: ['in', 'class', 'city'],
        layout: {
          'text-field': '{name}',
          'text-font': ['Noto Sans Regular'],
          'text-size': [
            'interpolate',
            ['linear'],
            ['zoom'],
            4, 10,
            8, 14,
            12, 18
          ],
          'text-anchor': 'center',
          'text-offset': [0, 0]
        },
        paint: {
          'text-color': '#333333',
          'text-halo-color': '#ffffff',
          'text-halo-width': 1
        }
      },
      
      // Place labels - Towns
      {
        id: 'place-town',
        source: 'protomaps',
        'source-layer': 'places',
        type: 'symbol',
        filter: ['in', 'class', 'town'],
        minzoom: 8,
        layout: {
          'text-field': '{name}',
          'text-font': ['Noto Sans Regular'],
          'text-size': [
            'interpolate',
            ['linear'],
            ['zoom'],
            8, 10,
            12, 14
          ],
          'text-anchor': 'center'
        },
        paint: {
          'text-color': '#555555',
          'text-halo-color': '#ffffff',
          'text-halo-width': 1
        }
      },
      
      // Road labels
      {
        id: 'road-labels',
        source: 'protomaps',
        'source-layer': 'roads',
        type: 'symbol',
        filter: ['in', 'class', 'motorway', 'trunk', 'primary'],
        minzoom: 10,
        layout: {
          'text-field': '{name}',
          'text-font': ['Noto Sans Regular'],
          'text-size': 11,
          'symbol-placement': 'line',
          'text-rotation-alignment': 'map'
        },
        paint: {
          'text-color': '#666666',
          'text-halo-color': '#ffffff',
          'text-halo-width': 1
        }
      }
    ];
  }

  // Get optimized style for different zoom levels
  getOptimizedStyle(zoom: number): Record<string, unknown> {
    const baseStyle = this.getMapStyle();
    
    // Optimize layers based on zoom level
    if (zoom < 8) {
      // Country/region level - hide detailed features
      (baseStyle.layers as Array<Record<string, unknown>>) = (baseStyle.layers as Array<Record<string, unknown>>).filter((layer: Record<string, unknown>) => 
        !['buildings', 'roads-local', 'road-labels'].includes(layer.id as string)
      );
    } else if (zoom < 12) {
      // County level - show more detail but hide buildings
      (baseStyle.layers as Array<Record<string, unknown>>) = (baseStyle.layers as Array<Record<string, unknown>>).filter((layer: Record<string, unknown>) => 
        layer.id !== 'buildings'
      );
    }
    
    return baseStyle;
  }

  // Create custom style for agricultural focus
  getAgriculturalStyle(): Record<string, unknown> {
    const baseStyle = this.getMapStyle();
    
    // Enhance agricultural areas visibility
    const agriculturalLayer = (baseStyle.layers as Array<Record<string, unknown>>).find((layer: Record<string, unknown>) => 
      layer.id === 'landuse-agricultural'
    );
    
    if (agriculturalLayer && agriculturalLayer.paint && typeof agriculturalLayer.paint === 'object') {
      (agriculturalLayer.paint as Record<string, unknown>)['fill-opacity'] = 0.7;
      (agriculturalLayer.paint as Record<string, unknown>)['fill-color'] = '#e8f5e8';
    }
    
    return baseStyle;
  }

  // Cleanup resources
  destroy(): void {
    if (this.pmtiles && typeof window !== 'undefined') {
      try {
        Protocol.remove(this.pmtiles);
      } catch (error) {
        console.warn('Error removing PMTiles protocol:', error);
      }
    }
    this.pmtiles = null;
    this.isInitialized = false;
  }
}

// Default configuration for Denmark
export const DENMARK_PROTOMAPS_CONFIG: ProtomapsConfig = {
  pmtilesUrl: '/protomaps/denmark.pmtiles',
  attribution: '© Protomaps © OpenStreetMap contributors'
};

// Utility functions
export function createProtomapsManager(config?: Partial<ProtomapsConfig>): ProtomapsManager {
  return new ProtomapsManager({
    ...DENMARK_PROTOMAPS_CONFIG,
    ...config
  });
}

// Check if PMTiles is supported
export function isPMTilesSupported(): boolean {
  try {
    return typeof PMTiles !== 'undefined' && typeof Protocol !== 'undefined';
  } catch {
    return false;
  }
}

// Preload PMTiles for better performance
export async function preloadPMTiles(url: string): Promise<void> {
  if (typeof window === 'undefined') return;
  
  try {
    const response = await fetch(url, { method: 'HEAD' });
    if (!response.ok) {
      throw new Error(`PMTiles not accessible: ${response.status}`);
    }
    console.log('PMTiles preloaded successfully');
  } catch (error) {
    console.warn('PMTiles preload failed:', error);
  }
} 
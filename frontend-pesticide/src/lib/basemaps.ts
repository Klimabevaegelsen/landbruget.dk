export interface BasemapConfig {
  id: string
  name: string
  style: Record<string, unknown>
  attribution: string
  flavor: string
}

// Denmark bounding box for optimized basemap coverage
const DENMARK_BOUNDS = {
  west: 7.5,
  south: 54.5,
  east: 15.5,
  north: 57.8
}

// Denmark PMTiles URL on GCS
const DENMARK_PMTILES_URL = 'pmtiles://https://data.pesticidkortet.dk/pmtiles/protomaps_denmark.pmtiles'

// Create basemap styles using the actual Denmark PMTiles
function createDenmarkBasemapStyle(flavor: 'light' | 'black') {
  const isDark = flavor === 'black'

  return {
    version: 8,
    name: `Denmark ${flavor}`,
    glyphs: 'https://fonts.openmaptiles.org/{fontstack}/{range}.pbf',
    sources: {
      'protomaps-denmark': {
        type: 'vector',
        url: DENMARK_PMTILES_URL,
        attribution: '© Protomaps © OpenStreetMap'
      }
    },
    layers: [
      {
        id: 'background',
        type: 'background',
        paint: {
          'background-color': isDark ? '#000000' : '#f8f9fa'
        }
      },
      // Water features
      {
        id: 'water',
        type: 'fill',
        source: 'protomaps-denmark',
        'source-layer': 'water',
        paint: {
          'fill-color': isDark ? '#1a1a1a' : '#a8d8ff',
          'fill-opacity': 0.8
        }
      },
      // Land features
      {
        id: 'land',
        type: 'fill',
        source: 'protomaps-denmark',
        'source-layer': 'land',
        paint: {
          'fill-color': isDark ? '#0a0a0a' : '#f0f0f0',
          'fill-opacity': 1
        }
      },
      // Roads for context (minimal styling)
      {
        id: 'roads',
        type: 'line',
        source: 'protomaps-denmark',
        'source-layer': 'roads',
        filter: ['in', 'pmap:kind', 'highway', 'major_road'],
        paint: {
          'line-color': isDark ? '#333333' : '#cccccc',
          'line-width': {
            base: 1.2,
            stops: [[6, 0.5], [20, 30]]
          },
          'line-opacity': isDark ? 0.6 : 0.8
        }
      },
      // Places for minimal labeling
      {
        id: 'places',
        type: 'symbol',
        source: 'protomaps-denmark',
        'source-layer': 'places',
        filter: ['in', 'pmap:kind', 'city', 'town'],
        layout: {
          'text-field': ['get', 'name'],
          'text-font': ['Noto Sans Regular'],
          'text-size': {
            base: 1.2,
            stops: [[7, 12], [11, 16]]
          }
        },
        paint: {
          'text-color': isDark ? '#ffffff' : '#333333',
          'text-halo-color': isDark ? '#000000' : '#ffffff',
          'text-halo-width': 1
        }
      }
    ]
  }
}

export const BASEMAPS: Record<string, BasemapConfig> = {
  light: {
    id: 'light',
    name: 'Light',
    style: createDenmarkBasemapStyle('light'),
    attribution: '© Protomaps © OpenStreetMap',
    flavor: 'light'
  },
  dark: {
    id: 'dark',
    name: 'Dark',
    style: createDenmarkBasemapStyle('black'),
    attribution: '© Protomaps © OpenStreetMap',
    flavor: 'black'
  }
}

export function getBasemapForTheme(theme: 'light' | 'dark' | 'system'): BasemapConfig {
  if (theme === 'system') {
    // Check system preference
    const isDark = typeof window !== 'undefined' && window.matchMedia('(prefers-color-scheme: dark)').matches
    return isDark ? BASEMAPS.dark : BASEMAPS.light
  }

  return BASEMAPS[theme] || BASEMAPS.light
}

export function getBasemapStyleForTheme(theme: 'light' | 'dark' | 'system') {
  return getBasemapForTheme(theme).style
}

// Optimized style for Denmark-focused data visualization
export function getDenmarkOptimizedStyle(theme: 'light' | 'dark' | 'system') {
  const baseStyle = getBasemapStyleForTheme(theme)

  // Enhance the style for better data visualization
  return {
    ...baseStyle,
    center: [10.0, 56.0], // Denmark center
    zoom: 7,
    bounds: [
      [DENMARK_BOUNDS.west, DENMARK_BOUNDS.south],
      [DENMARK_BOUNDS.east, DENMARK_BOUNDS.north]
    ]
  }
}

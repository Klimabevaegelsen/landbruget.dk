import type { MapInstance } from '@/components/field-analysis/map-constants';

/** Burden-colored fill + outline layers for the pesticidkort map. */
export function addFieldLayers(map: MapInstance, pmtilesUrl: string) {
  if (!map.getSource('fields')) {
    map.addSource('fields', {
      type: 'vector',
      url: `pmtiles://${pmtilesUrl}`,
    });
  }

  if (!map.getLayer('fields-fill')) {
    map.addLayer({
      id: 'fields-fill',
      source: 'fields',
      'source-layer': 'field_analysis',
      type: 'fill',
      paint: {
        'fill-color': [
          'interpolate',
          ['linear'],
          ['coalesce', ['get', 'total_pesticide_belastning'], 0],
          0,
          'oklch(75% 0.12 142)',
          2,
          'oklch(80% 0.14 100)',
          5,
          'oklch(70% 0.16 65)',
          10,
          'oklch(60% 0.18 25)',
        ],
        'fill-opacity': 0.7,
      },
    });
    map.addLayer({
      id: 'fields-outline',
      source: 'fields',
      'source-layer': 'field_analysis',
      type: 'line',
      paint: {
        'line-color': 'oklch(40% 0.05 250)',
        'line-width': 0.5,
        'line-opacity': 0.4,
      },
    });
  }
}

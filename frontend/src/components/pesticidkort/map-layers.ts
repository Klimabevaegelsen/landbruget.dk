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
          '#6abf69',
          2,
          '#d4c54a',
          5,
          '#d89135',
          10,
          '#c4512c',
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
        'line-color': '#5f6b80',
        'line-width': 0.5,
        'line-opacity': 0.4,
      },
    });
  }
}

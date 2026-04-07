import type { MapInstance } from '@/components/field-analysis/map-constants';

const FIELD_FILL_COLOR = [
  'case',
  ['>', ['coalesce', ['get', 'pfas_applications'], 0], 0],
  '#9333ea',
  ['==', ['coalesce', ['get', 'total_pesticide_belastning'], 0], 0],
  '#9ca3af',
  '#dc2626',
] as const;

const FIELD_FILL_OPACITY = [
  'case',
  ['==', ['coalesce', ['get', 'total_pesticide_belastning'], 0], 0],
  0.4,
  0.7,
] as const;

/** Detail layers — full property data, used for queries at high zoom. */
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
      minzoom: 12,
      paint: {
        'fill-color': FIELD_FILL_COLOR as unknown as string,
        'fill-opacity': FIELD_FILL_OPACITY as unknown as number,
      },
    });
    map.addLayer({
      id: 'fields-outline',
      source: 'fields',
      'source-layer': 'field_analysis',
      type: 'line',
      minzoom: 12,
      paint: {
        'line-color': '#5f6b80',
        'line-width': 0.5,
        'line-opacity': 0.4,
      },
    });
  }
}

/** Overview layers — 3 properties only, all features at every zoom. */
export function addOverviewLayers(map: MapInstance, overviewUrl: string) {
  if (!map.getSource('fields-overview')) {
    map.addSource('fields-overview', {
      type: 'vector',
      url: `pmtiles://${overviewUrl}`,
    });
  }

  if (!map.getLayer('fields-overview-fill')) {
    map.addLayer({
      id: 'fields-overview-fill',
      source: 'fields-overview',
      'source-layer': 'field_analysis',
      type: 'fill',
      maxzoom: 12,
      paint: {
        'fill-color': FIELD_FILL_COLOR as unknown as string,
        'fill-opacity': FIELD_FILL_OPACITY as unknown as number,
      },
    });
    map.addLayer({
      id: 'fields-overview-outline',
      source: 'fields-overview',
      'source-layer': 'field_analysis',
      type: 'line',
      maxzoom: 12,
      paint: {
        'line-color': '#5f6b80',
        'line-width': 0.5,
        'line-opacity': 0.3,
      },
    });
  }
}

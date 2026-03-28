import { MapInstance } from './map-constants';
import { LayerVisibility } from './types';
import { createBNBOPatterns } from './map-pattern-bnbo';

function applyBnboPattern(map: MapInstance) {
  createBNBOPatterns(map).then(() => {
    if (!map.getLayer('bnbo-fill')) return;
    map.setPaintProperty('bnbo-fill', 'fill-pattern', [
      'case',
      ['==', ['get', 'status_category'], 'Action Required'],
      'bnbo-action-pattern',
      ['==', ['get', 'status_category'], 'Completed'],
      'bnbo-completed-pattern',
      '',
    ]);
  });
}

export function addBNBOLayers(
  map: MapInstance,
  visibility: LayerVisibility
): void {
  if (!map.getSource('bnbo') || map.getLayer('bnbo-fill')) return;

  const visible = visibility.bnbo ? 'visible' : 'none';

  try {
    map.addLayer({
      id: 'bnbo-fill',
      source: 'bnbo',
      'source-layer': 'bnbo',
      type: 'fill',
      paint: {
        'fill-color': [
          'case',
          ['==', ['get', 'status_category'], 'Action Required'],
          '#EAB308',
          ['==', ['get', 'status_category'], 'Completed'],
          '#10B981',
          '#2563EB',
        ],
        'fill-opacity': 0.6,
      },
      layout: { visibility: visible },
    });
  } catch (error) {
    console.error('Failed to add BNBO fill layer:', error);
  }

  applyBnboPattern(map);

  try {
    map.addLayer({
      id: 'bnbo-outline',
      source: 'bnbo',
      'source-layer': 'bnbo',
      type: 'line',
      paint: {
        'line-color': [
          'case',
          ['==', ['get', 'status_category'], 'Action Required'],
          '#DC2626',
          ['==', ['get', 'status_category'], 'Completed'],
          '#059669',
          '#1D4ED8',
        ],
        'line-width': 1.5,
        'line-opacity': 0.9,
      },
      layout: { visibility: visible },
    });
  } catch (error) {
    console.error('Failed to add BNBO outline layer:', error);
  }
}

import { MapInstance } from './map-constants';
import { LayerVisibility } from './types';

export function addBuildingsLayers(
  map: MapInstance,
  visibility: LayerVisibility
): void {
  if (!map.getSource('buildings') || map.getLayer('buildings-fill')) return;

  const visible = visibility.buildings ? 'visible' : 'none';

  try {
    map.addLayer({
      id: 'buildings-fill',
      source: 'buildings',
      'source-layer': 'buildings',
      type: 'fill',
      paint: {
        'fill-color': [
          'case',
          ['==', ['get', 'building_usage_category'], 'publicServices'],
          '#EC4899',
          ['==', ['get', 'building_usage_category'], 'agricultural'],
          '#A16207',
          '#4A90E2',
        ],
        'fill-opacity': 0.6,
      },
      layout: { visibility: visible },
    });
  } catch (error) {
    console.error('Failed to add Buildings fill layer:', error);
  }

  try {
    map.addLayer({
      id: 'buildings-outline',
      source: 'buildings',
      'source-layer': 'buildings',
      type: 'line',
      paint: {
        'line-color': [
          'case',
          ['==', ['get', 'building_usage_category'], 'publicServices'],
          '#BE185D',
          ['==', ['get', 'building_usage_category'], 'agricultural'],
          '#92400E',
          '#2563EB',
        ],
        'line-width': 1,
        'line-opacity': 0.8,
      },
      layout: { visibility: visible },
    });
  } catch (error) {
    console.error('Failed to add Buildings outline layer:', error);
  }
}

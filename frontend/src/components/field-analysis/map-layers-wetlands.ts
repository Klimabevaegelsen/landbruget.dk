import { MapInstance } from './map-constants';
import { LayerVisibility } from './types';
import { createWetlandsPattern } from './map-pattern-wetlands';

export function addWetlandsLayers(
  map: MapInstance,
  visibility: LayerVisibility
): void {
  if (!map.getSource('wetlands') || map.getLayer('wetlands-fill')) return;

  const visible = visibility.wetlands ? 'visible' : 'none';

  map.addLayer({
    id: 'wetlands-fill',
    source: 'wetlands',
    'source-layer': 'wetlands',
    type: 'fill',
    paint: { 'fill-color': '#3B82F6', 'fill-opacity': 0.4 },
    layout: { visibility: visible },
  });

  createWetlandsPattern(map);

  map.addLayer({
    id: 'wetlands-outline',
    source: 'wetlands',
    'source-layer': 'wetlands',
    type: 'line',
    paint: { 'line-color': '#1E40AF', 'line-width': 1.5, 'line-opacity': 0.8 },
    layout: { visibility: visible },
  });
}

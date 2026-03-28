import { MapInstance } from './map-constants';
import { LayerVisibility } from './types';
import { createWaterProjectsPattern } from './map-pattern-water-projects';

export function addWaterProjectsLayers(
  map: MapInstance,
  visibility: LayerVisibility
): void {
  if (!map.getSource('water_projects') || map.getLayer('water-projects-fill'))
    return;

  const visible = visibility.water_projects ? 'visible' : 'none';

  map.addLayer({
    id: 'water-projects-fill',
    source: 'water_projects',
    'source-layer': 'water_projects',
    type: 'fill',
    paint: { 'fill-color': '#14B8A6', 'fill-opacity': 0.5 },
    layout: { visibility: visible },
  });

  createWaterProjectsPattern(map);

  map.addLayer({
    id: 'water-projects-outline',
    source: 'water_projects',
    'source-layer': 'water_projects',
    type: 'line',
    paint: { 'line-color': '#0F766E', 'line-width': 2, 'line-opacity': 0.9 },
    layout: { visibility: visible },
  });
}

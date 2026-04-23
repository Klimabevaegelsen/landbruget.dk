import type { MapInstance } from '@/components/field-analysis/map-constants';
import { resolvePesticidkortColor } from '@/components/pesticidkort/color-theme';

export function syncBuildingLayerTheme(map: MapInstance) {
  if (map.getLayer('buildings-edu-fill')) {
    map.setPaintProperty(
      'buildings-edu-fill',
      'fill-color',
      resolvePesticidkortColor('schoolFill')
    );
  }

  if (map.getLayer('buildings-edu-outline')) {
    map.setPaintProperty(
      'buildings-edu-outline',
      'line-color',
      resolvePesticidkortColor('schoolOutline')
    );
  }
}

/** School and institution building outlines for the pesticidkort map. */
export function addBuildingLayers(map: MapInstance, pmtilesUrl: string) {
  if (!map.getSource('buildings')) {
    map.addSource('buildings', {
      type: 'vector',
      url: `pmtiles://${pmtilesUrl}`,
    });
  }

  if (!map.getLayer('buildings-edu-fill')) {
    map.addLayer({
      id: 'buildings-edu-fill',
      source: 'buildings',
      'source-layer': 'buildings',
      type: 'fill',
      filter: ['==', ['get', 'building_usage_category'], 'publicServices'],
      paint: {
        'fill-color': resolvePesticidkortColor('schoolFill'),
        'fill-opacity': 0.45,
      },
    });

    map.addLayer({
      id: 'buildings-edu-outline',
      source: 'buildings',
      'source-layer': 'buildings',
      type: 'line',
      filter: ['==', ['get', 'building_usage_category'], 'publicServices'],
      paint: {
        'line-color': resolvePesticidkortColor('schoolOutline'),
        'line-width': 1.5,
        'line-opacity': 0.8,
      },
    });
  }

  syncBuildingLayerTheme(map);
}

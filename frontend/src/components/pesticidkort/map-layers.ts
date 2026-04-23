import type { MapInstance } from '@/components/field-analysis/map-constants';
import {
  getGradeFillColor,
  getGradeFillOpacity,
} from '@/components/pesticidkort/map-chemical-filter';
import { resolvePesticidkortColor } from '@/components/pesticidkort/color-theme';

export type { ChemicalFilter } from '@/components/pesticidkort/map-chemical-filter';
export {
  CHEMICAL_COLORS,
  applyChemicalFilter,
} from '@/components/pesticidkort/map-chemical-filter';

export function syncFieldLayerTheme(map: MapInstance) {
  const fillColor = getGradeFillColor();
  const fillOpacity = getGradeFillOpacity();
  const outlineColor = resolvePesticidkortColor('fieldOutline');

  for (const layerId of ['fields-fill', 'fields-overview-fill']) {
    if (!map.getLayer(layerId)) continue;
    map.setPaintProperty(layerId, 'fill-color', fillColor);
    map.setPaintProperty(layerId, 'fill-opacity', fillOpacity);
  }

  for (const layerId of ['fields-outline', 'fields-overview-outline']) {
    if (!map.getLayer(layerId)) continue;
    map.setPaintProperty(layerId, 'line-color', outlineColor);
  }
}

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
        'fill-color': getGradeFillColor() as unknown as string,
        'fill-opacity': getGradeFillOpacity() as unknown as number,
      },
    });
    map.addLayer({
      id: 'fields-outline',
      source: 'fields',
      'source-layer': 'field_analysis',
      type: 'line',
      minzoom: 12,
      paint: {
        'line-color': resolvePesticidkortColor('fieldOutline'),
        'line-width': 0.5,
        'line-opacity': 0.4,
      },
    });
  }

  syncFieldLayerTheme(map);
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
        'fill-color': getGradeFillColor() as unknown as string,
        'fill-opacity': getGradeFillOpacity() as unknown as number,
      },
    });
    map.addLayer({
      id: 'fields-overview-outline',
      source: 'fields-overview',
      'source-layer': 'field_analysis',
      type: 'line',
      maxzoom: 12,
      paint: {
        'line-color': resolvePesticidkortColor('fieldOutline'),
        'line-width': 0.5,
        'line-opacity': 0.3,
      },
    });
  }

  syncFieldLayerTheme(map);
}

import type { MapInstance } from '@/components/field-analysis/map-constants';
import { resolvePesticidkortColor } from '@/components/pesticidkort/color-theme';

export type ChemicalFilter = 'none' | 'pfas' | 'glyphosate' | 'diquat';

export const CHEMICAL_COLORS = {
  pfas: '#9333ea',
  glyphosate: '#0891b2',
  diquat: '#db2777',
} as const;

const FILTER_PROPERTY: Record<Exclude<ChemicalFilter, 'none'>, string> = {
  pfas: 'pfas_applications',
  glyphosate: 'glyphosate_applications',
  diquat: 'diquat_applications',
};

function getChemicalColors() {
  return {
    pfas: resolvePesticidkortColor('pfas'),
    glyphosate: resolvePesticidkortColor('glyphosate'),
    diquat: resolvePesticidkortColor('diquat'),
  } as const;
}

/** Stepped burden gradient based on total_pesticide_belastning (B/ha). */
export function getGradeFillColor() {
  return [
    'case',
    ['==', ['coalesce', ['get', 'total_pesticide_belastning'], 0], 0],
    resolvePesticidkortColor('burdenNone'),
    [
      'step',
      ['get', 'total_pesticide_belastning'],
      resolvePesticidkortColor('burdenLow'),
      0.5,
      resolvePesticidkortColor('burdenMidLow'),
      2.0,
      resolvePesticidkortColor('burdenMid'),
      4.0,
      resolvePesticidkortColor('burdenMidHigh'),
      8.0,
      resolvePesticidkortColor('burdenHigh'),
    ],
  ];
}

export function getGradeFillOpacity() {
  return [
    'case',
    ['==', ['coalesce', ['get', 'total_pesticide_belastning'], 0], 0],
    0.42,
    0.72,
  ];
}

/** Apply or clear a chemical filter on both detail and overview fill layers. */
export function applyChemicalFilter(map: MapInstance, filter: ChemicalFilter) {
  const gradeFillColor = getGradeFillColor();
  const gradeFillOpacity = getGradeFillOpacity();

  if (filter === 'none') {
    setLayerPaint(map, 'fields-fill', gradeFillColor, gradeFillOpacity);
    setLayerPaint(
      map,
      'fields-overview-fill',
      gradeFillColor,
      gradeFillOpacity
    );
    return;
  }

  const prop = FILTER_PROPERTY[filter];
  const color = getChemicalColors()[filter];
  const hasChemical = ['>', ['coalesce', ['get', prop], 0], 0];

  const filteredColor = ['case', hasChemical, color, gradeFillColor];
  const filteredOpacity = ['case', hasChemical, 0.78, 0.12];

  // Detail layers have all properties — apply any filter
  setLayerPaint(map, 'fields-fill', filteredColor, filteredOpacity);

  // Overview tiles only have pfas_applications
  if (filter === 'pfas') {
    setLayerPaint(map, 'fields-overview-fill', filteredColor, filteredOpacity);
  } else {
    // Can't filter glyphosate/diquat on overview — dim everything slightly
    setLayerPaint(map, 'fields-overview-fill', gradeFillColor, [
      'literal',
      0.12,
    ]);
  }
}

function setLayerPaint(
  map: MapInstance,
  layerId: string,
  fillColor: unknown,
  fillOpacity: unknown
) {
  if (!map.getLayer(layerId)) return;
  map.setPaintProperty(layerId, 'fill-color', fillColor);
  map.setPaintProperty(layerId, 'fill-opacity', fillOpacity);
}

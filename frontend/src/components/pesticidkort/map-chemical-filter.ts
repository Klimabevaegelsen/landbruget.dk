import type { MapInstance } from '@/components/field-analysis/map-constants';

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

/** Stepped burden gradient based on total_pesticide_belastning (B/ha). */
export const GRADE_FILL_COLOR = [
  'case',
  ['==', ['coalesce', ['get', 'total_pesticide_belastning'], 0], 0],
  '#d1d5db',
  [
    'step',
    ['get', 'total_pesticide_belastning'],
    '#22c55e',
    0.5,
    '#84cc16',
    2.0,
    '#eab308',
    4.0,
    '#f97316',
    8.0,
    '#dc2626',
  ],
];

export const GRADE_FILL_OPACITY = [
  'case',
  ['==', ['coalesce', ['get', 'total_pesticide_belastning'], 0], 0],
  0.35,
  0.7,
];

/** Apply or clear a chemical filter on both detail and overview fill layers. */
export function applyChemicalFilter(map: MapInstance, filter: ChemicalFilter) {
  if (filter === 'none') {
    setLayerPaint(map, 'fields-fill', GRADE_FILL_COLOR, GRADE_FILL_OPACITY);
    setLayerPaint(
      map,
      'fields-overview-fill',
      GRADE_FILL_COLOR,
      GRADE_FILL_OPACITY
    );
    return;
  }

  const prop = FILTER_PROPERTY[filter];
  const color = CHEMICAL_COLORS[filter];
  const hasChemical = ['>', ['coalesce', ['get', prop], 0], 0];

  const filteredColor = ['case', hasChemical, color, GRADE_FILL_COLOR];
  const filteredOpacity = ['case', hasChemical, 0.75, 0.08];

  // Detail layers have all properties — apply any filter
  setLayerPaint(map, 'fields-fill', filteredColor, filteredOpacity);

  // Overview tiles only have pfas_applications
  if (filter === 'pfas') {
    setLayerPaint(map, 'fields-overview-fill', filteredColor, filteredOpacity);
  } else {
    // Can't filter glyphosate/diquat on overview — dim everything slightly
    setLayerPaint(map, 'fields-overview-fill', GRADE_FILL_COLOR, [
      'literal',
      0.08,
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

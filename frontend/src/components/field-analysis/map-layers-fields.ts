import { MapInstance, MapLibreLayer } from './map-constants';
import { createPartialCoveragePattern } from './map-pattern-fields';

const FIELD_LAYER_IDS = [
  'organic-borders',
  'fields-outline',
  'fields-partial-coverage-pattern',
  'fields-partial-coverage-base',
  'fields-fill',
];

interface AddFieldsLayersOptions {
  paintProps: Record<string, unknown>;
  visible: boolean;
  companyFilter: string | undefined;
}

function buildCompanyFilter(companyFilter: string | undefined): unknown {
  return companyFilter
    ? ['==', ['get', 'cvr_number'], parseInt(companyFilter)]
    : null;
}

function buildPartialCoverageFilter(companyFilterExpr: unknown): unknown {
  return companyFilterExpr
    ? ['all', companyFilterExpr, ['==', ['get', 'is_partial_coverage'], true]]
    : ['==', ['get', 'is_partial_coverage'], true];
}

export function removeFieldsLayers(map: MapInstance): void {
  FIELD_LAYER_IDS.forEach((layerId) => {
    if (map.getLayer(layerId)) map.removeLayer(layerId);
  });
}

export function addFieldsLayers(
  map: MapInstance,
  options: AddFieldsLayersOptions
): void {
  if (!map.getSource('fields') || map.getLayer('fields-fill')) return;

  const { paintProps, visible, companyFilter } = options;
  const visibility = visible ? 'visible' : 'none';
  const companyFilterExpr = buildCompanyFilter(companyFilter);
  const partialFilter = buildPartialCoverageFilter(companyFilterExpr);

  createPartialCoveragePattern(map);

  const fieldsLayer: MapLibreLayer = {
    id: 'fields-fill',
    source: 'fields',
    'source-layer': 'field_analysis',
    type: 'fill',
    paint: {
      'fill-color': paintProps['fill-color'],
      'fill-opacity': [
        'interpolate',
        ['linear'],
        ['zoom'],
        6,
        0.3,
        10,
        0.7,
        14,
        0.7,
      ],
    },
    layout: { visibility },
    minzoom: 6,
  };

  if (companyFilterExpr) fieldsLayer.filter = companyFilterExpr;
  map.addLayer(fieldsLayer);

  map.addLayer({
    id: 'fields-partial-coverage-base',
    source: 'fields',
    'source-layer': 'field_analysis',
    type: 'fill',
    paint: { ...paintProps },
    filter: partialFilter,
    layout: { visibility },
  });

  map.addLayer({
    id: 'fields-partial-coverage-pattern',
    source: 'fields',
    'source-layer': 'field_analysis',
    type: 'fill',
    paint: { 'fill-pattern': 'partial-coverage-pattern', 'fill-opacity': 0.7 },
    filter: partialFilter,
    layout: { visibility },
  });

  const fieldsOutlineLayer: MapLibreLayer = {
    id: 'fields-outline',
    source: 'fields',
    'source-layer': 'field_analysis',
    type: 'line',
    paint: {
      'line-color': '#374151',
      'line-width': [
        'interpolate',
        ['linear'],
        ['zoom'],
        6,
        0,
        10,
        0.3,
        14,
        0.5,
      ],
      'line-opacity': [
        'interpolate',
        ['linear'],
        ['zoom'],
        6,
        0,
        10,
        0.5,
        14,
        0.8,
      ],
    },
    layout: { visibility },
  };

  if (companyFilterExpr) fieldsOutlineLayer.filter = companyFilterExpr;
  map.addLayer(fieldsOutlineLayer);

  const organicFilter = companyFilterExpr
    ? ['all', companyFilterExpr, ['==', ['get', 'is_organic'], true]]
    : ['==', ['get', 'is_organic'], true];

  map.addLayer({
    id: 'organic-borders',
    source: 'fields',
    'source-layer': 'field_analysis',
    type: 'line',
    filter: organicFilter,
    paint: {
      'line-color': '#16a34a',
      'line-width': 3,
      'line-opacity': 0.9,
      'line-dasharray': [2, 2],
    },
    layout: { visibility },
  });
}

import { FilterState } from './types';
import { getDecileBreakpoints, getColorScheme } from './colorUtils';

export function getVisualizationFieldName(
  mode: FilterState['visualizationMode']
): string {
  switch (mode) {
    case 'total_pesticide_belastning':
      return 'total_pesticide_belastning';
    case 'pfas_belastning':
      return 'total_pfas_belastning';
    case 'diquat_belastning':
      return 'total_diquat_belastning';
    case 'glyphosate_belastning':
      return 'total_glyphosate_belastning';
    case 'applications_count':
      return 'total_pesticide_applications';
    case 'area_size':
      return 'area_hectares';
    default:
      return 'total_pesticide_belastning';
  }
}

export function buildFieldsPaintProps(filterState: {
  visualizationMode: FilterState['visualizationMode'];
  colorUnit: FilterState['colorUnit'];
  useDecileColoring: boolean;
}): Record<string, unknown> {
  const { visualizationMode, colorUnit, useDecileColoring } = filterState;
  const colorScheme = getColorScheme(visualizationMode);

  if (visualizationMode === 'organic_status') {
    return {
      'fill-color': [
        'case',
        ['==', ['get', 'is_organic'], true],
        'transparent',
        '#f3f4f6',
      ],
      'fill-opacity': 0.6,
    };
  }

  const fieldName = getVisualizationFieldName(visualizationMode);
  if (useDecileColoring) {
    const breakpoints = getDecileBreakpoints(visualizationMode, colorUnit);
    const colors = colorScheme.colors;

    return {
      'fill-color': [
        'case',
        ['<=', ['coalesce', ['get', fieldName], 0], 0],
        '#f3f4f6',
        [
          'step',
          ['coalesce', ['get', fieldName], 0],
          colors[0],
          breakpoints[0],
          colors[1],
          breakpoints[1],
          colors[2],
          breakpoints[2],
          colors[3],
          breakpoints[3],
          colors[4],
          breakpoints[4],
          colors[5],
          breakpoints[5],
          colors[6],
          breakpoints[6],
          colors[7],
          breakpoints[7],
          colors[8],
          breakpoints[8],
          colors[9],
        ],
      ],
      'fill-opacity': 0.7,
    };
  }

  const colors = colorScheme.colors;
  return {
    'fill-color': [
      'case',
      ['<=', ['coalesce', ['get', fieldName], 0], 0],
      '#f3f4f6',
      [
        'interpolate',
        ['linear'],
        ['coalesce', ['get', fieldName], 0],
        0.1,
        colors[0],
        1,
        colors[2],
        10,
        colors[4],
        50,
        colors[6],
        100,
        colors[8],
        500,
        colors[9],
      ],
    ],
    'fill-opacity': 0.7,
  };
}

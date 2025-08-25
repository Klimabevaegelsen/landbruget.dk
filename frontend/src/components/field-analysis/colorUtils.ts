import { FieldAnalysisData, VisualizationMode, ColorUnit } from './types';

/**
 * Color utility functions for field analysis visualization
 */

// Color schemes for different chemicals
// Unified pesticide color scheme - white to red gradient for all pesticide types
const UNIFIED_PESTICIDE_COLORS = ['#ffffff', '#fef2f2', '#fecaca', '#fca5a5', '#f87171', '#ef4444', '#dc2626', '#b91c1c', '#991b1b', '#7f1d1d'];

export const COLOR_SCHEMES = {
  pfas: {
    name: 'PFAS',
    colors: UNIFIED_PESTICIDE_COLORS,
    description: 'Unified scale for PFAS compounds'
  },
  diquat: {
    name: 'Diquat',
    colors: UNIFIED_PESTICIDE_COLORS,
    description: 'Unified scale for Diquat compounds'
  },
  glyphosate: {
    name: 'Glyphosate',
    colors: UNIFIED_PESTICIDE_COLORS,
    description: 'Unified scale for Glyphosate compounds'
  },
  general: {
    name: 'General',
    colors: ['#fafafa', '#f4f4f5', '#e4e4e7', '#d4d4d8', '#a1a1aa', '#71717a', '#52525b', '#3f3f46', '#27272a', '#18181b'],
    description: 'Gray scale for non-pesticide data'
  },
  pesticide: {
    name: 'Pesticide',
    colors: UNIFIED_PESTICIDE_COLORS,
    description: 'Unified scale for total pesticide data'
  },
  applications: {
    name: 'Applications',
    colors: UNIFIED_PESTICIDE_COLORS,
    description: 'Unified scale for pesticide applications'
  },
  organic: {
    name: 'Organic',
    colors: ['#f0fdf4', '#dcfce7'],
    description: 'Green for organic fields'
  }
} as const;

// Real decile breakpoints from field analysis data (617,774 fields total)
export const REAL_DECILES = {
  // PFAS data (156,025 fields with PFAS, 25.26% of total)
  pfas_kg: [0.105956, 0.214680, 0.348328, 0.513398, 0.732242, 1.023773, 1.453523, 2.153765, 3.629328, 2106.917410],
  pfas_kg_per_ha: [29.307, 44.679, 78.976, 99.825, 128.507, 149.620, 189.624, 223.009, 327.331, 53137.428], // g/ha
  pfas_belastning: [0.237, 0.522, 0.963, 1.686, 2.853, 4.611, 7.299, 11.857, 21.257, 2100.731],

  // Diquat data (471 fields with diquat, 0.08% of total)
  diquat_liters: [5.474, 9.619, 14.186, 18.925, 23.403, 29.860, 37.531, 46.851, 64.388, 251.204],
  diquat_liters_per_ha: [1.085, 1.341, 1.636, 1.748, 2.047, 2.310, 2.674, 3.002, 3.762, 8.528],
  diquat_belastning: [3.240, 6.074, 8.930, 12.256, 15.116, 17.431, 21.391, 26.677, 33.567, 105.877],

  // Glyphosate data (105,511 fields with glyphosate, 17.08% of total)
  glyphosate_kg: [0.640112, 1.379007, 2.236467, 3.276975, 4.567332, 6.254282, 8.560291, 12.135696, 19.316233, 468.251199],
  glyphosate_kg_per_ha: [179.114, 320.419, 473.283, 617.866, 781.562, 950.369, 1083.182, 1353.464, 1517.744, 13071.985], // g/ha
  glyphosate_belastning: [0.237, 0.522, 0.963, 1.686, 2.853, 4.611, 7.299, 11.857, 21.257, 2100.731], // Same as PFAS

  // Total pesticide data (195,025 fields with pesticides, 31.57% of total)
  total_pesticide_belastning: [0.800, 1.806, 3.063, 4.735, 7.025, 10.327, 15.433, 24.208, 42.906, 2597.082],
  total_pesticide_applications: [3, 4, 5, 6, 7, 8, 10, 11, 13, 32],

  // Field areas (all 617,774 fields)
  area_hectares: [0.296, 0.554, 0.908, 1.368, 2.014, 2.934, 4.335, 6.589, 11.028, 876.023]
} as const;

/**
 * Calculate deciles from an array of values (fallback for dynamic calculation)
 */
export function calculateDeciles(values: number[]): number[] {
  const sortedValues = [...values].sort((a, b) => a - b);
  const deciles: number[] = [];

  for (let i = 1; i <= 10; i++) {
    const percentile = i / 10;
    const index = Math.ceil(percentile * sortedValues.length) - 1;
    deciles.push(sortedValues[Math.min(index, sortedValues.length - 1)]);
  }

  return deciles;
}

/**
 * Get value for visualization from field data
 */
export function getVisualizationValue(
  field: FieldAnalysisData,
  mode: VisualizationMode,
  unit: ColorUnit
): number {
  const area = field.area_hectares || 1; // Avoid division by zero

  switch (mode) {
    case 'total_pesticide_belastning':
      return unit === 'per_hectare'
        ? field.total_pesticide_belastning / area
        : field.total_pesticide_belastning;

    case 'pfas_belastning':
      const pfasBelastning = field.total_pfas_belastning || 0;
      return unit === 'per_hectare' ? pfasBelastning / area : pfasBelastning;

    case 'diquat_belastning':
      const diquatBelastning = field.total_diquat_belastning || 0;
      return unit === 'per_hectare' ? diquatBelastning / area : diquatBelastning;

    case 'glyphosate_belastning':
      const glyphosateBelastning = field.total_glyphosate_belastning || 0;
      return unit === 'per_hectare' ? glyphosateBelastning / area : glyphosateBelastning;

    case 'applications_count':
      return field.total_pesticide_applications || 0;

    case 'organic_status':
      return field.is_organic ? 1 : 0;

    case 'area_size':
      return field.area_hectares;

    default:
      return 0;
  }
}

/**
 * Get appropriate color scheme for visualization mode
 */
export function getColorScheme(mode: VisualizationMode): typeof COLOR_SCHEMES[keyof typeof COLOR_SCHEMES] {
  if (mode.includes('pfas')) return COLOR_SCHEMES.pfas;
  if (mode.includes('diquat')) return COLOR_SCHEMES.diquat;
  if (mode.includes('glyphosate')) return COLOR_SCHEMES.glyphosate;
  if (mode === 'organic_status') return COLOR_SCHEMES.organic;
  if (mode === 'total_pesticide_belastning') return COLOR_SCHEMES.pesticide;
  if (mode === 'applications_count') return COLOR_SCHEMES.applications;
  return COLOR_SCHEMES.general;
}

/**
 * Get the appropriate deciles for a visualization mode (prioritizing belastning for comparability)
 */
export function getDecileBreakpoints(mode: VisualizationMode, unit: ColorUnit): readonly number[] {
  // All modes now use belastning for optimal comparability
  switch (mode) {
    case 'total_pesticide_belastning':
      return unit === 'per_hectare'
        ? REAL_DECILES.total_pesticide_belastning.map(v => v / 2.5) // Rough per-hectare estimate
        : REAL_DECILES.total_pesticide_belastning;

    case 'pfas_belastning':
      return unit === 'per_hectare'
        ? REAL_DECILES.pfas_belastning.map(v => v / 2.5) // Rough per-hectare estimate
        : REAL_DECILES.pfas_belastning;

    case 'diquat_belastning':
      return unit === 'per_hectare'
        ? REAL_DECILES.diquat_belastning.map(v => v / 2.5) // Rough per-hectare estimate
        : REAL_DECILES.diquat_belastning;

    case 'glyphosate_belastning':
      return unit === 'per_hectare'
        ? REAL_DECILES.glyphosate_belastning.map(v => v / 2.5) // Rough per-hectare estimate
        : REAL_DECILES.glyphosate_belastning;

    case 'applications_count':
      return REAL_DECILES.total_pesticide_applications;

    case 'area_size':
      return REAL_DECILES.area_hectares;

    default:
      return [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]; // Fallback
  }
}

/**
 * Get color for a field based on decile ranking
 */
export function getDecileColor(
  field: FieldAnalysisData,
  mode: VisualizationMode,
  unit: ColorUnit
): string {
  const value = getVisualizationValue(field, mode, unit);
  const colorScheme = getColorScheme(mode);
  const deciles = getDecileBreakpoints(mode, unit);

  // Special case for organic status
  if (mode === 'organic_status') {
    return field.is_organic ? colorScheme.colors[1] : colorScheme.colors[0];
  }

  // Find which decile this value falls into
  let decileIndex = 0;
  for (let i = 0; i < deciles.length; i++) {
    if (value <= deciles[i]) {
      decileIndex = i;
      break;
    }
  }

  // Ensure we don't exceed array bounds
  decileIndex = Math.min(decileIndex, colorScheme.colors.length - 1);

  return colorScheme.colors[decileIndex];
}

/**
 * Get color for a field based on linear scaling
 */
export function getLinearColor(
  field: FieldAnalysisData,
  mode: VisualizationMode,
  unit: ColorUnit,
  minValue: number,
  maxValue: number
): string {
  const value = getVisualizationValue(field, mode, unit);
  const colorScheme = getColorScheme(mode);

  // Special case for organic status
  if (mode === 'organic_status') {
    return field.is_organic ? colorScheme.colors[1] : colorScheme.colors[0];
  }

  // Avoid division by zero
  if (maxValue === minValue) {
    return colorScheme.colors[0];
  }

  // Calculate normalized value (0-1)
  const normalized = (value - minValue) / (maxValue - minValue);

  // Map to color index
  const colorIndex = Math.floor(normalized * (colorScheme.colors.length - 1));
  const clampedIndex = Math.max(0, Math.min(colorIndex, colorScheme.colors.length - 1));

  return colorScheme.colors[clampedIndex];
}

/**
 * Format value for display with appropriate units
 */
export function formatVisualizationValue(
  value: number,
  mode: VisualizationMode,
  unit: ColorUnit
): string {
  const formatNumber = (num: number, decimals: number = 2): string => {
    return num.toLocaleString('da-DK', { maximumFractionDigits: decimals });
  };

  if (mode === 'organic_status') {
    return value === 1 ? 'Økologisk' : 'Konventionel';
  }

  if (unit === 'applications') {
    return `${Math.round(value)} applikationer`;
  }

  if (unit === 'per_hectare') {
    return `${formatNumber(value, 2)} belastning/ha`;
  }

  if (mode === 'area_size') {
    return `${formatNumber(value, 1)} ha`;
  }

  // Default: belastning
  return `${formatNumber(value, 2)} belastning`;
}

/**
 * Get unit label for display
 */
export function getUnitLabel(mode: VisualizationMode, unit: ColorUnit): string {
  if (mode === 'organic_status') return 'Status';
  if (mode === 'area_size') return 'Hektar';
  if (unit === 'applications') return 'Applikationer';
  if (unit === 'per_hectare') {
    if (mode.includes('grams')) return 'g/ha';
    return 'Belastning/ha';
  }
  if (mode.includes('grams')) return 'Gram';
  return 'Belastning';
}

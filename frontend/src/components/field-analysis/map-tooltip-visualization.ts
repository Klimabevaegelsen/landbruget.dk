import { FilterState } from './types';
import { TooltipDatum } from './map-tooltip-data';

export function pushVisualizationData(
  data: TooltipDatum[],
  properties: Record<string, unknown>,
  visualizationMode: FilterState['visualizationMode'],
  colorUnit: FilterState['colorUnit']
) {
  const perHaUnit = colorUnit === 'per_hectare' ? 'per ha' : '';
  if (visualizationMode === 'total_pesticide_belastning') {
    if (properties.total_pesticide_belastning)
      data.push({
        label: 'Total pesticidbelastning',
        value: properties.total_pesticide_belastning,
        unit: perHaUnit,
      });
    if (properties.total_pesticide_applications)
      data.push({
        label: 'Antal pesticider',
        value: properties.total_pesticide_applications,
      });
  } else if (visualizationMode === 'pfas_belastning') {
    if (properties.total_pfas_belastning)
      data.push({
        label: 'PFAS belastning',
        value: properties.total_pfas_belastning,
        unit: perHaUnit,
      });
    if (properties.total_pfas_active_ingredient_kg)
      data.push({
        label: 'PFAS aktivstof',
        value: properties.total_pfas_active_ingredient_kg,
        unit: 'kg',
      });
    if (properties.pfas_applications)
      data.push({
        label: 'PFAS pesticider',
        value: properties.pfas_applications,
      });
  } else if (visualizationMode === 'diquat_belastning') {
    if (properties.total_diquat_belastning)
      data.push({
        label: 'Diquat belastning',
        value: properties.total_diquat_belastning,
        unit: perHaUnit,
      });
    if (properties.diquat_applications)
      data.push({
        label: 'Diquat pesticider',
        value: properties.diquat_applications,
      });
  } else if (visualizationMode === 'glyphosate_belastning') {
    if (properties.total_glyphosate_belastning)
      data.push({
        label: 'Glyphosate belastning',
        value: properties.total_glyphosate_belastning,
        unit: perHaUnit,
      });
    if (properties.total_glyphosate_active_ingredient_kg)
      data.push({
        label: 'Glyphosate aktivstof',
        value: properties.total_glyphosate_active_ingredient_kg,
        unit: 'kg',
      });
    if (properties.glyphosate_applications)
      data.push({
        label: 'Glyphosate pesticider',
        value: properties.glyphosate_applications,
      });
  } else if (visualizationMode === 'applications_count') {
    if (properties.total_pesticide_applications)
      data.push({
        label: 'Total pesticider',
        value: properties.total_pesticide_applications,
      });
    if (properties.unique_pesticide_products)
      data.push({
        label: 'Unikke produkter',
        value: properties.unique_pesticide_products,
      });
  } else if (visualizationMode === 'area_size' && properties.area_hectares) {
    data.push({
      label: 'Markareal',
      value: properties.area_hectares,
      unit: 'ha',
    });
  }
}

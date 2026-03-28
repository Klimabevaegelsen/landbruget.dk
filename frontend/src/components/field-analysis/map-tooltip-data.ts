import {
  BBR_USAGE_LABELS,
  BUILDING_CATEGORY_LABELS,
  INSPIRE_USAGE_LABELS,
} from './map-constants';
import { FilterState } from './types';

export interface TooltipDatum {
  label: string;
  value: unknown;
  unit?: string;
}

function pushBaseData(
  data: TooltipDatum[],
  properties: Record<string, unknown>
) {
  if (properties.crop_name)
    data.push({ label: 'Afgrøde', value: properties.crop_name });
  if (properties.area_hectares) {
    data.push({
      label: 'Markareal',
      value: properties.area_hectares,
      unit: 'ha',
    });
  }
  if (properties.is_organic !== undefined) {
    data.push({
      label: 'Økologisk',
      value: properties.is_organic ? 'Ja' : 'Nej',
    });
  }
  if (properties.kommune)
    data.push({ label: 'Kommune', value: properties.kommune });
}

function pushEnvironmentalData(
  data: TooltipDatum[],
  properties: Record<string, unknown>,
  layerName: string
) {
  if (layerName === 'BNBO Område' && properties.status_category) {
    const isAction = properties.status_category === 'Action Required';
    const isCompleted = properties.status_category === 'Completed';
    data.push({
      label: isAction
        ? 'BNBO handling påkrævet'
        : isCompleted
          ? 'BNBO gennemført'
          : 'BNBO status',
      value: isAction
        ? 'Handling påkrævet'
        : isCompleted
          ? 'Gennemført'
          : properties.status_category,
    });
  }
  if (layerName === 'Lavbundsområde') {
    if (properties.wetland_id)
      data.push({ label: 'Vådomr. ID', value: properties.wetland_id });
    if (properties.toerv_pct)
      data.push({ label: 'Tørv indhold', value: properties.toerv_pct });
    if (properties.toerv_description) {
      data.push({
        label: 'Tørv beskrivelse',
        value: properties.toerv_description,
      });
    }
  }
  if (layerName === 'Vandprojekt') {
    if (properties.project_id)
      data.push({ label: 'Projekt ID', value: properties.project_id });
    if (properties.feature_count)
      data.push({ label: 'Antal features', value: properties.feature_count });
    if (properties.dissolved_at)
      data.push({ label: 'Opløst dato', value: properties.dissolved_at });
  }
}

function pushBuildingData(
  data: TooltipDatum[],
  properties: Record<string, unknown>
) {
  if (properties.address)
    data.push({ label: 'Adresse', value: properties.address });
  if (properties.building_usage_category) {
    const key = properties.building_usage_category as string;
    data.push({
      label: 'Kategori',
      value: BUILDING_CATEGORY_LABELS[key] || key,
    });
  }
  if (properties.bbr_usage_code) {
    const code = properties.bbr_usage_code as string;
    data.push({
      label: 'BBR anvendelse',
      value: BBR_USAGE_LABELS[code] || `BBR kode ${code}`,
    });
  } else if (properties.inspire_current_use) {
    const usage = properties.inspire_current_use as string;
    data.push({
      label: 'Anvendelse',
      value: INSPIRE_USAGE_LABELS[usage] || usage,
    });
  }
  if (properties.building_type)
    data.push({ label: 'Bygningstype', value: properties.building_type });
  if (properties.inspire_construction_year) {
    data.push({
      label: 'Byggeår',
      value: properties.inspire_construction_year,
    });
  }
  if (properties.inspire_floor_area) {
    data.push({
      label: 'Etageareal',
      value: properties.inspire_floor_area,
      unit: 'm²',
    });
  }
  if (properties.inspire_floors)
    data.push({ label: 'Etager', value: properties.inspire_floors });
  if (properties.inspire_dwellings)
    data.push({ label: 'Boliger', value: properties.inspire_dwellings });
  if (properties.distance_m)
    data.push({
      label: 'Afstand til mark',
      value: properties.distance_m,
      unit: 'm',
    });
}

function pushVisualizationData(
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

export function buildTooltipData(
  properties: Record<string, unknown>,
  layerName: string,
  visualizationMode: FilterState['visualizationMode'],
  colorUnit: FilterState['colorUnit']
): TooltipDatum[] {
  const data: TooltipDatum[] = [];
  pushEnvironmentalData(data, properties, layerName);
  pushBaseData(data, properties);
  if (layerName === 'Bygning') pushBuildingData(data, properties);
  pushVisualizationData(data, properties, visualizationMode, colorUnit);
  return data.slice(0, 6);
}

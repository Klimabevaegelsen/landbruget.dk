import { FilterState } from './types';
import { pushBuildingData } from './map-tooltip-buildings';
import { pushVisualizationData } from './map-tooltip-visualization';

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

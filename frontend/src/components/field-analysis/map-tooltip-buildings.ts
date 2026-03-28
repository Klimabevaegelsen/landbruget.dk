import {
  BBR_USAGE_LABELS,
  BUILDING_CATEGORY_LABELS,
  INSPIRE_USAGE_LABELS,
} from './map-constants';
import { TooltipDatum } from './map-tooltip-data';

export function pushBuildingData(
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

import { dataFetch } from './config';

export interface ClimateEmission {
  id: string;
  company_id: string;
  cvr_number: string;
  year: number;
  total_co2e_kg: number;
  emissions_by_category: {
    [category: string]: number;
  };
  co2e_per_ha: number;
  co2e_per_animal_unit: number;
  co2e_per_production_unit: number;
  data_completeness: number;
  calculation_timestamp: string;
}

export async function getClimateEmissions(
  companyId: string,
  _year?: number
): Promise<ClimateEmission[]> {
  return dataFetch<ClimateEmission[]>(`/companies/${companyId}/climate.json`);
}

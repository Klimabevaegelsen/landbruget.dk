import { apiFetch } from "./config";

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
  cvr: string,
  year?: number
): Promise<ClimateEmission[]> {
  try {
    let path = `/functions/v1/api/climate?cvr=${cvr}`;
    if (year) {
      path += `&year=${year}`;
    }

    const response = await apiFetch(path);

    if (!response.ok) {
      throw new Error(
        `Failed to fetch climate emissions: ${response.statusText}`
      );
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error fetching climate emissions:", error);
    throw error;
  }
}

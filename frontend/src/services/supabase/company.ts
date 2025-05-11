import { apiFetch } from "./config";
import { CompanyResponse } from "./types";

export interface Company {
  id: string;
  // Add other company fields as they become known
}

export async function getCompanyById(id: string): Promise<CompanyResponse> {
  try {
    console.log("Fetching company by id:", id);

    const response = await apiFetch(`/functions/v1/api?id=${id}`);

    if (!response.ok) {
      throw new Error(`Failed to fetch company: ${response.statusText}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error("Error fetching company:", error);
    throw error;
  }
}

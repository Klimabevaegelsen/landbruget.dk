import { apiFetch } from './config';

export interface BasicCompanyInfo {
  id: string;
  municipality: string;
  cvr_number: string;
  company_name?: string;
  address?: string;
  address_geom?: Record<string, unknown>;
}

export interface BasicCompanyResponse {
  metadata: {
    api_version: string;
    generated_at: string;
    company_id: string;
    company_cvr: string;
    municipality: string;
  };
  company: BasicCompanyInfo;
}

export async function getBasicCompanyById(
  id: string
): Promise<BasicCompanyResponse> {
  try {
    console.log('Fetching basic company info by id:', id);

    const response = await apiFetch(`/functions/v1/company-basic?id=${id}`);

    if (!response.ok) {
      throw new Error(
        `Failed to fetch basic company info: ${response.statusText}`
      );
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Error fetching basic company info:', error);
    throw error;
  }
}

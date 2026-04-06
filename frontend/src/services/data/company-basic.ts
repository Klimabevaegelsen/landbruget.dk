import { dataFetch } from './config';

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
  return dataFetch<BasicCompanyResponse>(`/companies/${id}/basic.json`);
}

import { CompanyResponse } from '@/services/data/types';

import { dataFetch } from './config';

export async function getCompanyById(id: string): Promise<CompanyResponse> {
  return dataFetch<CompanyResponse>(`/companies/${id}.json`);
}

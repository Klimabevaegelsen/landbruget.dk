import { useEffect, useState } from 'react';
import {
  BasicCompanyInfo,
  getBasicCompanyById,
} from '@/services/data/company-basic';
import { getCompanyById } from '@/services/data/company';
import { CompanyResponse } from '@/services/data/types';
import { toast } from 'sonner';

const COMPANY_NAVIGATION_LOADING_TOAST_ID = 'company-navigation-loading';
const COMPANY_DETAILS_LOADING_TOAST_ID = 'company-details-loading';

interface UseCompanyDataReturn {
  basicCompanyInfo: BasicCompanyInfo | null;
  fullCompanyData: CompanyResponse | null;
  basicLoading: boolean;
  fullLoading: boolean;
  error: string | null;
}

export function useCompanyData(companyId: string): UseCompanyDataReturn {
  const [basicCompanyInfo, setBasicCompanyInfo] =
    useState<BasicCompanyInfo | null>(null);
  const [fullCompanyData, setFullCompanyData] =
    useState<CompanyResponse | null>(null);
  const [basicLoading, setBasicLoading] = useState(true);
  const [fullLoading, setFullLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadBasicInfo = async () => {
      try {
        const basicData = await getBasicCompanyById(companyId);
        setBasicCompanyInfo(basicData.company);
        setBasicLoading(false);

        // Clear navigation loading toast
        toast.dismiss(COMPANY_NAVIGATION_LOADING_TOAST_ID);

        // Show loading toast for detailed data
        const toastId = toast.loading('Indlæser detaljeret data', {
          id: COMPANY_DETAILS_LOADING_TOAST_ID,
          description: 'Henter grafer, tabeller og kort...',
        });

        // Load full data
        const fullData = await getCompanyById(companyId);
        setFullCompanyData(fullData);
        setFullLoading(false);

        // Remove detailed loading toast
        toast.dismiss(toastId);
      } catch (err) {
        console.error('Error loading company data:', err);
        setError(
          err instanceof Error ? err.message : 'Failed to load company data'
        );
        setBasicLoading(false);
        setFullLoading(false);
        toast.dismiss(COMPANY_NAVIGATION_LOADING_TOAST_ID);
        toast.dismiss(COMPANY_DETAILS_LOADING_TOAST_ID);
      }
    };

    loadBasicInfo();
  }, [companyId]);

  return {
    basicCompanyInfo,
    fullCompanyData,
    basicLoading,
    fullLoading,
    error,
  };
}

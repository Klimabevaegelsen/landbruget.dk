import { useEffect, useState } from 'react';
import {
  BasicCompanyInfo,
  getBasicCompanyById,
} from '@/services/supabase/company-basic';
import { getCompanyById } from '@/services/supabase/company';
import { CompanyResponse } from '@/services/supabase/types';
import { useToast } from '@/components/ui/toast';

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

  const { clearLoadingToasts, addToast, removeToast } = useToast();

  useEffect(() => {
    const loadBasicInfo = async () => {
      try {
        const basicData = await getBasicCompanyById(companyId);
        setBasicCompanyInfo(basicData.company);
        setBasicLoading(false);

        // Clear navigation loading toast
        clearLoadingToasts();

        // Show loading toast for detailed data
        const toastId = addToast({
          title: 'Indlæser detaljeret data',
          description: 'Henter grafer, tabeller og kort...',
          variant: 'loading',
        });

        // Load full data
        const fullData = await getCompanyById(companyId);
        setFullCompanyData(fullData);
        setFullLoading(false);

        // Remove detailed loading toast
        removeToast(toastId);
      } catch (err) {
        console.error('Error loading company data:', err);
        setError(
          err instanceof Error ? err.message : 'Failed to load company data'
        );
        setBasicLoading(false);
        setFullLoading(false);
        clearLoadingToasts();
      }
    };

    loadBasicInfo();
  }, [companyId, clearLoadingToasts, addToast, removeToast]);

  return {
    basicCompanyInfo,
    fullCompanyData,
    basicLoading,
    fullLoading,
    error,
  };
}

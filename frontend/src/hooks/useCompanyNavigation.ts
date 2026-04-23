'use client';

import { useRouter } from 'next/navigation';
import { toast } from 'sonner';
import { useCallback } from 'react';

const COMPANY_NAVIGATION_LOADING_TOAST_ID = 'company-navigation-loading';
const COMPANY_DETAILS_LOADING_TOAST_ID = 'company-details-loading';

/**
 * Custom hook for handling company navigation with loading indicators
 */
export function useCompanyNavigation() {
  const router = useRouter();

  const navigateToCompany = useCallback(
    (companyId: string, companyName?: string) => {
      toast.dismiss(COMPANY_NAVIGATION_LOADING_TOAST_ID);
      toast.dismiss(COMPANY_DETAILS_LOADING_TOAST_ID);

      const toastId = toast.loading('Indlæser virksomhed', {
        id: COMPANY_NAVIGATION_LOADING_TOAST_ID,
        description: companyName
          ? `Henter data for ${companyName}...`
          : 'Henter virksomhedsdata...',
      });

      router.push(`/company/${companyId}`);

      return toastId;
    },
    [router]
  );

  return { navigateToCompany };
}

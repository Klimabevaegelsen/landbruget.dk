'use client';

import { useRouter } from 'next/navigation';
import { useToast } from '@/components/ui/toast';
import { useCallback } from 'react';

/**
 * Custom hook for handling company navigation with loading indicators
 */
export function useCompanyNavigation() {
  const router = useRouter();
  const { addToast, clearLoadingToasts } = useToast();

  const navigateToCompany = useCallback(
    (companyId: string, companyName?: string) => {
      // Clear any existing loading toasts
      clearLoadingToasts();

      // Show loading toast
      const toastId = addToast({
        title: 'Indlæser virksomhed',
        description: companyName
          ? `Henter data for ${companyName}...`
          : 'Henter virksomhedsdata...',
        variant: 'loading',
      });

      // Navigate to company page
      router.push(`/virksomhed/${companyId}`);

      // Return toast ID in case caller wants to manage it
      return toastId;
    },
    [router, addToast, clearLoadingToasts]
  );

  return { navigateToCompany };
}

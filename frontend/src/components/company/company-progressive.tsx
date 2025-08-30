'use client';

import { useEffect, useState } from 'react';
import { CompanyResponse } from '@/services/supabase/types';
import {
  BasicCompanyInfo,
  getBasicCompanyById,
} from '@/services/supabase/company-basic';
import { getCompanyById } from '@/services/supabase/company';
import { CompanyHero } from './company-hero';
import { Container } from '../layout/container';
import { PageBuilderProgressive } from '../pagebuilder/pagebuilder-progressive';
import { useToast } from '@/components/ui/toast';
import { Skeleton } from '@/components/ui/skeleton';

interface CompanyProgressiveProps {
  companyId: string;
}

export function CompanyProgressive({ companyId }: CompanyProgressiveProps) {
  const [basicCompanyInfo, setBasicCompanyInfo] =
    useState<BasicCompanyInfo | null>(null);
  const [fullCompanyData, setFullCompanyData] =
    useState<CompanyResponse | null>(null);
  const [basicLoading, setBasicLoading] = useState(true);
  const [fullLoading, setFullLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const { clearLoadingToasts, addToast, removeToast } = useToast();

  // Load basic company info first
  useEffect(() => {
    const loadBasicInfo = async () => {
      try {
        console.log('Loading basic company info for:', companyId);
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
        console.log('Loading full company data for:', companyId);
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

  if (error) {
    return (
      <Container section>
        <div className="py-12 text-center">
          <h2 className="mb-4 text-2xl font-bold text-gray-900">
            Fejl ved indlæsning
          </h2>
          <p className="text-gray-600">{error}</p>
        </div>
      </Container>
    );
  }

  if (basicLoading || !basicCompanyInfo) {
    return (
      <Container section>
        <div className="space-y-8">
          <div className="space-y-4">
            <Skeleton className="h-8 w-64" />
            <Skeleton className="h-4 w-48" />
          </div>
          <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
            <div className="space-y-4">
              <Skeleton className="h-6 w-32" />
              <Skeleton className="h-20 w-full" />
            </div>
            <div className="space-y-4">
              <Skeleton className="h-6 w-32" />
              <Skeleton className="h-48 w-full" />
            </div>
          </div>
        </div>
      </Container>
    );
  }

  // No need to create a basic company response anymore

  // Filter out components that are already displayed in the hero section
  const filteredPageBuilder =
    fullCompanyData?.pageBuilder.filter(
      (block) =>
        block._key !== 'company-identity' &&
        block._key !== 'company-map-overview'
    ) || [];

  return (
    <article>
      <CompanyHero
        company={fullCompanyData}
        basicInfo={basicCompanyInfo}
        isLoadingDetails={fullLoading}
      />
      <Container section>
        <PageBuilderProgressive
          pageBlocks={filteredPageBuilder}
          isLoading={fullLoading}
          skeletonCount={6}
        />
      </Container>
    </article>
  );
}

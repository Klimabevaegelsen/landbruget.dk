'use client';

import { useCompanyData } from '@/hooks/useCompanyData';
import { CompanyHero } from './company-hero';
import { Container } from '@/components/layout/container';
import { PageBuilder } from '@/components/pagebuilder/pagebuilder';
import {
  SkeletonKpiGroup,
  SkeletonChart,
  SkeletonTable,
  SkeletonMap,
  SkeletonIteratedSection,
} from '@/components/pagebuilder/pageBlocks/skeleton-blocks';
import { Skeleton } from '@/components/ui/skeleton';

interface CompanyProgressiveProps {
  companyId: string;
}

export function CompanyProgressive({ companyId }: CompanyProgressiveProps) {
  const {
    basicCompanyInfo,
    fullCompanyData,
    basicLoading,
    fullLoading,
    error,
  } = useCompanyData(companyId);

  if (error) {
    return (
      <Container section>
        <div className="py-12 text-center">
          <h2 className="text-foreground mb-4 text-2xl font-bold">
            Fejl ved indlæsning
          </h2>
          <p className="text-muted-foreground">{error}</p>
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
        company={fullCompanyData || undefined}
        basicInfo={basicCompanyInfo}
        isLoadingDetails={fullLoading}
      />
      <Container section>
        {fullLoading ? (
          <div className="space-y-11">
            <SkeletonKpiGroup />
            <SkeletonChart />
            <SkeletonTable />
            <SkeletonMap />
            <SkeletonIteratedSection />
          </div>
        ) : (
          <PageBuilder pageBlocks={filteredPageBuilder} />
        )}
      </Container>
    </article>
  );
}

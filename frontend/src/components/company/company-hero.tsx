'use client';

import { CompanyResponse } from '@/services/data/types';
import type { BasicCompanyInfo } from '@/services/data/company-basic';
import { Container } from '@/components/layout/container';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import { ArrowLeftIcon, ArrowDownIcon } from '@heroicons/react/24/outline';
import { BlockMapChart } from '@/components/pagebuilder/pageBlocks/block-map-chart';
import { useRouter } from 'next/navigation';

interface CompanyHeroProps {
  company?: CompanyResponse;
  basicInfo?: BasicCompanyInfo;
  isLoadingDetails?: boolean;
}

export function CompanyHero({
  company,
  basicInfo,
  isLoadingDetails = false,
}: CompanyHeroProps) {
  const router = useRouter();

  // Find the company identity and map components from pageBuilder
  const companyIdentity = company?.pageBuilder.find(
    (block) => block._key === 'company-identity'
  );
  const companyMap = company?.pageBuilder.find(
    (block) => block._key === 'company-map-overview'
  );

  // Use basic info if available, otherwise fallback to pageBuilder data
  const companyName =
    basicInfo?.company_name ||
    (companyIdentity?._type === 'infoCard'
      ? companyIdentity.items?.find((item) => item.label === 'Navn')?.value
      : undefined) ||
    'Virksomhed';
  const cvrNumber =
    basicInfo?.cvr_number ||
    company?.metadata.company_cvr ||
    (companyIdentity?._type === 'infoCard'
      ? companyIdentity.items?.find((item) => item.label === 'CVR')?.value
      : undefined);
  const address =
    basicInfo?.address ||
    (companyIdentity?._type === 'infoCard'
      ? companyIdentity.items?.find((item) => item.label === 'Adresse')?.value
      : undefined);

  return (
    <Container className="bg-foreground-darker" section>
      <div className="flex flex-col gap-20 md:flex-row">
        <div className="flex w-full flex-col gap-4">
          <div>
            <Button variant="secondary" onClick={() => router.push('/')}>
              <ArrowLeftIcon
                strokeWidth={2.5}
                className="text-primary size-3"
              />
              Tilbage til oversigt
            </Button>
          </div>

          {/* Company Identity Info */}
          <div className="space-y-3">
            <h1 className="text-foreground text-4xl font-bold">
              {companyName}
            </h1>
            <div className="space-y-1">
              <p className="text-foreground text-lg">CVR: {cvrNumber}</p>
              {address && (
                <p className="text-muted-foreground text-base">{address}</p>
              )}
              {basicInfo?.municipality && (
                <p className="text-muted-foreground text-base">
                  {basicInfo.municipality}
                </p>
              )}
            </div>
          </div>

          <div>
            <Button>
              <ArrowDownIcon
                strokeWidth={2.5}
                className="text-primary-foreground size-3"
              />
              Download data (CSV)
            </Button>
          </div>
        </div>

        <div className="relative w-full">
          {/* Company Map */}
          {isLoadingDetails ? (
            <Skeleton className="h-64 w-full rounded" />
          ) : (
            companyMap &&
            companyMap._type === 'mapChart' && (
              <BlockMapChart chart={companyMap} />
            )
          )}
        </div>
      </div>
    </Container>
  );
}

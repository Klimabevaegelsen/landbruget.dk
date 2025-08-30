'use client';

import { CompanyResponse } from '@/services/supabase/types';
import { BasicCompanyInfo } from '@/services/supabase/company-basic';
import { Container } from '../layout/container';
import { Button } from '../ui/button';
import { Skeleton } from '../ui/skeleton';
import { ArrowLeftIcon, ArrowDownIcon } from '@heroicons/react/24/outline';
import { BlockMapChart } from '../pagebuilder/pageBlocks/block-map-chart';
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
                className="size-3 text-green-900"
              />
              Tilbage til oversigt
            </Button>
          </div>

          {/* Company Identity Info */}
          <div className="space-y-3">
            <h1 className="text-4xl font-bold text-gray-800">{companyName}</h1>
            <div className="space-y-1">
              <p className="text-lg text-gray-700">CVR: {cvrNumber}</p>
              {address && <p className="text-base text-gray-600">{address}</p>}
              {basicInfo?.municipality && (
                <p className="text-base text-gray-600">
                  {basicInfo.municipality}
                </p>
              )}
            </div>
          </div>

          <div>
            <Button>
              <ArrowDownIcon strokeWidth={2.5} className="size-3 text-white" />
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

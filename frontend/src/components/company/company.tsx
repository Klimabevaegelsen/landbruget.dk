'use client';

import { useEffect } from 'react';
import { CompanyResponse } from '@/services/data/types';
import { CompanyHero } from './company-hero';
import { Container } from '@/components/layout/container';
import { PageBuilder } from '@/components/pagebuilder/pagebuilder';
import { toast } from 'sonner';

const COMPANY_NAVIGATION_LOADING_TOAST_ID = 'company-navigation-loading';
const COMPANY_DETAILS_LOADING_TOAST_ID = 'company-details-loading';

export function Company({ company }: { company: CompanyResponse }) {
  // Clear any loading toasts when the company page loads
  useEffect(() => {
    toast.dismiss(COMPANY_NAVIGATION_LOADING_TOAST_ID);
    toast.dismiss(COMPANY_DETAILS_LOADING_TOAST_ID);
  }, []);

  // Filter out components that are already displayed in the hero section
  const filteredPageBuilder = company.pageBuilder.filter(
    (block) =>
      block._key !== 'company-identity' && block._key !== 'company-map-overview'
  );

  return (
    <article>
      <CompanyHero company={company} />
      <Container section>
        <PageBuilder pageBlocks={filteredPageBuilder} />
      </Container>
    </article>
  );
}

'use client';

import { ArticleLayout } from '@/components/methodology/article-layout';
import { PfasHero } from '@/components/methodology-pfas/pfas-hero';
import { SectionPfasIntroduction } from '@/components/methodology-pfas/section-pfas-introduction';
import { PfasScrolly } from '@/components/methodology-pfas/scrollytelling/pfas-scrolly';
import { SectionPfasDataSources } from '@/components/methodology-pfas/section-pfas-data-sources';
import { SectionPfasFindings } from '@/components/methodology-pfas/section-pfas-findings';
import { SectionPfasPathways } from '@/components/methodology-pfas/section-pfas-pathways';
import { SectionPfasLimitations } from '@/components/methodology-pfas/section-pfas-limitations';
import { SectionPfasReferences } from '@/components/methodology-pfas/section-pfas-references';

export function PfasPageContent() {
  return (
    <ArticleLayout data-testid="pfas-page-content">
      <PfasHero />
      <SectionPfasIntroduction />
      <PfasScrolly />
      <SectionPfasDataSources />
      <SectionPfasFindings />
      <SectionPfasPathways />
      <SectionPfasLimitations />
      <SectionPfasReferences />
    </ArticleLayout>
  );
}

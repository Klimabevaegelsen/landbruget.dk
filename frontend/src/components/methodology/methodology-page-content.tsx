'use client';

import { ArticleLayout } from '@/components/methodology/article-layout';
import { MethodologyHero } from '@/components/methodology/methodology-hero';
import { SectionDataSources } from '@/components/methodology/section-data-sources';
import { SectionLiteratureReview } from '@/components/methodology/section-literature-review';
import { SectionDisaggregation } from '@/components/methodology/section-disaggregation';
import { SectionResults } from '@/components/methodology/section-results';
import { SectionLimitations } from '@/components/methodology/section-limitations';
import { SectionReferences } from '@/components/methodology/section-references';

export function MethodologyPageContent() {
  return (
    <ArticleLayout>
      <MethodologyHero />
      <SectionDataSources />
      <SectionLiteratureReview />
      <SectionDisaggregation />
      <SectionResults />
      <SectionLimitations />
      <SectionReferences />
    </ArticleLayout>
  );
}

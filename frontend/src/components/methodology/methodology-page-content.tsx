'use client';

import Link from 'next/link';
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

      <footer className="mt-20 pb-8">
        <div className="border-border border-t pt-10">
          <p className="font-display text-foreground text-[20px] font-semibold tracking-tight">
            Tjek din egen adresse
          </p>
          <p className="text-muted-foreground mt-2 max-w-[480px] text-[15px] leading-relaxed">
            Se hvilke pesticider der bruges t&aelig;t p&aring; dit hjem &mdash;
            baseret p&aring; offentlige data fra 92&nbsp;% af alle danske
            marker.
          </p>
          <Link
            href="/pesticidkort"
            data-testid="methodology-cta-pesticidkort"
            className="bg-primary text-primary-foreground mt-5 inline-block rounded-full px-6 py-2.5 text-sm font-medium transition-opacity hover:opacity-90"
          >
            &Aring;bn pesticidkortet &rarr;
          </Link>
        </div>
      </footer>
    </ArticleLayout>
  );
}

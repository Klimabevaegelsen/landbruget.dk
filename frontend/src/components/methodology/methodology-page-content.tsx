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

      <footer className="border-border mt-16 border-t pt-8 pb-8">
        <div className="bg-primary/[0.04] rounded-lg p-5">
          <p className="font-display text-foreground text-lg font-semibold">
            Tjek din egen adresse
          </p>
          <p className="text-muted-foreground mt-1 text-sm leading-relaxed">
            Se hvilke pesticider der bruges tæt på dit hjem — baseret på
            offentlige data fra 92 % af alle danske marker.
          </p>
          <Link
            href="/pesticidkort"
            data-testid="methodology-cta-pesticidkort"
            className="bg-primary text-primary-foreground mt-4 inline-block rounded-full px-5 py-2 text-sm font-medium"
          >
            Åbn pesticidkortet →
          </Link>
        </div>
      </footer>
    </ArticleLayout>
  );
}

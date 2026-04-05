'use client';

import { ArticleLayout } from '@/components/methodology/article-layout';
import { GrundvandHero } from '@/components/methodology-groundwater/grundvand-hero';
import { SectionIntroduction } from '@/components/methodology-groundwater/section-introduction';
import { SectionGrundvandDataSources } from '@/components/methodology-groundwater/section-grundvand-data-sources';
import { GroundwaterScrolly } from '@/components/methodology-groundwater/scrollytelling/groundwater-scrolly';
import { SectionSpatialLinkage } from '@/components/methodology-groundwater/section-spatial-linkage';
import { SectionStatisticalMethod } from '@/components/methodology-groundwater/section-statistical-method';
import { SectionKeyFindings } from '@/components/methodology-groundwater/section-key-findings';
import { SectionGrundvandLimitations } from '@/components/methodology-groundwater/section-grundvand-limitations';
import { SectionEmergingSubstances } from '@/components/methodology-groundwater/section-emerging-substances';
import { SectionGrundvandReferences } from '@/components/methodology-groundwater/section-grundvand-references';

export function GrundvandPageContent() {
  return (
    <ArticleLayout>
      <GrundvandHero />
      <SectionIntroduction />
      <GroundwaterScrolly />
      <SectionGrundvandDataSources />
      <SectionSpatialLinkage />
      <SectionStatisticalMethod />
      <SectionKeyFindings />
      <SectionGrundvandLimitations />
      <SectionEmergingSubstances />
      <SectionGrundvandReferences />
    </ArticleLayout>
  );
}

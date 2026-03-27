'use client';

import React from 'react';
import {
  Accordion,
  AccordionItem,
  AccordionTrigger,
  AccordionContent,
} from '@/components/ui/accordion';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { RiskSummaryCard } from './risk-summary-card';
import { BasicInfoCard } from './basic-info-card';
import { CoordinatesCard } from './coordinates-card';
import { PesticideSummaryCard } from './pesticide-summary-card';
import { PesticideProductsList } from './pesticide-products-list';
import { CategorizedPesticides } from './categorized-pesticides';
import { ChemicalInfoSection } from './chemical-info-section';
import { EnvironmentalCard } from './environmental-card';
import { ProximityCard } from './proximity-card';

interface FieldDetailsContentProps {
  field: FieldAnalysisData;
}

export function FieldDetailsContent({ field }: FieldDetailsContentProps) {
  const hasPesticideData =
    field.total_pesticide_belastning > 0.001 ||
    (field.unique_pesticide_products != null &&
      field.unique_pesticide_products > 0) ||
    (field.total_pesticide_applications != null &&
      field.total_pesticide_applications > 0) ||
    field.pesticides_kg_detail ||
    field.pesticides_liters_detail ||
    field.pesticides_grams_detail ||
    field.pesticides_ml_detail ||
    field.pesticides_tablets_detail ||
    field.pfas_products_detail ||
    field.diquat_products_detail ||
    field.glyphosate_products_detail ||
    field.other_products_detail;

  const defaultSections = ['overblik'];
  if (hasPesticideData) defaultSections.push('pesticider');
  defaultSections.push('miljoe');

  return (
    <Accordion type="multiple" defaultValue={defaultSections}>
      <AccordionItem value="overblik">
        <AccordionTrigger
          className="text-base font-semibold"
          data-testid="overview-accordion-trigger"
        >
          Overblik
        </AccordionTrigger>
        <AccordionContent>
          <div className="space-y-4">
            <RiskSummaryCard field={field} />
            <BasicInfoCard field={field} />
            {field.click_coordinates && (
              <CoordinatesCard coordinates={field.click_coordinates} />
            )}
          </div>
        </AccordionContent>
      </AccordionItem>

      {hasPesticideData && (
        <AccordionItem value="pesticider">
          <AccordionTrigger
            className="text-base font-semibold"
            data-testid="pesticide-accordion-trigger"
          >
            Pesticider
          </AccordionTrigger>
          <AccordionContent>
            <div className="space-y-4">
              <PesticideSummaryCard field={field} />
              <PesticideProductsList field={field} />
              <CategorizedPesticides field={field} />
              <ChemicalInfoSection field={field} />
            </div>
          </AccordionContent>
        </AccordionItem>
      )}

      <AccordionItem value="miljoe">
        <AccordionTrigger
          className="text-base font-semibold"
          data-testid="environmental-accordion-trigger"
        >
          Miljø & Nærhed
        </AccordionTrigger>
        <AccordionContent>
          <div className="space-y-4">
            <EnvironmentalCard field={field} />
            <ProximityCard field={field} />
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}

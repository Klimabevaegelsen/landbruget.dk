'use client';

import React, { useMemo } from 'react';
import { Card } from '@/components/ui/card';
import {
  Accordion,
  AccordionItem,
  AccordionTrigger,
  AccordionContent,
} from '@/components/ui/accordion';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { parsePesticideDetailWithUnit } from './pesticide-utils';
import { CategoryGroup } from './category-group';

interface CategorizedPesticidesProps {
  field: FieldAnalysisData;
}

export function CategorizedPesticides({ field }: CategorizedPesticidesProps) {
  const hasData =
    field.pfas_products_detail ||
    field.diquat_products_detail ||
    field.glyphosate_products_detail ||
    field.other_products_detail;

  if (!hasData) return null;

  const categories = [
    {
      key: 'pfas',
      detail: field.pfas_products_detail,
      count: field.pfas_applications || 0,
      label: 'PFAS-holdige produkter',
      borderColor: 'border-warning/50',
      bgColor: 'bg-warning/5',
      textColor: 'text-warning-foreground',
      itemBg: 'bg-warning/10',
      itemText: 'text-warning-foreground',
      itemSubText: 'text-warning',
    },
    {
      key: 'diquat',
      detail: field.diquat_products_detail,
      count: field.diquat_applications || 0,
      label: 'Diquat-holdige produkter',
      borderColor: 'border-destructive/50',
      bgColor: 'bg-destructive/5',
      textColor: 'text-destructive',
      itemBg: 'bg-destructive/10',
      itemText: 'text-destructive',
      itemSubText: 'text-destructive/80',
    },
    {
      key: 'glyphosate',
      detail: field.glyphosate_products_detail,
      count: field.glyphosate_applications || 0,
      label: 'Glyphosat-holdige produkter',
      borderColor: 'border-warning/50',
      bgColor: 'bg-warning/5',
      textColor: 'text-warning-foreground',
      itemBg: 'bg-warning/10',
      itemText: 'text-warning-foreground',
      itemSubText: 'text-warning',
    },
    {
      key: 'other',
      detail: field.other_products_detail,
      count: field.other_applications || 0,
      label: 'Øvrige produkter',
      borderColor: 'border-border',
      bgColor: 'bg-muted',
      textColor: 'text-foreground',
      itemBg: 'bg-muted',
      itemText: 'text-foreground',
      itemSubText: 'text-muted-foreground',
    },
  ].filter((c) => c.detail);

  const parsedCategories = useMemo(
    () =>
      categories.map((cat) => ({
        ...cat,
        products: parsePesticideDetailWithUnit(cat.detail),
      })),
    [
      field.pfas_products_detail,
      field.diquat_products_detail,
      field.glyphosate_products_detail,
      field.other_products_detail,
    ]
  );

  return (
    <Card className="p-4 lg:p-6">
      <Accordion type="single" collapsible>
        <AccordionItem value="categorized">
          <AccordionTrigger
            className="text-base font-semibold lg:text-lg"
            data-testid="categorized-pesticides-trigger"
          >
            Anvendte pesticider (kategoriseret)
          </AccordionTrigger>
          <AccordionContent>
            <div className="max-h-64 space-y-3 overflow-y-auto lg:max-h-80">
              {parsedCategories.map((cat) => (
                <CategoryGroup
                  key={cat.key}
                  category={cat}
                  products={cat.products}
                />
              ))}
            </div>
          </AccordionContent>
        </AccordionItem>
      </Accordion>
    </Card>
  );
}

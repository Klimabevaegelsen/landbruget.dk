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
      borderColor: 'border-orange-400',
      bgColor: 'bg-orange-50',
      textColor: 'text-orange-700',
      itemBg: 'bg-orange-100',
      itemText: 'text-orange-800',
      itemSubText: 'text-orange-600',
    },
    {
      key: 'diquat',
      detail: field.diquat_products_detail,
      count: field.diquat_applications || 0,
      label: 'Diquat-holdige produkter',
      borderColor: 'border-red-400',
      bgColor: 'bg-red-50',
      textColor: 'text-red-700',
      itemBg: 'bg-red-100',
      itemText: 'text-red-800',
      itemSubText: 'text-red-600',
    },
    {
      key: 'glyphosate',
      detail: field.glyphosate_products_detail,
      count: field.glyphosate_applications || 0,
      label: 'Glyphosat-holdige produkter',
      borderColor: 'border-yellow-400',
      bgColor: 'bg-yellow-50',
      textColor: 'text-yellow-700',
      itemBg: 'bg-yellow-100',
      itemText: 'text-yellow-800',
      itemSubText: 'text-yellow-600',
    },
    {
      key: 'other',
      detail: field.other_products_detail,
      count: field.other_applications || 0,
      label: 'Øvrige produkter',
      borderColor: 'border-gray-400',
      bgColor: 'bg-gray-50',
      textColor: 'text-gray-700',
      itemBg: 'bg-gray-100',
      itemText: 'text-gray-800',
      itemSubText: 'text-gray-600',
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

'use client';

import React, { useMemo } from 'react';
import {
  Accordion,
  AccordionItem,
  AccordionTrigger,
  AccordionContent,
} from '@/components/ui/accordion';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { formatNumber } from '@/lib/formatting';
import { parsePesticideDetail } from './pesticide-utils';

interface PesticideProductsListProps {
  field: FieldAnalysisData;
}

export function PesticideProductsList({ field }: PesticideProductsListProps) {
  const hasProducts =
    field.pesticides_kg_detail ||
    field.pesticides_liters_detail ||
    field.pesticides_grams_detail ||
    field.pesticides_ml_detail ||
    field.pesticides_tablets_detail;

  if (!hasProducts) return null;

  const productGroups = [
    {
      key: 'kg',
      detail: field.pesticides_kg_detail,
      unit: 'kg',
      decimals: 2,
      bg: 'bg-muted',
    },
    {
      key: 'l',
      detail: field.pesticides_liters_detail,
      unit: 'L',
      decimals: 1,
      bg: 'bg-primary/10',
    },
    {
      key: 'g',
      detail: field.pesticides_grams_detail,
      unit: 'g',
      decimals: 0,
      bg: 'bg-muted/50',
    },
    {
      key: 'ml',
      detail: field.pesticides_ml_detail,
      unit: 'ml',
      decimals: 0,
      bg: 'bg-bnbo/10',
    },
    {
      key: 'tablet',
      detail: field.pesticides_tablets_detail,
      unit: 'tabletter',
      decimals: 0,
      bg: 'bg-conventional/10',
    },
  ];

  const allProducts = useMemo(
    () =>
      productGroups.flatMap((group) =>
        parsePesticideDetail(group.detail).map((p) => ({
          ...p,
          unit: group.unit,
          decimals: group.decimals,
          bg: group.bg,
          groupKey: group.key,
        }))
      ),
    [
      field.pesticides_kg_detail,
      field.pesticides_liters_detail,
      field.pesticides_grams_detail,
      field.pesticides_ml_detail,
      field.pesticides_tablets_detail,
    ]
  );

  if (allProducts.length === 0) return null;

  return (
    <Accordion type="single" collapsible>
      <AccordionItem value="products">
        <AccordionTrigger
          className="text-sm font-medium"
          data-testid="pesticide-products-trigger"
        >
          Anvendte produkter ({allProducts.length})
        </AccordionTrigger>
        <AccordionContent>
          <div className="max-h-48 space-y-2 overflow-y-auto lg:max-h-64">
            {allProducts.map((product, index) => (
              <div
                key={`${product.groupKey}-${index}`}
                className={`flex items-center justify-between rounded p-2 text-xs lg:p-3 lg:text-sm ${product.bg}`}
              >
                <span className="truncate font-medium">{product.name}</span>
                <span className="text-muted-foreground ml-2 flex-shrink-0">
                  {formatNumber(product.dosage, product.decimals)}{' '}
                  {product.unit}
                </span>
              </div>
            ))}
          </div>
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}

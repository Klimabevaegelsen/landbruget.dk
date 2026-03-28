'use client';

import { formatNumber } from '@/lib/formatting';
import type { FieldAnalysisData } from './types';
import { parsePesticideDetail } from '@/app/markanalyse/components/shared/field-details/pesticide-utils';

const DETAIL_CONFIGS = [
  {
    key: 'pesticides_kg_detail',
    prefix: 'kg',
    unit: 'kg',
    decimals: 2,
    bg: 'bg-muted',
    text: 'text-foreground',
    sub: 'text-muted-foreground',
  },
  {
    key: 'pesticides_liters_detail',
    prefix: 'l',
    unit: 'L',
    decimals: 1,
    bg: 'bg-primary/10',
    text: 'text-primary',
    sub: 'text-primary/80',
  },
  {
    key: 'pesticides_grams_detail',
    prefix: 'g',
    unit: 'g',
    decimals: 0,
    bg: 'bg-muted/50',
    text: 'text-primary',
    sub: 'text-primary',
  },
  {
    key: 'pesticides_ml_detail',
    prefix: 'ml',
    unit: 'ml',
    decimals: 0,
    bg: 'bg-bnbo/10',
    text: 'text-bnbo',
    sub: 'text-bnbo/80',
  },
  {
    key: 'pesticides_tablets_detail',
    prefix: 'tablet',
    unit: 'tabletter',
    decimals: 0,
    bg: 'bg-conventional/10',
    text: 'text-conventional',
    sub: 'text-conventional/80',
  },
] as const;

export function FieldDetailedProducts({
  fieldData,
}: {
  fieldData: FieldAnalysisData;
}) {
  const hasAny = DETAIL_CONFIGS.some(
    (c) => fieldData[c.key as keyof FieldAnalysisData]
  );
  if (!hasAny) return null;

  return (
    <div className="mt-3">
      <h4 className="text-foreground mb-2 text-sm font-medium">
        Anvendte produkter
      </h4>
      <div className="max-h-32 space-y-2 overflow-y-auto">
        {DETAIL_CONFIGS.map((config) => {
          const detail = fieldData[config.key as keyof FieldAnalysisData] as
            | string
            | undefined;
          if (!detail) return null;
          return parsePesticideDetail(detail).map((product, index) => (
            <div
              key={`${config.prefix}-${index}`}
              className={`${config.bg} flex items-center justify-between rounded p-2 text-xs`}
            >
              <span className={`${config.text} truncate font-medium`}>
                {product.name}
              </span>
              <span className={`${config.sub} ml-2 flex-shrink-0`}>
                {formatNumber(product.dosage, config.decimals)} {config.unit}
              </span>
            </div>
          ));
        })}
      </div>
    </div>
  );
}

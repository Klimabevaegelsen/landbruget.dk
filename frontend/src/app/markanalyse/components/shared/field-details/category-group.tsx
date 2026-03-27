'use client';

import React from 'react';
import { formatNumber } from '@/lib/formatting';
import { getRiskIcon } from './pesticide-utils';
import type { ParsedEnhancedPesticide } from './field-details-types';

export interface CategoryConfig {
  key: string;
  count: number;
  label: string;
  borderColor: string;
  bgColor: string;
  textColor: string;
  itemBg: string;
  itemText: string;
  itemSubText: string;
}

interface CategoryGroupProps {
  category: CategoryConfig;
  products: ParsedEnhancedPesticide[];
}

export function CategoryGroup({ category, products }: CategoryGroupProps) {
  return (
    <div className={`rounded-lg border p-3 ${category.bgColor}`}>
      <div className={`mb-2 text-sm font-medium ${category.textColor}`}>
        {category.label} ({category.count})
      </div>
      <div className="space-y-2">
        {products.map((product, index) => {
          const riskIcon = getRiskIcon(
            product.healthRisk,
            product.envRisk,
            product.signalWord
          );
          return (
            <div
              key={`${category.key}-${index}`}
              className={`rounded border-l-4 p-2 ${category.borderColor} ${category.itemBg}`}
            >
              <div className="flex items-start justify-between gap-2">
                <div className="flex-1">
                  <div className={`font-medium ${category.itemText}`}>
                    {product.name}
                  </div>
                  <div className={`text-sm ${category.itemSubText}`}>
                    {formatNumber(product.dosage, 2)} {product.unit}
                  </div>
                </div>
                {riskIcon && (
                  <div
                    className={`flex items-center gap-1 rounded px-2 py-1 ${riskIcon.bgColor}`}
                    title={`${riskIcon.ghs} - ${riskIcon.level}`}
                  >
                    <riskIcon.Icon className={`h-4 w-4 ${riskIcon.color}`} />
                    <span className={`text-xs font-medium ${riskIcon.color}`}>
                      {riskIcon.level}
                    </span>
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

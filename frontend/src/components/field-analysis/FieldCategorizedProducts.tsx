'use client';

import { formatNumber } from '@/lib/formatting';
import type { FieldAnalysisData } from './types';
import {
  parsePesticideDetailWithUnit,
  classifyRisk,
} from '@/app/markanalyse/components/shared/field-details/pesticide-utils';
import { getGHSRiskIcon } from './ghs-risk-icons';

const CATEGORY_CONFIGS = [
  {
    key: 'pfas',
    detailField: 'pfas_products_detail',
    countField: 'pfas_applications',
    emoji: '🚨',
    label: 'PFAS-holdige produkter',
    border: 'border-warning/50',
    bg: 'bg-warning/5',
    text: 'text-warning-foreground',
    sub: 'text-warning',
    showRisk: true,
  },
  {
    key: 'diquat',
    detailField: 'diquat_products_detail',
    countField: 'diquat_applications',
    emoji: '⚠️',
    label: 'Diquat-holdige produkter',
    border: 'border-destructive/50',
    bg: 'bg-destructive/5',
    text: 'text-destructive',
    sub: 'text-destructive',
    showRisk: false,
  },
  {
    key: 'glyphosate',
    detailField: 'glyphosate_products_detail',
    countField: 'glyphosate_applications',
    emoji: '🌾',
    label: 'Glyphosat-holdige produkter',
    border: 'border-warning/50',
    bg: 'bg-warning/5',
    text: 'text-warning-foreground',
    sub: 'text-warning',
    showRisk: false,
  },
  {
    key: 'other',
    detailField: 'other_products_detail',
    countField: 'other_applications',
    emoji: '🧪',
    label: 'Øvrige produkter',
    border: 'border-border',
    bg: 'bg-muted',
    text: 'text-foreground',
    sub: 'text-muted-foreground',
    showRisk: false,
  },
] as const;

export function FieldCategorizedProducts({
  fieldData,
}: {
  fieldData: FieldAnalysisData;
}) {
  const hasAny = CATEGORY_CONFIGS.some(
    (c) => fieldData[c.detailField as keyof FieldAnalysisData]
  );
  if (!hasAny) return null;

  return (
    <div className="mt-3">
      <h4 className="text-foreground mb-2 text-sm font-medium">
        Anvendte pesticider (kategoriseret)
      </h4>
      <div className="max-h-48 space-y-2 overflow-y-auto">
        {CATEGORY_CONFIGS.map((cat) => {
          const detail = fieldData[
            cat.detailField as keyof FieldAnalysisData
          ] as string | undefined;
          if (!detail) return null;
          const count =
            (fieldData[cat.countField as keyof FieldAnalysisData] as
              | number
              | undefined) || 0;
          const products = parsePesticideDetailWithUnit(detail);
          return (
            <div key={cat.key} className="mb-2">
              <div className={`${cat.sub} mb-1 text-xs font-medium`}>
                {cat.emoji} {cat.label} ({count})
              </div>
              {products.map((product, index) => {
                const riskIcon = cat.showRisk
                  ? getGHSRiskIcon(
                      classifyRisk(
                        product.healthRisk,
                        product.envRisk,
                        product.signalWord
                      )
                    )
                  : null;
                return (
                  <div
                    key={`${cat.key}-${index}`}
                    className={`${cat.border} ${cat.bg} mb-1 rounded border-l-4 p-2`}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="flex-1">
                        <div className={`${cat.text} font-medium`}>
                          {product.name}
                        </div>
                        <div className={`${cat.sub} text-sm`}>
                          {formatNumber(product.dosage, 2)} {product.unit}
                        </div>
                      </div>
                      {riskIcon && (
                        <div
                          className={`flex items-center gap-1 rounded px-2 py-1 ${riskIcon.bgColor}`}
                          title={`${riskIcon.ghs} - ${riskIcon.level}`}
                        >
                          <riskIcon.Icon
                            className={`h-4 w-4 ${riskIcon.color}`}
                          />
                          <span
                            className={`text-xs font-medium ${riskIcon.color}`}
                          >
                            {riskIcon.level}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
    </div>
  );
}

'use client';

import { formatNumber } from '@/lib/formatting';
import type { FieldAnalysisData } from './types';
import type { PesticideRiskLevel } from '@/app/markanalyse/components/shared/field-details/field-details-types';
import { FieldDosageInfo } from './FieldDosageInfo';
import { FieldDetailedProducts } from './FieldDetailedProducts';
import { FieldCategorizedProducts } from './FieldCategorizedProducts';
import { FieldChemicalInfo } from './FieldChemicalInfo';

interface FieldPesticideInfoProps {
  fieldData: FieldAnalysisData;
  riskLevel: PesticideRiskLevel;
}

export function FieldPesticideInfo({
  fieldData,
  riskLevel,
}: FieldPesticideInfoProps) {
  return (
    <div className="mb-4">
      <h3 className="text-foreground mb-2 text-base font-semibold">
        Pesticidforbrug
      </h3>
      <PesticideSummary fieldData={fieldData} riskLevel={riskLevel} />
      <ProductsSummary fieldData={fieldData} />
      <FieldDosageInfo fieldData={fieldData} />
      <FieldDetailedProducts fieldData={fieldData} />
      <FieldCategorizedProducts fieldData={fieldData} />
      <FieldChemicalInfo fieldData={fieldData} />
    </div>
  );
}

function PesticideSummary({
  fieldData,
  riskLevel,
}: {
  fieldData: FieldAnalysisData;
  riskLevel: PesticideRiskLevel;
}) {
  return (
    <div className="bg-muted mb-2 rounded-lg p-3">
      <div className="mb-1 flex items-center justify-between">
        <span className="text-sm font-medium">Samlet belastning</span>
        <span className={`font-bold ${riskLevel.color}`}>
          {formatNumber(fieldData.total_pesticide_belastning)}
        </span>
      </div>
      <div className="flex items-center justify-between">
        <span className="text-muted-foreground text-xs">Risikoniveau</span>
        <span className={`text-xs font-medium ${riskLevel.color}`}>
          {riskLevel.level}
        </span>
      </div>
    </div>
  );
}

function ProductsSummary({ fieldData }: { fieldData: FieldAnalysisData }) {
  if (
    !fieldData.unique_pesticide_products ||
    fieldData.unique_pesticide_products <= 0
  )
    return null;

  return (
    <div className="bg-primary/10 mb-2 rounded-lg p-3">
      <div className="mb-1 flex items-center justify-between">
        <span className="text-primary text-sm font-medium">
          Produkter anvendt
        </span>
        <span className="text-primary font-bold">
          {fieldData.unique_pesticide_products}
        </span>
      </div>
      {fieldData.total_pesticide_applications &&
        fieldData.total_pesticide_applications > 0 && (
          <div className="flex items-center justify-between">
            <span className="text-primary/80 text-xs">Total allokeringer</span>
            <span className="text-primary text-xs font-medium">
              {fieldData.total_pesticide_applications}
            </span>
          </div>
        )}
    </div>
  );
}

'use client';

import { formatNumber } from '@/lib/formatting';
import { TestTube, Leaf, AlertTriangle } from 'lucide-react';
import type { FieldAnalysisData } from './types';
import { ChemicalCard } from './ChemicalCard';

export function FieldChemicalInfo({
  fieldData,
}: {
  fieldData: FieldAnalysisData;
}) {
  return (
    <div className="space-y-2">
      {fieldData.pfas_applications && fieldData.pfas_applications > 0 && (
        <ChemicalCard
          icon={<TestTube className="mr-1 h-4 w-4" />}
          label="PFAS"
          count={fieldData.pfas_applications}
          border="border-warning/30"
          bg="bg-warning/5 dark:bg-warning/10"
          textColor="text-warning-foreground"
          metricColor="text-warning-foreground/80"
          metrics={[
            fieldData.total_pfas_active_ingredient_kg &&
            fieldData.total_pfas_active_ingredient_kg > 0
              ? {
                  label: 'Aktivstof:',
                  value: `${formatNumber(fieldData.total_pfas_active_ingredient_kg, 3)} kg`,
                }
              : null,
            fieldData.total_pfas_belastning &&
            fieldData.total_pfas_belastning > 0
              ? {
                  label: 'Belastning:',
                  value: formatNumber(fieldData.total_pfas_belastning) ?? '',
                }
              : null,
          ]}
        />
      )}

      {fieldData.diquat_applications && fieldData.diquat_applications > 0 && (
        <ChemicalCard
          label="💧 Diquat"
          count={fieldData.diquat_applications}
          border="border-info/30"
          bg="bg-info/5 dark:bg-info/10"
          textColor="text-info"
          metricColor="text-info/80"
          metrics={[
            fieldData.total_diquat_belastning &&
            fieldData.total_diquat_belastning > 0
              ? {
                  label: 'Belastning:',
                  value: formatNumber(fieldData.total_diquat_belastning) ?? '',
                }
              : null,
          ]}
        />
      )}

      {fieldData.glyphosate_applications &&
        fieldData.glyphosate_applications > 0 && (
          <ChemicalCard
            icon={<Leaf className="mr-1 h-4 w-4" />}
            label="Glyphosate"
            count={fieldData.glyphosate_applications}
            bg="bg-muted/50"
            textColor="text-primary"
            metricColor="text-primary/80"
            metrics={[
              fieldData.total_glyphosate_active_ingredient_kg &&
              fieldData.total_glyphosate_active_ingredient_kg > 0
                ? {
                    label: 'Aktivstof:',
                    value: `${formatNumber(fieldData.total_glyphosate_active_ingredient_kg, 3)} kg`,
                  }
                : null,
              fieldData.total_glyphosate_belastning &&
              fieldData.total_glyphosate_belastning > 0
                ? {
                    label: 'Belastning:',
                    value:
                      formatNumber(fieldData.total_glyphosate_belastning) ?? '',
                  }
                : null,
            ]}
          />
        )}

      {fieldData.is_partial_coverage && (
        <div className="bg-conventional/10 flex items-center space-x-2 rounded-lg p-2">
          <AlertTriangle className="text-conventional h-4 w-4" />
          <span className="text-conventional/80 text-xs">
            Delvis markdækning
          </span>
        </div>
      )}
    </div>
  );
}

'use client';

import React from 'react';
import { TestTube, Leaf } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { FieldAnalysisData } from '@/components/field-analysis/types';
import { formatNumber } from '@/lib/formatting';

interface ChemicalConfig {
  key: string;
  label: string;
  icon?: LucideIcon;
  applications: number;
  activeIngredient?: number | null;
  belastning?: number | null;
  borderColor: string;
  bgColor: string;
  textColor: string;
  metricTextClass: string;
}

interface ChemicalInfoSectionProps {
  field: FieldAnalysisData;
}

export function ChemicalInfoSection({ field }: ChemicalInfoSectionProps) {
  const chemicals: ChemicalConfig[] = [
    {
      key: 'pfas',
      label: 'PFAS',
      icon: TestTube,
      applications: field.pfas_applications ?? 0,
      activeIngredient: field.total_pfas_active_ingredient_kg,
      belastning: field.total_pfas_belastning,
      borderColor: 'border-amber-200 dark:border-amber-800',
      bgColor: 'bg-amber-50 dark:bg-amber-950/20',
      textColor: 'text-amber-700 dark:text-amber-300',
      metricTextClass: 'text-amber-700/80 dark:text-amber-300/80',
    },
    {
      key: 'diquat',
      label: 'Diquat',
      applications: field.diquat_applications ?? 0,
      activeIngredient: undefined,
      belastning: field.total_diquat_belastning,
      borderColor: 'border-purple-200 dark:border-purple-800',
      bgColor: 'bg-purple-50 dark:bg-purple-950/20',
      textColor: 'text-purple-700 dark:text-purple-300',
      metricTextClass: 'text-purple-700/80 dark:text-purple-300/80',
    },
    {
      key: 'glyphosate',
      label: 'Glyphosate',
      icon: Leaf,
      applications: field.glyphosate_applications ?? 0,
      activeIngredient: field.total_glyphosate_active_ingredient_kg,
      belastning: field.total_glyphosate_belastning,
      borderColor: 'border-blue-200 dark:border-blue-800',
      bgColor: 'bg-blue-50 dark:bg-blue-950/20',
      textColor: 'text-blue-700 dark:text-blue-300',
      metricTextClass: 'text-blue-700/80 dark:text-blue-300/80',
    },
  ].filter((c) => c.applications > 0);

  if (chemicals.length === 0) return null;

  return (
    <div className="space-y-2 lg:space-y-3">
      {chemicals.map((chem) => (
        <div
          key={chem.key}
          className={`rounded-lg border p-2 lg:p-3 ${chem.borderColor} ${chem.bgColor}`}
        >
          <div className="mb-1 flex items-center justify-between">
            <span
              className={`flex items-center text-sm font-medium lg:text-base ${chem.textColor}`}
            >
              {chem.icon && <chem.icon className="mr-1 h-4 w-4" />}
              {chem.label}
            </span>
            <span
              className={`text-sm font-bold lg:text-base ${chem.textColor}`}
            >
              {chem.applications} produkter
            </span>
          </div>
          <ChemicalMetrics
            activeIngredient={chem.activeIngredient}
            belastning={chem.belastning}
            textClass={chem.metricTextClass}
          />
        </div>
      ))}
    </div>
  );
}

function ChemicalMetrics({
  activeIngredient,
  belastning,
  textClass,
}: {
  activeIngredient?: number | null;
  belastning?: number | null;
  textClass: string;
}) {
  if (
    (activeIngredient == null || activeIngredient <= 0) &&
    (belastning == null || belastning <= 0)
  ) {
    return null;
  }

  return (
    <div className={`space-y-1 text-xs lg:text-sm ${textClass}`}>
      {activeIngredient != null && activeIngredient > 0 && (
        <div className="flex justify-between">
          <span>Aktivstof:</span>
          <span className="font-medium">
            {formatNumber(activeIngredient, 3)} kg
          </span>
        </div>
      )}
      {belastning != null && belastning > 0 && (
        <div className="flex justify-between">
          <span>Belastning:</span>
          <span className="font-medium">{formatNumber(belastning)}</span>
        </div>
      )}
    </div>
  );
}

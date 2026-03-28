import type { ComponentType } from 'react';
import {
  GHS06_SkullCrossbones,
  GHS07_ExclamationMark,
  GHS08_HealthHazard,
  GHS09_Environmental,
  GHS05_Corrosive,
} from './GHSPictograms';
import type { RiskClassification } from '@/app/markanalyse/components/shared/field-details/field-details-types';

export interface GHSRiskResult extends RiskClassification {
  Icon: ComponentType<{ className?: string }>;
}

const GHS_ICON_MAP: Record<string, ComponentType<{ className?: string }>> = {
  GHS06: GHS06_SkullCrossbones,
  GHS05: GHS05_Corrosive,
  GHS08: GHS08_HealthHazard,
  GHS07: GHS07_ExclamationMark,
  GHS09: GHS09_Environmental,
  'SIGNAL-Fare': GHS06_SkullCrossbones,
  'SIGNAL-Advarsel': GHS07_ExclamationMark,
};

export function getGHSRiskIcon(
  classification: RiskClassification | null
): GHSRiskResult | null {
  if (!classification) return null;
  const key =
    classification.ghs === 'SIGNAL'
      ? `SIGNAL-${classification.level}`
      : classification.ghs;
  const Icon = GHS_ICON_MAP[key] ?? GHS07_ExclamationMark;
  return { ...classification, Icon };
}

import {
  Skull,
  ShieldAlert,
  AlertTriangle,
  TriangleAlert,
  Trees,
  OctagonAlert,
} from 'lucide-react';
import type { RiskClassification, RiskIconResult } from './field-details-types';

/**
 * Classify pesticide risk based on health/env risk strings and signal word.
 * Returns icon-agnostic classification data (colors, level, GHS code).
 * Use this when you need the classification without a specific icon set.
 */
export function classifyRisk(
  healthRisk?: string,
  envRisk?: string,
  signalWord?: string
): RiskClassification | null {
  if (healthRisk?.includes('Meget giftig') || healthRisk?.includes('Tx')) {
    return {
      color: 'text-destructive',
      bgColor: 'bg-destructive/10',
      level: 'Meget giftig',
      ghs: 'GHS06',
    };
  }
  if (healthRisk?.includes('Giftig') || healthRisk?.includes('T')) {
    return {
      color: 'text-destructive',
      bgColor: 'bg-destructive/5',
      level: 'Giftig',
      ghs: 'GHS06',
    };
  }
  if (healthRisk?.includes('Ætsende') || healthRisk?.includes('C')) {
    return {
      color: 'text-destructive',
      bgColor: 'bg-destructive/5',
      level: 'Ætsende',
      ghs: 'GHS05',
    };
  }
  if (healthRisk?.includes('Sundhedsskadelig') || healthRisk?.includes('Xn')) {
    return {
      color: 'text-warning',
      bgColor: 'bg-warning/5',
      level: 'Sundhedsskadelig',
      ghs: 'GHS08',
    };
  }
  if (healthRisk?.includes('Lokalirriterende') || healthRisk?.includes('Xi')) {
    return {
      color: 'text-warning',
      bgColor: 'bg-warning/5',
      level: 'Lokalirriterende',
      ghs: 'GHS07',
    };
  }
  if (envRisk?.includes('Miljøfarlig') || envRisk?.includes('N')) {
    return {
      color: 'text-primary/80',
      bgColor: 'bg-primary/5',
      level: 'Miljøfarlig',
      ghs: 'GHS09',
    };
  }
  if (signalWord === 'Fare') {
    return {
      color: 'text-destructive',
      bgColor: 'bg-destructive/5',
      level: 'Fare',
      ghs: 'SIGNAL',
    };
  }
  if (signalWord === 'Advarsel') {
    return {
      color: 'text-warning',
      bgColor: 'bg-warning/5',
      level: 'Advarsel',
      ghs: 'SIGNAL',
    };
  }
  return null;
}

/** Lucide icon mapping keyed by GHS code + level */
const LUCIDE_ICON_MAP: Record<string, RiskIconResult['Icon']> = {
  GHS06: Skull,
  GHS05: ShieldAlert,
  GHS08: AlertTriangle,
  GHS07: TriangleAlert,
  GHS09: Trees,
  'SIGNAL-Fare': OctagonAlert,
  'SIGNAL-Advarsel': TriangleAlert,
};

function lucideIconForClassification(
  c: RiskClassification
): RiskIconResult['Icon'] {
  if (c.ghs === 'SIGNAL') {
    return LUCIDE_ICON_MAP[`SIGNAL-${c.level}`] ?? TriangleAlert;
  }
  return LUCIDE_ICON_MAP[c.ghs] ?? TriangleAlert;
}

/**
 * Classify risk and attach a Lucide icon.
 * For GHS pictogram icons, use `classifyRisk` directly instead.
 */
export function getRiskIcon(
  healthRisk?: string,
  envRisk?: string,
  signalWord?: string
): RiskIconResult | null {
  const classification = classifyRisk(healthRisk, envRisk, signalWord);
  if (!classification) return null;
  return {
    ...classification,
    Icon: lucideIconForClassification(classification),
  };
}

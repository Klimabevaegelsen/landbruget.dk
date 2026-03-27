import {
  Skull,
  ShieldAlert,
  AlertTriangle,
  TriangleAlert,
  Trees,
  OctagonAlert,
} from 'lucide-react';
import type { RiskIconResult, PesticideRiskLevel } from './field-details-types';

export function getRiskIcon(
  healthRisk?: string,
  envRisk?: string,
  signalWord?: string
): RiskIconResult | null {
  if (healthRisk?.includes('Meget giftig') || healthRisk?.includes('Tx')) {
    return {
      Icon: Skull,
      color: 'text-red-700',
      bgColor: 'bg-red-100',
      level: 'Meget giftig',
      ghs: 'GHS06',
    };
  }
  if (healthRisk?.includes('Giftig') || healthRisk?.includes('T')) {
    return {
      Icon: Skull,
      color: 'text-red-600',
      bgColor: 'bg-red-50',
      level: 'Giftig',
      ghs: 'GHS06',
    };
  }
  if (healthRisk?.includes('Ætsende') || healthRisk?.includes('C')) {
    return {
      Icon: ShieldAlert,
      color: 'text-red-600',
      bgColor: 'bg-red-50',
      level: 'Ætsende',
      ghs: 'GHS05',
    };
  }
  if (healthRisk?.includes('Sundhedsskadelig') || healthRisk?.includes('Xn')) {
    return {
      Icon: AlertTriangle,
      color: 'text-orange-600',
      bgColor: 'bg-orange-50',
      level: 'Sundhedsskadelig',
      ghs: 'GHS08',
    };
  }
  if (healthRisk?.includes('Lokalirriterende') || healthRisk?.includes('Xi')) {
    return {
      Icon: TriangleAlert,
      color: 'text-yellow-600',
      bgColor: 'bg-yellow-50',
      level: 'Lokalirriterende',
      ghs: 'GHS07',
    };
  }
  if (envRisk?.includes('Miljøfarlig') || envRisk?.includes('N')) {
    return {
      Icon: Trees,
      color: 'text-green-700',
      bgColor: 'bg-green-50',
      level: 'Miljøfarlig',
      ghs: 'GHS09',
    };
  }
  if (signalWord === 'Fare') {
    return {
      Icon: OctagonAlert,
      color: 'text-red-600',
      bgColor: 'bg-red-50',
      level: 'Fare',
      ghs: 'SIGNAL',
    };
  }
  if (signalWord === 'Advarsel') {
    return {
      Icon: TriangleAlert,
      color: 'text-yellow-600',
      bgColor: 'bg-yellow-50',
      level: 'Advarsel',
      ghs: 'SIGNAL',
    };
  }
  return null;
}

export function getPesticideRiskLevel(belastning: number): PesticideRiskLevel {
  if (belastning === 0)
    return {
      level: 'Ingen',
      color: 'text-green-600',
      description: 'Ingen registreret pesticidanvendelse',
      variant: 'secondary',
    };
  if (belastning < 10)
    return {
      level: 'Lav',
      color: 'text-conventional',
      description: 'Lav pesticidbelastning',
      variant: 'default',
    };
  if (belastning < 50)
    return {
      level: 'Moderat',
      color: 'text-conventional',
      description: 'Moderat pesticidbelastning',
      variant: 'secondary',
    };
  return {
    level: 'Høj',
    color: 'text-destructive',
    description: 'Høj pesticidbelastning',
    variant: 'destructive',
  };
}

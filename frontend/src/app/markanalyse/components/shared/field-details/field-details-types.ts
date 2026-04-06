import type { LucideIcon } from 'lucide-react';

export interface ParsedPesticide {
  name: string;
  dosage: number;
}

export interface ParsedEnhancedPesticide {
  name: string;
  dosage: number;
  unit: string;
  healthRisk?: string;
  envRisk?: string;
  signalWord?: string;
  productGroup?: string;
  burden?: number;
}

/** Icon-agnostic risk classification data */
export interface RiskClassification {
  color: string;
  bgColor: string;
  level: string;
  ghs: string;
}

/** Risk classification with a Lucide icon attached */
export interface RiskIconResult extends RiskClassification {
  Icon: LucideIcon;
}

export interface PesticideRiskLevel {
  level: string;
  color: string;
  description: string;
  variant: 'default' | 'secondary' | 'destructive';
}

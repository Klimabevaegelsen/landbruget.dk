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
}

export interface RiskIconResult {
  Icon: LucideIcon;
  color: string;
  bgColor: string;
  level: string;
  ghs: string;
}

export interface PesticideRiskLevel {
  level: string;
  color: string;
  description: string;
  variant: 'default' | 'secondary' | 'destructive';
}

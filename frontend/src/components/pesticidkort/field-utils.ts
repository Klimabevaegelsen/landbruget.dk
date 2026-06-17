import type { PesticidkortColorKey } from '@/components/pesticidkort/color-theme';

/**
 * Scale max for the burden histogram track.
 * Kartofler peak at ~10.2 B/ha (Bekæmpelsesmiddelstatistik).
 */
const SCALE_MAX = 12;

/** National average PBI: 2.15 B/ha (Bekæmpelsesmiddelstatistik). */
const NATIONAL_AVG = 2.15;

export const BURDEN_THRESHOLDS = {
  midLow: 0.5,
  mid: 2,
  midHigh: 4,
  high: 8,
} as const;

export type BurdenLevelKey =
  | 'none'
  | 'low'
  | 'midLow'
  | 'mid'
  | 'midHigh'
  | 'high';

export interface BurdenLevel {
  key: BurdenLevelKey;
  label: string;
  colorKey: PesticidkortColorKey;
}

export const BURDEN_LEVELS = {
  none: {
    key: 'none',
    label: 'Ingen pesticider',
    colorKey: 'burdenNone',
  },
  low: {
    key: 'low',
    label: 'Meget lav belastning',
    colorKey: 'burdenLow',
  },
  midLow: {
    key: 'midLow',
    label: 'Lav belastning',
    colorKey: 'burdenMidLow',
  },
  mid: {
    key: 'mid',
    label: 'Middel belastning',
    colorKey: 'burdenMid',
  },
  midHigh: {
    key: 'midHigh',
    label: 'Forhøjet belastning',
    colorKey: 'burdenMidHigh',
  },
  high: {
    key: 'high',
    label: 'Høj belastning',
    colorKey: 'burdenHigh',
  },
} as const satisfies Record<BurdenLevelKey, BurdenLevel>;

export const BURDEN_COLOR_STOPS = [
  { min: 0, level: BURDEN_LEVELS.low },
  { min: BURDEN_THRESHOLDS.midLow, level: BURDEN_LEVELS.midLow },
  { min: BURDEN_THRESHOLDS.mid, level: BURDEN_LEVELS.mid },
  { min: BURDEN_THRESHOLDS.midHigh, level: BURDEN_LEVELS.midHigh },
  { min: BURDEN_THRESHOLDS.high, level: BURDEN_LEVELS.high },
] as const;

export const NATIONAL_AVG_PERCENT = (NATIONAL_AVG / SCALE_MAX) * 100;

export function burdenToPercent(belastning: number): number {
  return Math.min((belastning / SCALE_MAX) * 100, 100);
}

export function formatBurden(belastning: number): string {
  return `${belastning.toFixed(1).replace('.', ',')} B/ha`;
}

export function getBurdenLevel(belastning: number): BurdenLevel {
  if (belastning <= 0) return BURDEN_LEVELS.none;
  if (belastning < BURDEN_THRESHOLDS.midLow) return BURDEN_LEVELS.low;
  if (belastning < BURDEN_THRESHOLDS.mid) return BURDEN_LEVELS.midLow;
  if (belastning < BURDEN_THRESHOLDS.midHigh) return BURDEN_LEVELS.mid;
  if (belastning < BURDEN_THRESHOLDS.high) return BURDEN_LEVELS.midHigh;
  return BURDEN_LEVELS.high;
}

export function burdenHeadline(belastning: number): string {
  const level = getBurdenLevel(belastning);
  if (level.key === 'none') return level.label;
  return `${level.label} · ${formatBurden(belastning)}`;
}

export type {
  ParsedEnhancedPesticide,
  RiskIconResult,
} from '@/app/markanalyse/components/shared/field-details/field-details-types';

export { parsePesticideDetailWithUnit } from '@/app/markanalyse/components/shared/field-details/pesticide-parsers';

export { getRiskIcon } from '@/app/markanalyse/components/shared/field-details/pesticide-risk';

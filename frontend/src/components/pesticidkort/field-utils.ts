const CROP_EMOJI: Record<string, string> = {
  Hvede: '🌾',
  Byg: '🌾',
  Havre: '🌾',
  Rug: '🌾',
  Triticale: '🌾',
  Majs: '🌽',
  Raps: '🌻',
  Kartofler: '🥔',
  Sukkerroer: '🟤',
  Græs: '🌿',
  Kløver: '🍀',
};

export function getCropEmoji(crop: string): string {
  for (const [key, emoji] of Object.entries(CROP_EMOJI)) {
    if (crop.toLowerCase().includes(key.toLowerCase())) return emoji;
  }
  return '🌱';
}

/**
 * Scale max for the burden histogram track.
 * Kartofler peak at ~10.2 B/ha (Bekæmpelsesmiddelstatistik 2023).
 */
const SCALE_MAX = 12;

/** National average PBI: 2.15 B/ha (Bekæmpelsesmiddelstatistik 2023). */
const NATIONAL_AVG = 2.15;

export const NATIONAL_AVG_PERCENT = (NATIONAL_AVG / SCALE_MAX) * 100;

export function burdenToPercent(belastning: number): number {
  return Math.min((belastning / SCALE_MAX) * 100, 100);
}

export function formatBurden(belastning: number): string {
  return `${belastning.toFixed(1).replace('.', ',')} B/ha`;
}

export type {
  ParsedEnhancedPesticide,
  RiskIconResult,
} from '@/app/markanalyse/components/shared/field-details/field-details-types';

export { parsePesticideDetailWithUnit } from '@/app/markanalyse/components/shared/field-details/pesticide-parsers';

export { getRiskIcon } from '@/app/markanalyse/components/shared/field-details/pesticide-risk';

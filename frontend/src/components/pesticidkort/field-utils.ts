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

interface ParsedProduct {
  name: string;
  dose: string;
  count: string;
  flags: string[];
}

export function parseProductString(raw: string): ParsedProduct[] {
  return raw
    .split(';')
    .map((segment) => segment.trim())
    .filter(Boolean)
    .map((segment) => {
      const parts = segment.split(':').map((p) => p.trim());
      const name = parts[0] || '';
      const dose = parts[1] || '';
      const count = parts[2] || '';
      const flags = parts.slice(3).filter(Boolean);
      return { name, dose, count, flags };
    })
    .filter((p) => p.name);
}

export function formatProduct(p: ParsedProduct): string {
  const parts = [p.name];
  if (p.dose) parts.push(`${p.dose.replace('.', ',')} l/ha`);
  if (p.count) parts.push(`${p.count} beh.`);
  if (p.flags.length > 0) parts.push(p.flags.join(', '));
  return parts.join(' — ');
}

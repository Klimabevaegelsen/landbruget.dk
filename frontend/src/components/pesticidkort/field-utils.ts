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
 * Map a field's belastning (B/ha) to a 1–10 decile for the burden bar.
 *
 * Breakpoints based on Miljøstyrelsen crop burden data (2023):
 *   Græs 0.03 → decile 1, Vårsæd 0.77 → 3, Vintersæd 2.74 → 5,
 *   Roer 4.13 → 7, Kartofler 10.2 → 10
 */
export function getBurdenDecile(belastning: number): number {
  const breaks = [0.1, 0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0];
  for (let i = 0; i < breaks.length; i++) {
    if (belastning <= breaks[i]) return i + 1;
  }
  return 10;
}

export function getBarColor(decile: number): string {
  if (decile <= 3) return 'oklch(60% 0.15 142)';
  if (decile <= 6) return 'oklch(70% 0.14 85)';
  return 'oklch(60% 0.18 25)';
}

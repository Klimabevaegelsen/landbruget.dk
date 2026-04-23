'use client';

const COLOR_VARS = {
  burdenLow: '--pesticidkort-color-burden-low',
  burdenMidLow: '--pesticidkort-color-burden-mid-low',
  burdenMid: '--pesticidkort-color-burden-mid',
  burdenMidHigh: '--pesticidkort-color-burden-mid-high',
  burdenHigh: '--pesticidkort-color-burden-high',
  burdenNone: '--pesticidkort-color-burden-none',
  pfas: '--pesticidkort-color-pfas',
  glyphosate: '--pesticidkort-color-glyphosate',
  diquat: '--pesticidkort-color-diquat',
  ring: '--pesticidkort-color-ring',
  highlight: '--pesticidkort-color-highlight',
  fieldOutline: '--pesticidkort-color-field-outline',
  schoolFill: '--pesticidkort-color-school-fill',
  schoolOutline: '--pesticidkort-color-school-outline',
  storyMuted: '--pesticidkort-color-story-muted',
} as const;

const FALLBACKS = {
  burdenLow: '#22c55e',
  burdenMidLow: '#84cc16',
  burdenMid: '#eab308',
  burdenMidHigh: '#f97316',
  burdenHigh: '#dc2626',
  burdenNone: '#d1d5db',
  pfas: '#9333ea',
  glyphosate: '#0891b2',
  diquat: '#db2777',
  ring: '#3f6ab3',
  highlight: '#3a9d5d',
  fieldOutline: '#5f6b80',
  schoolFill: '#ec4899',
  schoolOutline: '#be185d',
  storyMuted: '#dfe6f2',
} as const;

export type PesticidkortColorKey = keyof typeof COLOR_VARS;

export function pesticidkortCssColor(key: PesticidkortColorKey): string {
  return `var(${COLOR_VARS[key]})`;
}

export function resolvePesticidkortColor(key: PesticidkortColorKey): string {
  if (typeof document === 'undefined') {
    return FALLBACKS[key];
  }

  const value = getComputedStyle(document.documentElement)
    .getPropertyValue(COLOR_VARS[key])
    .trim();

  return value || FALLBACKS[key];
}

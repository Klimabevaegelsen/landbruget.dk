/* oxlint-disable landbruget/require-test-coverage */
import type { PesticideGrade } from '@/components/pesticidkort/types';

const GRADE_TEXT_COLORS: Record<PesticideGrade, string> = {
  UNDER_AVG: 'text-[oklch(60%_0.18_145)]',
  TOP_50: 'text-[oklch(68%_0.16_115)]',
  TOP_25: 'text-[oklch(72%_0.15_85)]',
  TOP_10: 'text-[oklch(70%_0.16_60)]',
  TOP_5: 'text-[oklch(65%_0.18_35)]',
  TOP_1: 'text-destructive',
};

const GRADE_BG_COLORS: Record<PesticideGrade, string> = {
  UNDER_AVG: 'bg-[oklch(60%_0.18_145)]',
  TOP_50: 'bg-[oklch(68%_0.16_115)]',
  TOP_25: 'bg-[oklch(72%_0.15_85)]',
  TOP_10: 'bg-[oklch(70%_0.16_60)]',
  TOP_5: 'bg-[oklch(65%_0.18_35)]',
  TOP_1: 'bg-destructive',
};

/**
 * Hex equivalents of the oklch ramp. Used outside the Tailwind runtime
 * (e.g. PDF HTML generation) where class tokens don't resolve.
 */
const GRADE_HEX_COLORS: Record<PesticideGrade, string> = {
  UNDER_AVG: '#3fa961',
  TOP_50: '#9ea833',
  TOP_25: '#c29b2c',
  TOP_10: '#d4762c',
  TOP_5: '#c95632',
  TOP_1: '#c43030',
};

export function getGradeColor(grade: PesticideGrade): string {
  return GRADE_TEXT_COLORS[grade];
}

export function getGradeBgColor(grade: PesticideGrade): string {
  return GRADE_BG_COLORS[grade];
}

export function getGradeHexColor(grade: PesticideGrade): string {
  return GRADE_HEX_COLORS[grade];
}

import {
  GRADE_DEFINITIONS,
  getGradeHexColor,
  isPesticideGrade,
} from '@/lib/pesticide-score';
import type { PesticideGrade } from '@/components/pesticidkort/types';

const DEFAULT_ACCENT_COLOR = '#57534e';
const DEFAULT_GRADE_META = {
  label: 'Pesticideksponering',
  description: 'Modelleret estimat baseret på offentlige data',
};

export function resolveGradeMeta(grade: string | null) {
  const pesticideGrade: PesticideGrade | null =
    grade && isPesticideGrade(grade) ? grade : null;

  return {
    color: pesticideGrade
      ? getGradeHexColor(pesticideGrade)
      : DEFAULT_ACCENT_COLOR,
    meta: pesticideGrade
      ? GRADE_DEFINITIONS[pesticideGrade]
      : DEFAULT_GRADE_META,
    hasGrade: Boolean(pesticideGrade),
  };
}

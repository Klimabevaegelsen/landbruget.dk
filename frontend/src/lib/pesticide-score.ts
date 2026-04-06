/* oxlint-disable landbruget/require-test-coverage */
import type {
  PesticideGrade,
  GradeInfo,
  NearbyFieldSummary,
} from '@/components/pesticidkort/types';

/**
 * Grade thresholds in B/ha (belastning per hectare), anchored to
 * Miljøstyrelsen's official Pesticidbelastningsindikator (PBI).
 *
 * Source: Bekæmpelsesmiddelstatistik 2023 (Miljøstyrelsen, April 2025)
 *
 * Reference points (fladebelastning, 2023 sales data):
 *   Græs/kløver  0.03 B/ha  →  A
 *   Vårsæd       0.77 B/ha  →  B
 *   National avg  2.15 B/ha  →  C
 *   Grøntsager   4.44 B/ha  →  D
 *   Kartofler   10.20 B/ha  →  E
 */
const GRADE_THRESHOLDS: { max: number; grade: PesticideGrade }[] = [
  { max: 0.5, grade: 'A' },
  { max: 2.0, grade: 'B' },
  { max: 4.0, grade: 'C' },
  { max: 8.0, grade: 'D' },
];

const GRADE_DEFINITIONS: Record<PesticideGrade, Omit<GradeInfo, 'grade'>> = {
  A: {
    label: 'Meget lav belastning',
    description:
      'Der sprøjtes markant mindre med pesticider nær din adresse end de fleste steder i Danmark',
  },
  B: {
    label: 'Under gennemsnit',
    description:
      'Der sprøjtes mindre med pesticider nær din adresse end landsgennemsnittet',
  },
  C: {
    label: 'Omkring gennemsnit',
    description:
      'Pesticidbrugen nær din adresse svarer til et typisk dansk landbrugsområde',
  },
  D: {
    label: 'Over gennemsnit',
    description:
      'Der sprøjtes mere med pesticider nær din adresse end landsgennemsnittet',
  },
  E: {
    label: 'Meget høj belastning',
    description:
      'Der sprøjtes markant mere med pesticider nær din adresse end de fleste steder i Danmark',
  },
};

/**
 * Compute a proximity-based pesticide score for a given address.
 *
 * Returns the distance-weighted average burden (B/ha) across nearby fields,
 * with multipliers for PFAS and BNBO overlap, mapped to an A–E grade
 * anchored to Miljøstyrelsen's official fladebelastning data.
 */
export function computePesticideScore(
  fields: NearbyFieldSummary[],
  radius_m: number
): { score: number; grade: GradeInfo } {
  if (fields.length === 0) {
    return { score: 0, grade: { grade: 'A', ...GRADE_DEFINITIONS.A } };
  }

  let totalWeightedBurden = 0;
  let totalWeight = 0;

  for (const field of fields) {
    const distanceFraction = Math.max(0, 1 - field.distance_m / radius_m);
    let fieldBurden = field.total_pesticide_belastning * distanceFraction;

    if (field.pfas_applications > 0) {
      fieldBurden *= 1.5;
    }
    if (
      field.pfas_applications > 0 &&
      field.bnbo_area_hectares &&
      field.bnbo_area_hectares > 0
    ) {
      fieldBurden *= 1.3;
    }

    totalWeightedBurden += fieldBurden;
    totalWeight += distanceFraction;
  }

  // Weighted average burden in B/ha units
  const avgBurden = totalWeight > 0 ? totalWeightedBurden / totalWeight : 0;
  const grade = burdenToGrade(avgBurden);

  return {
    score: Math.round(avgBurden * 100) / 100,
    grade: { grade, ...GRADE_DEFINITIONS[grade] },
  };
}

function burdenToGrade(burden: number): PesticideGrade {
  for (const { max, grade } of GRADE_THRESHOLDS) {
    if (burden < max) return grade;
  }
  return 'E';
}

export function getGradeColor(grade: PesticideGrade): string {
  const colors: Record<PesticideGrade, string> = {
    A: 'text-[oklch(65%_0.15_155)]',
    B: 'text-[oklch(60%_0.15_142)]',
    C: 'text-[oklch(70%_0.14_85)]',
    D: 'text-[oklch(65%_0.15_45)]',
    E: 'text-destructive',
  };
  return colors[grade];
}

export function getGradeBgColor(grade: PesticideGrade): string {
  const colors: Record<PesticideGrade, string> = {
    A: 'bg-[oklch(65%_0.15_155)]',
    B: 'bg-[oklch(60%_0.15_142)]',
    C: 'bg-[oklch(70%_0.14_85)]',
    D: 'bg-[oklch(65%_0.15_45)]',
    E: 'bg-destructive',
  };
  return colors[grade];
}

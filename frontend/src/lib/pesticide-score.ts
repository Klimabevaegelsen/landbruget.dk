/* oxlint-disable landbruget/require-test-coverage */
import type {
  PesticideGrade,
  GradeInfo,
  NearbyFieldSummary,
} from '@/components/pesticidkort/types';
import { GRADE_DEFINITIONS, GRADE_ORDER } from '@/lib/pesticide-score-labels';

export {
  getGradeColor,
  getGradeBgColor,
  getGradeHexColor,
} from '@/lib/pesticide-score-colors';
export { GRADE_DEFINITIONS, GRADE_ORDER };

/**
 * Miljøstyrelsen's national average pesticide burden (PBI fladebelastning,
 * 2023). Cutoff between TOP_50 and UNDER_AVG in the burden-based fallback.
 */
const NATIONAL_AVG_BURDEN_B_HA = 2.15;

/**
 * B/ha percentile approximations from the national field histogram. Anchors
 * the fallback grade so it aligns with the drift-based grade: top 1% fields
 * around 11 B/ha (potato-heavy), down to the national average near p50.
 */
const FALLBACK_BURDEN_BUCKETS: { min: number; grade: PesticideGrade }[] = [
  { min: 10.0, grade: 'TOP_1' },
  { min: 6.0, grade: 'TOP_5' },
  { min: 4.0, grade: 'TOP_10' },
  { min: 2.5, grade: 'TOP_25' },
  { min: NATIONAL_AVG_BURDEN_B_HA, grade: 'TOP_50' },
];

/**
 * Map a drift-exposure percentile (0-100, higher = more exposed) to a grade.
 * Below p50, compare absolute drift dose to the national average to decide
 * between TOP_50 and UNDER_AVG.
 */
export function driftPercentileToGrade(
  percentile: number,
  driftDoseKg: number,
  nationalAvgDriftDoseKg: number | null
): GradeInfo {
  let grade: PesticideGrade;
  if (percentile >= 99) grade = 'TOP_1';
  else if (percentile >= 95) grade = 'TOP_5';
  else if (percentile >= 90) grade = 'TOP_10';
  else if (percentile >= 75) grade = 'TOP_25';
  else if (percentile >= 50) grade = 'TOP_50';
  else if (
    nationalAvgDriftDoseKg !== null &&
    driftDoseKg < nationalAvgDriftDoseKg
  )
    grade = 'UNDER_AVG';
  else grade = 'TOP_50';

  return { grade, ...GRADE_DEFINITIONS[grade] };
}

/**
 * Fallback grade for addresses without a BBR drift-exposure match: maps the
 * distance-weighted local field burden onto the same 6 buckets using
 * approximate national percentile thresholds.
 */
function burdenToGrade(burden: number): GradeInfo {
  let grade: PesticideGrade = 'UNDER_AVG';
  for (const { min, grade: g } of FALLBACK_BURDEN_BUCKETS) {
    if (burden >= min) {
      grade = g;
      break;
    }
  }
  return { grade, ...GRADE_DEFINITIONS[grade] };
}

/**
 * Distance-weighted average burden (B/ha) across nearby fields, mapped to a
 * 6-bucket grade via the fallback path. Prefer {@link driftPercentileToGrade}
 * when drift-exposure data is available.
 */
export function computePesticideScore(
  fields: NearbyFieldSummary[],
  radius_m: number
): { score: number; grade: GradeInfo } {
  if (fields.length === 0) {
    return { score: 0, grade: burdenToGrade(0) };
  }

  let totalWeightedBurden = 0;
  let totalWeight = 0;

  for (const field of fields) {
    const distanceFraction = Math.max(0, 1 - field.distance_m / radius_m);
    const fieldBurden = field.total_pesticide_belastning * distanceFraction;
    totalWeightedBurden += fieldBurden;
    totalWeight += distanceFraction;
  }

  const avgBurden = totalWeight > 0 ? totalWeightedBurden / totalWeight : 0;
  return {
    score: Math.round(avgBurden * 100) / 100,
    grade: burdenToGrade(avgBurden),
  };
}

export function isPesticideGrade(value: string): value is PesticideGrade {
  return (GRADE_ORDER as string[]).includes(value);
}

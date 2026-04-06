import { computePesticideScore } from '@/lib/pesticide-score';
import type {
  PesticideReport,
  NearbyFieldSummary,
} from '@/components/pesticidkort/types';

export function buildReport(
  fields: NearbyFieldSummary[],
  address: string,
  lat: number,
  lng: number,
  radiusM: number,
  year: number
): PesticideReport {
  const { score, grade } = computePesticideScore(fields, radiusM);
  return {
    address,
    lat,
    lng,
    radius_m: radiusM,
    year,
    grade,
    score,
    fields_count: fields.length,
    pfas_fields_count: fields.filter((f) => f.pfas_applications > 0).length,
    nearest_field_m: fields[0]?.distance_m ?? 0,
    fields,
    has_bnbo_overlap: fields.some(
      (f) => f.bnbo_area_hectares && f.bnbo_area_hectares > 0
    ),
    has_violations: false,
  };
}

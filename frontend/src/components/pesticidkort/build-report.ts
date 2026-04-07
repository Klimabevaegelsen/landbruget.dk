import { computePesticideScore } from '@/lib/pesticide-score';
import type {
  PesticideReport,
  NearbyFieldSummary,
  ExposureSummary,
} from '@/components/pesticidkort/types';

function computeExposure(
  fields: NearbyFieldSummary[],
  maxDist: number
): ExposureSummary {
  const nearby = fields.filter((f) => f.distance_m <= maxDist);
  return {
    radius_m: maxDist,
    fields_sprayed: nearby.filter((f) => f.total_pesticide_belastning > 0)
      .length,
    total_applications: nearby.reduce(
      (s, f) => s + f.total_pesticide_applications,
      0
    ),
    pfas_fields_count: nearby.filter((f) => f.pfas_applications > 0).length,
  };
}

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
    avg_burden:
      fields.length > 0
        ? fields.reduce((sum, f) => sum + f.total_pesticide_belastning, 0) /
          fields.length
        : 0,
    nearest_field_m: fields[0]?.distance_m ?? 0,
    fields,
    has_bnbo_overlap: fields.some(
      (f) => f.bnbo_area_hectares && f.bnbo_area_hectares > 0
    ),
    has_violations: false,
    exposure_100m: computeExposure(fields, 100),
    exposure_1000m: computeExposure(fields, radiusM),
  };
}

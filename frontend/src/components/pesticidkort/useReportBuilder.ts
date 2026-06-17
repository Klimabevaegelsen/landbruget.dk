import { useState, useCallback, useEffect } from 'react';
import {
  computePesticideScore,
  driftPercentileToGrade,
} from '@/lib/pesticide-score';
import { NEAR_EXPOSURE_RADIUS_M } from '@/components/pesticidkort/exposure-utils';
import type {
  PesticideReport,
  NearbyFieldSummary,
  ExposureSummary,
  GradeInfo,
  PesticideGradeStatus,
} from '@/components/pesticidkort/types';
import type { DriftExposureMatch } from '@/components/pesticidkort/useDriftExposure';

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

type DriftStatus = 'idle' | 'loading' | 'ready' | 'no_match';

interface UseReportBuilderOptions {
  address: string;
  lat: number;
  lng: number;
  radiusM: number;
  year: number;
  driftExposure: DriftExposureMatch | null;
  driftStatus: DriftStatus;
}

export function useReportBuilder({
  address,
  lat,
  lng,
  radiusM,
  year,
  driftExposure,
  driftStatus,
}: UseReportBuilderOptions) {
  const [fields, setFields] = useState<NearbyFieldSummary[] | null>(null);
  const [report, setReport] = useState<PesticideReport | null>(null);

  const handleFieldsLoaded = useCallback((fields: NearbyFieldSummary[]) => {
    setFields(fields);
  }, []);

  useEffect(() => {
    if (!fields) {
      setReport(null);
      return;
    }

    const { score, grade: fallbackGrade } = computePesticideScore(
      fields,
      radiusM
    );

    let grade: GradeInfo | null = null;
    let gradeStatus: PesticideGradeStatus = 'loading';

    if (driftExposure) {
      grade = driftPercentileToGrade(
        driftExposure.exposure_percentile,
        driftExposure.drift_dose_kg,
        driftExposure.national_avg_drift_dose_kg
      );
      gradeStatus = 'ready';
    } else if (driftStatus === 'no_match') {
      if (fields.length === 0) {
        gradeStatus = 'no_data';
      } else {
        grade = fallbackGrade;
        gradeStatus = 'ready';
      }
    }

    let pfasCount = 0;
    let totalBurden = 0;
    let hasBnbo = false;
    for (const f of fields) {
      if (f.pfas_applications > 0) pfasCount++;
      totalBurden += f.total_pesticide_belastning;
      if (f.bnbo_area_hectares && f.bnbo_area_hectares > 0) hasBnbo = true;
    }

    setReport({
      address,
      lat,
      lng,
      radius_m: radiusM,
      year,
      grade,
      grade_status: gradeStatus,
      score,
      fields_count: fields.length,
      pfas_fields_count: pfasCount,
      avg_burden:
        fields.length > 0
          ? Math.round((totalBurden / fields.length) * 100) / 100
          : 0,
      nearest_field_m: fields[0]?.distance_m ?? 0,
      fields,
      has_bnbo_overlap: hasBnbo,
      has_violations: false,
      exposure_100m: computeExposure(fields, NEAR_EXPOSURE_RADIUS_M),
      exposure_1000m: computeExposure(fields, radiusM),
    });
  }, [address, driftExposure, driftStatus, fields, lat, lng, radiusM, year]);

  return { report, handleFieldsLoaded };
}

import { useState, useCallback } from 'react';
import { computePesticideScore } from '@/lib/pesticide-score';
import type {
  PesticideReport,
  NearbyFieldSummary,
} from '@/components/pesticidkort/types';

interface UseReportBuilderOptions {
  address: string;
  lat: number;
  lng: number;
  radiusM: number;
  year: number;
}

export function useReportBuilder({
  address,
  lat,
  lng,
  radiusM,
  year,
}: UseReportBuilderOptions) {
  const [report, setReport] = useState<PesticideReport | null>(null);

  const handleFieldsLoaded = useCallback(
    (fields: NearbyFieldSummary[]) => {
      const { score, grade } = computePesticideScore(fields, radiusM);
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
      });
    },
    [address, lat, lng, radiusM, year]
  );

  return { report, handleFieldsLoaded };
}

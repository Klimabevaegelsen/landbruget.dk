'use client';

import { useState, useCallback } from 'react';
import { useRouter } from 'next/navigation';
import dynamic from 'next/dynamic';
import { PersonalReport } from '@/components/pesticidkort/PersonalReport';
import { BottomSheet } from '@/components/pesticidkort/BottomSheet';
import { ReportHeader } from '@/components/pesticidkort/ReportHeader';
import { YearTimeline } from '@/components/pesticidkort/YearTimeline';
import { StoryMode } from '@/components/pesticidkort/StoryMode';
import { ModeToggle } from '@/components/pesticidkort/ModeToggle';
import { computePesticideScore } from '@/lib/pesticide-score';
import type {
  PesticideReport,
  NearbyFieldSummary,
} from '@/components/pesticidkort/types';

const PesticidkortMap = dynamic(
  () =>
    import('@/components/pesticidkort/PesticidkortMap').then(
      (m) => m.PesticidkortMap
    ),
  {
    ssr: false,
    loading: () => (
      <div
        role="status"
        className="bg-muted flex h-full items-center justify-center"
      >
        <p className="text-muted-foreground text-sm">Indlæser kort...</p>
      </div>
    ),
  }
);

interface ReportMapViewProps {
  address: string;
  lat: number;
  lng: number;
  radiusM: number;
  year: number;
  onYearChange: (year: number) => void;
  onBack: () => void;
}

export function ReportMapView({
  address,
  lat,
  lng,
  radiusM,
  year,
  onYearChange,
  onBack,
}: ReportMapViewProps) {
  const [report, setReport] = useState<PesticideReport | null>(null);
  const [selectedField, setSelectedField] = useState<string | null>(null);
  const [showStory, setShowStory] = useState(false);
  const [sheetState, setSheetState] = useState<'peek' | 'half' | 'full'>(
    'half'
  );
  const router = useRouter();

  const handleFieldsLoaded = useCallback(
    (fields: NearbyFieldSummary[]) => {
      const { score, grade } = computePesticideScore(fields, radiusM);
      const pfasFields = fields.filter((f) => f.pfas_applications > 0);
      setReport({
        address,
        lat,
        lng,
        radius_m: radiusM,
        year,
        grade,
        score,
        fields_count: fields.length,
        pfas_fields_count: pfasFields.length,
        nearest_field_m: fields[0]?.distance_m ?? 0,
        fields,
        has_bnbo_overlap: fields.some(
          (f) => f.bnbo_area_hectares && f.bnbo_area_hectares > 0
        ),
        has_violations: false,
      });
    },
    [address, lat, lng, radiusM, year]
  );

  const handleModeChange = useCallback(
    (m: 'citizen' | 'expert') => {
      if (m === 'expert') router.push(`/markanalyse?lat=${lat}&lng=${lng}`);
    },
    [router, lat, lng]
  );

  const reportPanel = (
    <>
      <div className="px-6 pt-2">
        <ModeToggle mode="citizen" onChange={handleModeChange} />
        <YearTimeline year={year} onChange={onYearChange} compact />
      </div>
      {report ? (
        <PersonalReport
          report={report}
          onFieldSelect={setSelectedField}
          onOpenStory={() => setShowStory(true)}
        />
      ) : (
        <div aria-live="polite" className="space-y-4 px-6 py-5">
          <div className="bg-muted h-5 w-32 animate-pulse rounded" />
          <div className="bg-muted h-20 w-full animate-pulse rounded-xl" />
          <div className="bg-muted h-24 w-full animate-pulse rounded-xl" />
          <div className="bg-muted h-24 w-full animate-pulse rounded-xl" />
          <p className="text-muted-foreground text-xs">
            Indlæser pesticidrapport...
          </p>
        </div>
      )}
    </>
  );

  return (
    <main className="relative h-screen w-full">
      <div className="absolute inset-0" role="region" aria-label="Pesticidkort">
        <PesticidkortMap
          lat={lat}
          lng={lng}
          radiusM={radiusM}
          year={year}
          selectedFieldUuid={selectedField}
          onFieldsLoaded={handleFieldsLoaded}
        />
      </div>
      <ReportHeader address={address} year={year} onBack={onBack} />

      <div className="md:hidden">
        <BottomSheet state={sheetState} onStateChange={setSheetState}>
          {reportPanel}
        </BottomSheet>
      </div>

      <div className="pointer-events-none absolute inset-y-0 left-0 hidden w-[420px] md:block">
        <div className="bg-background/95 pointer-events-auto h-full overflow-y-auto border-r pt-14 backdrop-blur-sm">
          {reportPanel}
        </div>
      </div>

      {showStory && report && (
        <StoryMode report={report} onClose={() => setShowStory(false)} />
      )}
    </main>
  );
}

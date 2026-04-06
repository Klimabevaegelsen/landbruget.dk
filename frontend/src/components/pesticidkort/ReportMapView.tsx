'use client';

import { useState, useCallback } from 'react';
import { useRouter } from 'next/navigation';
import dynamic from 'next/dynamic';
import { PersonalReport } from '@/components/pesticidkort/PersonalReport';
import { useBurdenHistogram } from '@/components/pesticidkort/useBurdenHistogram';
import { BottomSheet } from '@/components/pesticidkort/BottomSheet';
import { ReportHeader } from '@/components/pesticidkort/ReportHeader';
import { StoryMode } from '@/components/pesticidkort/StoryMode';
import { ReportSkeleton } from '@/components/pesticidkort/ReportSkeleton';
import { ReportControls } from '@/components/pesticidkort/ReportControls';
import { buildReport } from '@/components/pesticidkort/build-report';
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
  const histogram = useBurdenHistogram(year);

  const handleFieldsLoaded = useCallback(
    (fields: NearbyFieldSummary[]) =>
      setReport(buildReport(fields, address, lat, lng, radiusM, year)),
    [address, lat, lng, radiusM, year]
  );
  const handleModeChange = useCallback(
    (m: 'citizen' | 'expert') => {
      if (m === 'expert') router.push(`/markanalyse?lat=${lat}&lng=${lng}`);
    },
    [router, lat, lng]
  );

  const reportContent = report ? (
    <PersonalReport
      report={report}
      histogram={histogram}
      onFieldSelect={setSelectedField}
      onOpenStory={() => setShowStory(true)}
    />
  ) : (
    <ReportSkeleton />
  );
  const controls = (
    <ReportControls
      mode="citizen"
      onModeChange={handleModeChange}
      year={year}
      onYearChange={onYearChange}
    />
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
          {reportContent}
          {controls}
        </BottomSheet>
      </div>
      <div className="pointer-events-none absolute inset-y-0 left-0 hidden w-[420px] md:block">
        <div className="bg-background/95 pointer-events-auto flex h-full flex-col border-r pt-14 backdrop-blur-sm">
          <div
            className="flex-1 overflow-y-auto"
            role="region"
            aria-label="Pesticidrapport"
          >
            {reportContent}
          </div>
          {controls}
        </div>
      </div>
      {showStory && report && (
        <StoryMode report={report} onClose={() => setShowStory(false)} />
      )}
    </main>
  );
}

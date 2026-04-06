'use client';

import { useState, useCallback } from 'react';
import { useRouter } from 'next/navigation';
import dynamic from 'next/dynamic';
import { PersonalReport } from '@/components/pesticidkort/PersonalReport';
import { useBurdenHistogram } from '@/components/pesticidkort/useBurdenHistogram';
import { useReportBuilder } from '@/components/pesticidkort/useReportBuilder';
import { BottomSheet } from '@/components/pesticidkort/BottomSheet';
import { DesktopSidebar } from '@/components/pesticidkort/DesktopSidebar';
import { ReportHeader } from '@/components/pesticidkort/ReportHeader';
import { YearTimeline } from '@/components/pesticidkort/YearTimeline';
import { StoryMode } from '@/components/pesticidkort/StoryMode';
import { ModeToggle } from '@/components/pesticidkort/ModeToggle';

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
  const [selectedField, setSelectedField] = useState<string | null>(null);
  const [showStory, setShowStory] = useState(false);
  const [sheetState, setSheetState] = useState<'peek' | 'half' | 'full'>(
    'half'
  );
  const router = useRouter();
  const histogram = useBurdenHistogram(year);
  const { report, handleFieldsLoaded } = useReportBuilder({
    address,
    lat,
    lng,
    radiusM,
    year,
  });

  const handleMapFieldClick = useCallback(
    (fieldUuid: string) => {
      setSelectedField(fieldUuid);
      if (sheetState === 'peek') setSheetState('half');
    },
    [sheetState]
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
      selectedFieldUuid={selectedField}
      onFieldSelect={setSelectedField}
      onOpenStory={() => setShowStory(true)}
    />
  ) : (
    <div
      aria-live="polite"
      className="animate-fade-slide-up space-y-4 px-5 py-5 motion-reduce:animate-none"
    >
      <p className="text-muted-foreground text-sm">
        Analyserer marker nær din adresse...
      </p>
      {['h-20', 'h-12', 'h-24'].map((h) => (
        <div key={h} className={`bg-muted ${h} animate-pulse rounded-xl`} />
      ))}
    </div>
  );

  const controls = (
    <div className="bg-background/95 border-border border-t px-5 py-3 backdrop-blur-sm">
      <ModeToggle mode="citizen" onChange={handleModeChange} />
      <YearTimeline year={year} onChange={onYearChange} compact />
    </div>
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
          onFieldClick={handleMapFieldClick}
        />
      </div>
      <ReportHeader address={address} year={year} onBack={onBack} />

      <div className="md:hidden">
        <BottomSheet state={sheetState} onStateChange={setSheetState}>
          {reportContent}
          {controls}
        </BottomSheet>
      </div>

      <DesktopSidebar controls={controls}>{reportContent}</DesktopSidebar>
      {showStory && report && (
        <StoryMode report={report} onClose={() => setShowStory(false)} />
      )}
    </main>
  );
}

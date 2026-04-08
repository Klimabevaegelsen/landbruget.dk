'use client';

import { useState, useCallback } from 'react';
import dynamic from 'next/dynamic';
import { PersonalReport } from '@/components/pesticidkort/PersonalReport';
import type { NearbyFieldSummary } from '@/components/pesticidkort/types';
import { useBurdenHistogram } from '@/components/pesticidkort/useBurdenHistogram';
import { useReportBuilder } from '@/components/pesticidkort/useReportBuilder';
import { BottomSheet } from '@/components/pesticidkort/BottomSheet';
import { DesktopSidebar } from '@/components/pesticidkort/DesktopSidebar';
import { ReportHeader } from '@/components/pesticidkort/ReportHeader';
import { YearTimeline } from '@/components/pesticidkort/YearTimeline';
import { ChemicalFilterPills } from '@/components/pesticidkort/ChemicalFilterPills';
import { MapLegend } from '@/components/pesticidkort/MapLegend';
import { StoryMode } from '@/components/pesticidkort/StoryMode';
import type { ChemicalFilter } from '@/components/pesticidkort/map-layers';

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
        <p className="text-muted-foreground text-sm">Indl&aelig;ser kort...</p>
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
  const [clickedField, setClickedField] = useState<NearbyFieldSummary | null>(
    null
  );
  const [showStory, setShowStory] = useState(false);
  const [sheetState, setSheetState] = useState<'peek' | 'half' | 'full'>(
    'half'
  );
  const [chemFilter, setChemFilter] = useState<ChemicalFilter>('none');
  const histogram = useBurdenHistogram(year);
  const { report, handleFieldsLoaded } = useReportBuilder({
    address,
    lat,
    lng,
    radiusM,
    year,
  });

  const handleMapFieldClick = useCallback(
    (fieldUuid: string, fieldData: NearbyFieldSummary) => {
      setSelectedField(fieldUuid);
      setClickedField(fieldData);
      if (sheetState === 'peek') setSheetState('half');
    },
    [sheetState]
  );

  const reportContent = report ? (
    <PersonalReport
      report={report}
      histogram={histogram}
      selectedFieldUuid={selectedField}
      clickedField={clickedField}
      onFieldSelect={setSelectedField}
      onOpenStory={() => setShowStory(true)}
    />
  ) : (
    <div
      aria-live="polite"
      className="animate-fade-slide-up space-y-4 px-5 py-5 motion-reduce:animate-none"
    >
      <p className="text-muted-foreground text-sm">
        Analyserer marker n&aelig;r din adresse...
      </p>
      {['h-20', 'h-12', 'h-24'].map((h) => (
        <div key={h} className={`bg-muted ${h} animate-pulse rounded-xl`} />
      ))}
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
          activeFilter={chemFilter}
        />
      </div>
      <ReportHeader address={address} onBack={onBack} />

      <MapLegend activeFilter={chemFilter} />

      {/* Floating map controls: year + chemical pills */}
      <div className="pointer-events-none absolute inset-x-0 bottom-[55%] z-40 flex flex-col items-center gap-2 px-4 md:right-[420px] md:bottom-6">
        <div className="bg-background/90 pointer-events-auto max-w-xs min-w-[200px] self-stretch rounded-full px-5 py-1 backdrop-blur-sm md:min-w-[280px] md:self-auto">
          <YearTimeline year={year} onChange={onYearChange} compact />
        </div>
        <ChemicalFilterPills
          active={chemFilter}
          onChange={setChemFilter}
          zoomLevel={14}
        />
      </div>

      <div className="md:hidden">
        <BottomSheet state={sheetState} onStateChange={setSheetState}>
          {reportContent}
        </BottomSheet>
      </div>

      <DesktopSidebar>{reportContent}</DesktopSidebar>
      {showStory && report && (
        <StoryMode report={report} onClose={() => setShowStory(false)} />
      )}
    </main>
  );
}

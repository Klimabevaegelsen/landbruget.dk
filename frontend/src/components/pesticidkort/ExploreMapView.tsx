'use client';

import { useState, useCallback } from 'react';
import dynamic from 'next/dynamic';
import { AddressAutocomplete } from '@/components/pesticidkort/AddressAutocomplete';
import { YearTimeline } from '@/components/pesticidkort/YearTimeline';
import { MapLegend } from '@/components/pesticidkort/MapLegend';
import { MapOnboardingHint } from '@/components/pesticidkort/MapOnboardingHint';
import { ChemicalFilterPills } from '@/components/pesticidkort/ChemicalFilterPills';
import { ExploreFieldPanel } from '@/components/pesticidkort/ExploreFieldPanel';
import type { ChemicalFilter } from '@/components/pesticidkort/map-layers';
import type {
  AddressResult,
  NearbyFieldSummary,
} from '@/components/pesticidkort/types';

const ExploreMap = dynamic(
  () =>
    import('@/components/pesticidkort/ExploreMapInner').then(
      (m) => m.ExploreMapInner
    ),
  { ssr: false, loading: () => <MapSkeleton /> }
);

function MapSkeleton() {
  return (
    <div
      role="status"
      className="bg-muted flex h-full items-center justify-center"
    >
      <p className="text-muted-foreground text-sm">Indlæser kort...</p>
    </div>
  );
}

interface ExploreMapViewProps {
  year: number;
  onYearChange: (year: number) => void;
  onAddressSelect: (result: AddressResult) => void;
  onBack: () => void;
}

export function ExploreMapView({
  year,
  onYearChange,
  onAddressSelect,
  onBack,
}: ExploreMapViewProps) {
  const [searchOpen, setSearchOpen] = useState(true);
  const [chemFilter, setChemFilter] = useState<ChemicalFilter>('none');
  const [zoomLevel, setZoomLevel] = useState(7);
  const [selectedField, setSelectedField] = useState<NearbyFieldSummary | null>(
    null
  );

  const handleSelect = useCallback(
    (result: AddressResult) => {
      onAddressSelect(result);
    },
    [onAddressSelect]
  );

  const handleFieldClick = useCallback(
    (_uuid: string, data: NearbyFieldSummary) => {
      setSelectedField(data);
    },
    []
  );

  return (
    <main className="relative h-screen w-full">
      <div className="absolute inset-0" role="region" aria-label="Pesticidkort">
        <ExploreMap
          year={year}
          activeFilter={chemFilter}
          onZoomChange={setZoomLevel}
          onFieldClick={handleFieldClick}
          selectedFieldUuid={selectedField?.field_uuid ?? null}
        />
      </div>

      <MapLegend activeFilter={chemFilter} />
      <MapOnboardingHint />

      <div className="absolute top-0 right-0 left-0 z-20 px-3 pt-3 sm:px-4">
        <div className="mx-auto flex max-w-lg items-center gap-2">
          <button
            onClick={onBack}
            data-testid="explore-back-button"
            aria-label="Tilbage"
            className="bg-background/90 text-muted-foreground border-border/70 flex h-12 w-12 shrink-0 items-center justify-center rounded-full border backdrop-blur-sm"
          >
            &larr;
          </button>
          {searchOpen && (
            <div className="bg-background/90 border-border/70 flex-1 rounded-full border backdrop-blur-sm">
              <AddressAutocomplete onSelect={handleSelect} />
            </div>
          )}
          {!searchOpen && (
            <button
              onClick={() => setSearchOpen(true)}
              data-testid="explore-search-open-button"
              className="bg-background/90 text-muted-foreground border-border/70 h-12 rounded-full border px-5 text-sm backdrop-blur-sm"
            >
              S&oslash;g adresse...
            </button>
          )}
        </div>

        <div className="mx-auto mt-2 flex max-w-lg flex-col items-stretch gap-2">
          <div className="bg-background/90 border-border/70 w-full max-w-full rounded-full border px-2 py-1 backdrop-blur-sm md:max-w-md md:min-w-[480px] md:self-center md:px-3">
            <YearTimeline year={year} onChange={onYearChange} />
          </div>
          <ChemicalFilterPills
            active={chemFilter}
            onChange={setChemFilter}
            zoomLevel={zoomLevel}
          />
        </div>
      </div>

      {selectedField && (
        <ExploreFieldPanel
          field={selectedField}
          onClose={() => setSelectedField(null)}
        />
      )}
    </main>
  );
}

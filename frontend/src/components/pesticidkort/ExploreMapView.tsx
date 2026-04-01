'use client';

import { useState, useCallback } from 'react';
import dynamic from 'next/dynamic';
import { AddressAutocomplete } from '@/components/pesticidkort/AddressAutocomplete';
import { YearTimeline } from '@/components/pesticidkort/YearTimeline';
import type { AddressResult } from '@/components/pesticidkort/types';

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

  const handleSelect = useCallback(
    (result: AddressResult) => {
      onAddressSelect(result);
    },
    [onAddressSelect]
  );

  return (
    <main className="relative h-screen w-full">
      <div className="absolute inset-0" role="region" aria-label="Pesticidkort">
        <ExploreMap year={year} />
      </div>

      <div className="absolute top-0 right-0 left-0 z-20 px-4 pt-3">
        <div className="mx-auto flex max-w-lg items-center gap-2">
          <button
            onClick={onBack}
            data-testid="explore-back-button"
            aria-label="Tilbage"
            className="bg-background/90 text-muted-foreground flex h-12 w-12 shrink-0 items-center justify-center rounded-full backdrop-blur-sm"
          >
            ←
          </button>
          {searchOpen && (
            <div className="bg-background/90 flex-1 rounded-full backdrop-blur-sm">
              <AddressAutocomplete onSelect={handleSelect} />
            </div>
          )}
          {!searchOpen && (
            <button
              onClick={() => setSearchOpen(true)}
              data-testid="explore-search-open-button"
              className="bg-background/90 text-muted-foreground h-12 rounded-full px-5 text-sm backdrop-blur-sm"
            >
              Søg adresse...
            </button>
          )}
        </div>

        <div className="mx-auto mt-2 max-w-xs">
          <div className="bg-background/90 rounded-full px-5 py-1 backdrop-blur-sm">
            <YearTimeline year={year} onChange={onYearChange} compact />
          </div>
        </div>
      </div>
    </main>
  );
}

'use client';

import { useState, useCallback, useEffect } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import { AnimatePresence, motion } from 'framer-motion';
import { LandingHero } from '@/components/pesticidkort/LandingHero';
import { ReportMapView } from '@/components/pesticidkort/ReportMapView';
import { ExploreMapView } from '@/components/pesticidkort/ExploreMapView';
import type { AddressResult } from '@/components/pesticidkort/types';

interface SearchState {
  address: string;
  lat: number;
  lng: number;
}

function parseUrlState(params: URLSearchParams) {
  const lat = parseFloat(params.get('lat') ?? '');
  const lng = parseFloat(params.get('lng') ?? '');
  const y = parseInt(params.get('y') ?? '', 10);
  const addr = params.get('addr') ?? '';
  if (!isNaN(lat) && !isNaN(lng) && addr) {
    return { lat, lng, address: addr, year: isNaN(y) ? 2023 : y };
  }
  return null;
}

export function PesticidkortApp() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const urlState = parseUrlState(searchParams);

  const [search, setSearch] = useState<SearchState | null>(
    urlState
      ? { address: urlState.address, lat: urlState.lat, lng: urlState.lng }
      : null
  );
  const [mode, setMode] = useState<'landing' | 'report' | 'map'>(
    urlState ? 'report' : 'landing'
  );
  const [year, setYear] = useState(urlState?.year ?? 2023);

  useEffect(() => {
    if (mode === 'report' && search) {
      const params = new URLSearchParams();
      params.set('lat', search.lat.toFixed(5));
      params.set('lng', search.lng.toFixed(5));
      params.set('addr', search.address);
      params.set('y', String(year));
      router.replace(`/pesticidkort?${params.toString()}`, { scroll: false });
    } else if (mode === 'landing') {
      router.replace('/pesticidkort', { scroll: false });
    }
  }, [mode, search, year, router]);

  const handleAddressSelect = useCallback((result: AddressResult) => {
    setSearch({ address: result.address, lat: result.lat, lng: result.lng });
    setMode('report');
  }, []);

  const handleExploreMap = useCallback(() => {
    setMode('map');
  }, []);

  const handleBack = useCallback(() => {
    setMode('landing');
    setSearch(null);
  }, []);

  return (
    <AnimatePresence mode="wait" initial={false}>
      {mode === 'landing' && (
        <motion.div
          key="landing"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.25 }}
        >
          <LandingHero
            onAddressSelect={handleAddressSelect}
            onExploreMap={handleExploreMap}
            year={year}
            onYearChange={setYear}
          />
        </motion.div>
      )}

      {mode === 'report' && search && (
        <motion.div
          key="report"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.2 }}
        >
          <ReportMapView
            address={search.address}
            lat={search.lat}
            lng={search.lng}
            radiusM={1000}
            year={year}
            onYearChange={setYear}
            onBack={handleBack}
          />
        </motion.div>
      )}

      {!(mode === 'report' && search) && mode !== 'landing' && (
        <motion.div
          key="map"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.2 }}
        >
          <ExploreMapView
            year={year}
            onYearChange={setYear}
            onAddressSelect={handleAddressSelect}
            onBack={handleBack}
          />
        </motion.div>
      )}
    </AnimatePresence>
  );
}

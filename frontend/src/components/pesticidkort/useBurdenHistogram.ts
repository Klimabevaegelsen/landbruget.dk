'use client';

import { useState, useEffect } from 'react';
import type { HistogramBin } from '@/components/pesticidkort/BurdenScale';

export function useBurdenHistogram(year: number): HistogramBin[] {
  const [histogram, setHistogram] = useState<HistogramBin[]>([]);

  useEffect(() => {
    fetch(`/api/burden-histogram?year=${year}`)
      .then((r) => (r.ok ? r.json() : []))
      .then(setHistogram)
      .catch(() => setHistogram([]));
  }, [year]);

  return histogram;
}

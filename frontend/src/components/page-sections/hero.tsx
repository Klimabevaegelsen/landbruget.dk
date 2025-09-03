'use client';

import { useEffect, useState, useCallback } from 'react';
import { GlobalSearch } from '../global-search';
import { useHomepageStatsCache } from '@/hooks/useHomepageStatsCache';

interface HomepageStatistics {
  total_data_points: number;
  total_companies: number;
  last_updated: string;
  formatted: {
    data_points: string;
    companies: string;
  };
  fallback?: boolean;
}

export default function Hero() {
  const [stats, setStats] = useState<HomepageStatistics | null>(null);
  const [loading, setLoading] = useState(true);
  const [usingCache, setUsingCache] = useState(false);

  const { getCachedData, setCachedData } = useHomepageStatsCache();

  const fetchStats = useCallback(
    async (forceRefresh: boolean = false) => {
      // Check cache first if not forcing refresh
      if (!forceRefresh) {
        const cachedData = getCachedData();
        if (cachedData) {
          setStats(cachedData);
          setUsingCache(true);
          setLoading(false);
          return;
        }
      }

      setUsingCache(false);
      setLoading(true);

      try {
        const response = await fetch('/api/homepage-statistics');
        if (response.ok) {
          const data = await response.json();

          // Cache the fresh data
          setCachedData(data);

          setStats(data);
        } else {
          console.error('Failed to fetch homepage statistics');
        }
      } catch (error) {
        console.error('Error fetching homepage statistics:', error);
      } finally {
        setLoading(false);
      }
    },
    [getCachedData, setCachedData]
  );

  useEffect(() => {
    fetchStats();
  }, [fetchStats]);

  // Format numbers with Danish formatting (periods for thousands)
  const formatDanishNumber = (num: number) => {
    return num.toLocaleString('da-DK');
  };

  const displayDataPoints = loading
    ? '...'
    : stats
      ? formatDanishNumber(stats.total_data_points)
      : '29.100.000+'; // Fallback based on our query results

  const displayCompanies = loading
    ? '...'
    : stats
      ? formatDanishNumber(stats.total_companies)
      : '46.000+'; // Fallback based on our query results

  return (
    <div className="relative isolate px-6 pt-14 lg:px-8">
      <div className="mx-auto max-w-4xl py-18 sm:py-28 lg:py-40">
        <div className="flex flex-col gap-6 text-center">
          <h1 className="text-5xl font-bold tracking-tight text-balance text-white sm:text-5xl">
            Dansk landbrugsdata - samlet ét sted
          </h1>
          <div className="md:mx-auto">
            <div className="w-full md:w-[500px]">
              <GlobalSearch
                className=""
                borderless
                searchSuggestions={[
                  'TYBJERGGAARD AGRI ApS',
                  'Bram I/S',
                  'Egegaard Landbrug ApS',
                ]}
              />
            </div>
          </div>

          <p className="text-sm font-medium text-pretty text-white sm:text-xl/8">
            <span className="font-bold">{displayDataPoints} datapunkter</span>{' '}
            fordelt på{' '}
            <span className="font-bold">
              {displayCompanies} danske landbrugsvirksomheder
            </span>
            .<br /> Data gennemsigtighed. Fri adgang og{' '}
            <a
              href="https://github.com/klimabevaegelsen/landbruget.dk/"
              target="_blank"
              rel="noopener noreferrer"
              className="underline hover:no-underline"
            >
              open source
            </a>
            .
          </p>

          {usingCache && !loading && (
            <p className="mt-2 text-xs text-white/70">
              📊 Data fra cache • Opdateres automatisk hver uge
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

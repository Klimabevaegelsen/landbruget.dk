'use client';

import { useEffect, useState, useCallback } from 'react';
import { GlobalSearch } from '@/components/global-search';
import { useHomepageStatsCache } from '@/hooks/useHomepageStatsCache';
import { useTheme } from '@/components/theme/theme-provider';
import { BarChart3 } from 'lucide-react';

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

export function Hero() {
  const [stats, setStats] = useState<HomepageStatistics | null>(null);
  const [loading, setLoading] = useState(true);
  const [usingCache, setUsingCache] = useState(false);
  const [isDark, setIsDark] = useState(false);

  const { getCachedData, setCachedData } = useHomepageStatsCache();
  const { theme } = useTheme();

  // Handle theme detection
  useEffect(() => {
    if (theme === 'system') {
      const systemTheme = window.matchMedia(
        '(prefers-color-scheme: dark)'
      ).matches;
      setIsDark(systemTheme);

      // Listen for system theme changes
      const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
      const handleChange = (e: MediaQueryListEvent) => setIsDark(e.matches);
      mediaQuery.addEventListener('change', handleChange);

      return () => mediaQuery.removeEventListener('change', handleChange);
    } else {
      setIsDark(theme === 'dark');
    }
  }, [theme]);

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
    <div className="hero-section relative isolate overflow-hidden px-6 pt-14 lg:px-8">
      {/* Background Image */}
      <div className="absolute inset-0 -z-10">
        <picture>
          <source
            media="(min-width: 1024px)"
            srcSet={
              isDark
                ? '/images/hero/hero-desktop-dark.webp'
                : '/images/hero/hero-desktop.webp'
            }
            type="image/webp"
          />
          <source
            media="(min-width: 1024px)"
            srcSet={
              isDark
                ? '/images/hero/hero-desktop-dark.jpg'
                : '/images/hero/hero-desktop.jpg'
            }
            type="image/jpeg"
          />
          <source
            media="(min-width: 768px)"
            srcSet={
              isDark
                ? '/images/hero/hero-tablet-dark.webp'
                : '/images/hero/hero-tablet.webp'
            }
            type="image/webp"
          />
          <source
            media="(min-width: 768px)"
            srcSet={
              isDark
                ? '/images/hero/hero-tablet-dark.jpg'
                : '/images/hero/hero-tablet.jpg'
            }
            type="image/jpeg"
          />
          <source
            srcSet={
              isDark
                ? '/images/hero/hero-mobile-dark.webp'
                : '/images/hero/hero-mobile.webp'
            }
            type="image/webp"
          />
          <img
            src={
              isDark
                ? '/images/hero/hero-mobile-dark.jpg'
                : '/images/hero/hero-mobile.jpg'
            }
            alt="Danish agricultural landscape"
            className="aspect-video h-full w-full max-w-full object-cover object-center"
            loading="eager"
            decoding="async"
          />
        </picture>
        {/* Overlay for better text readability - adjust opacity for dark mode */}
        <div
          className={`absolute inset-0 ${isDark ? 'bg-background/60' : 'bg-background/40'}`}
        ></div>
      </div>
      <div className="mx-auto max-w-4xl py-18 sm:py-28 lg:py-40">
        <div className="flex flex-col gap-6 text-center">
          <h1 className="text-primary-foreground text-5xl font-bold tracking-tight text-balance sm:text-5xl">
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

          <p className="text-primary-foreground text-sm font-medium text-pretty sm:text-xl/8">
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
            <p className="text-primary-foreground/70 mt-2 text-xs">
              <BarChart3 className="mr-1 inline h-3 w-3" /> Data fra cache •
              Opdateres hver tirsdag
            </p>
          )}
        </div>
      </div>
    </div>
  );
}

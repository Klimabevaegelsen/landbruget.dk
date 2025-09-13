'use client';

import { useState, useEffect, useCallback } from 'react';
import {
  getNextTuesdayExpiration,
  isCacheValidUntilTuesday,
} from '@/lib/cache-utils';

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

interface CachedStats {
  data: HomepageStatistics;
  timestamp: number;
  expiresAt: number; // Tuesday-based expiration
}

const CACHE_KEY = 'homepage_statistics';

export function useHomepageStatsCache() {
  const [cache, setCache] = useState<CachedStats | null>(null);

  // Load cache from localStorage on mount
  useEffect(() => {
    const loadCache = () => {
      try {
        const cachedItem = localStorage.getItem(CACHE_KEY);
        if (cachedItem) {
          const parsed: CachedStats = JSON.parse(cachedItem);

          // Check if cache is still valid (Tuesday-based expiration)
          if (parsed.expiresAt && isCacheValidUntilTuesday(parsed.expiresAt)) {
            setCache(parsed);
          } else {
            // Remove expired cache
            localStorage.removeItem(CACHE_KEY);
          }
        }
      } catch (error) {
        console.warn('Failed to parse cached homepage statistics:', error);
        // Remove invalid cache entry
        localStorage.removeItem(CACHE_KEY);
      }
    };

    loadCache();
  }, []);

  const getCachedData = useCallback((): HomepageStatistics | null => {
    if (!cache) {
      return null;
    }

    // Check if cache is still valid (Tuesday-based expiration)
    if (!cache.expiresAt || !isCacheValidUntilTuesday(cache.expiresAt)) {
      // Remove expired cache
      setCache(null);
      localStorage.removeItem(CACHE_KEY);
      return null;
    }

    return cache.data;
  }, [cache]);

  const setCachedData = useCallback((data: HomepageStatistics) => {
    const cachedItem: CachedStats = {
      data,
      timestamp: Date.now(),
      expiresAt: getNextTuesdayExpiration(),
    };

    // Update in-memory cache
    setCache(cachedItem);

    // Persist to localStorage
    try {
      localStorage.setItem(CACHE_KEY, JSON.stringify(cachedItem));
    } catch (error) {
      console.warn(
        'Failed to cache homepage statistics to localStorage:',
        error
      );
    }
  }, []);

  const clearCache = useCallback(() => {
    setCache(null);
    localStorage.removeItem(CACHE_KEY);
  }, []);

  const isCacheValid = useCallback((): boolean => {
    return cache && cache.expiresAt
      ? isCacheValidUntilTuesday(cache.expiresAt)
      : false;
  }, [cache]);

  const getCacheAge = useCallback((): number | null => {
    return cache ? Date.now() - cache.timestamp : null;
  }, [cache]);

  return {
    getCachedData,
    setCachedData,
    clearCache,
    isCacheValid,
    getCacheAge,
    hasCache: !!cache,
  };
}

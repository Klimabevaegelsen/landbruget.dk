'use client';

import { useState, useEffect, useCallback } from 'react';
import {
  getNextTuesdayExpiration,
  isCacheValidUntilTuesday,
} from '@/lib/cache-utils';

interface RankingItem {
  company_id: string;
  cvr_number: string;
  company_name: string;
  municipality?: string;
  rank: number;
  value: number;
  formatted_value: string;
  year?: number;
}

interface RankingTable {
  id: string;
  title: string;
  category: string;
  description: string;
  unit: string;
  last_updated: string;
  company_count: number;
  items: RankingItem[];
}

interface HomepageRankingsResponse {
  rankings: RankingTable[];
  metadata: {
    generated_at: string;
    total_tables: number;
  };
}

interface CachedData {
  data: HomepageRankingsResponse;
  timestamp: number;
  expiresAt: number; // Tuesday-based expiration
  category: string;
}

const CACHE_KEY_PREFIX = 'homepage_rankings_';

export function useRankingsCache() {
  const [cache, setCache] = useState<Map<string, CachedData>>(new Map());

  // Load cache from localStorage on mount
  useEffect(() => {
    const loadCache = () => {
      const newCache = new Map<string, CachedData>();

      // Load all cached rankings from localStorage
      for (let i = 0; i < localStorage.length; i++) {
        const key = localStorage.key(i);
        if (key?.startsWith(CACHE_KEY_PREFIX)) {
          try {
            const cachedItem = JSON.parse(localStorage.getItem(key) || '');
            const category = key.replace(CACHE_KEY_PREFIX, '');

            // Check if cache is still valid (Tuesday-based expiration)
            if (
              cachedItem.expiresAt &&
              isCacheValidUntilTuesday(cachedItem.expiresAt)
            ) {
              newCache.set(category, cachedItem);
            } else {
              // Remove expired cache
              localStorage.removeItem(key);
            }
          } catch (error) {
            console.warn('Failed to parse cached rankings:', error);
            // Remove invalid cache entry
            localStorage.removeItem(key);
          }
        }
      }

      setCache(newCache);
    };

    loadCache();
  }, []);

  const getCachedData = useCallback(
    (category: string): HomepageRankingsResponse | null => {
      const cached = cache.get(category);

      if (!cached) {
        return null;
      }

      // Check if cache is still valid (Tuesday-based expiration)
      if (!cached.expiresAt || !isCacheValidUntilTuesday(cached.expiresAt)) {
        // Remove expired cache
        const newCache = new Map(cache);
        newCache.delete(category);
        setCache(newCache);
        localStorage.removeItem(`${CACHE_KEY_PREFIX}${category}`);
        return null;
      }

      return cached.data;
    },
    [cache]
  );

  const setCachedData = useCallback(
    (category: string, data: HomepageRankingsResponse) => {
      const cachedItem: CachedData = {
        data,
        timestamp: Date.now(),
        expiresAt: getNextTuesdayExpiration(),
        category,
      };

      // Update in-memory cache
      const newCache = new Map(cache);
      newCache.set(category, cachedItem);
      setCache(newCache);

      // Persist to localStorage
      try {
        localStorage.setItem(
          `${CACHE_KEY_PREFIX}${category}`,
          JSON.stringify(cachedItem)
        );
      } catch (error) {
        console.warn('Failed to cache rankings to localStorage:', error);
      }
    },
    [cache]
  );

  const clearCache = useCallback(
    (category?: string) => {
      if (category) {
        // Clear specific category
        const newCache = new Map(cache);
        newCache.delete(category);
        setCache(newCache);
        localStorage.removeItem(`${CACHE_KEY_PREFIX}${category}`);
      } else {
        // Clear all rankings cache
        setCache(new Map());

        // Remove all cached rankings from localStorage
        const keysToRemove: string[] = [];
        for (let i = 0; i < localStorage.length; i++) {
          const key = localStorage.key(i);
          if (key?.startsWith(CACHE_KEY_PREFIX)) {
            keysToRemove.push(key);
          }
        }

        keysToRemove.forEach((key) => localStorage.removeItem(key));
      }
    },
    [cache]
  );

  const isCacheValid = useCallback(
    (category: string): boolean => {
      const cached = cache.get(category);
      return cached && cached.expiresAt
        ? isCacheValidUntilTuesday(cached.expiresAt)
        : false;
    },
    [cache]
  );

  const getCacheAge = useCallback(
    (category: string): number | null => {
      const cached = cache.get(category);
      return cached ? Date.now() - cached.timestamp : null;
    },
    [cache]
  );

  return {
    getCachedData,
    setCachedData,
    clearCache,
    isCacheValid,
    getCacheAge,
    cacheSize: cache.size,
  };
}

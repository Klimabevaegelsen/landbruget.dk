'use client';

import { useState, useEffect, useCallback } from 'react';
import {
  getNextTuesdayExpiration,
  isCacheValidUntilTuesday,
} from '@/lib/cache-utils';

interface CompanyBasicInfo {
  company_id: string;
  cvr_number: string;
  company_name: string;
  municipality?: string;
  address?: string;
  postal_code?: string;
  city?: string;
  phone?: string;
  email?: string;
  website?: string;
}

interface CompanyDetails {
  company_id: string;
  cvr_number: string;
  company_name: string;
  municipality?: string;
  address?: string;
  postal_code?: string;
  city?: string;
  phone?: string;
  email?: string;
  website?: string;
  employee_count?: number;
  founded_year?: number;
  industry_codes?: string[];
  total_area_ha?: number;
  // Add other relevant fields that might be fetched
}

interface CachedCompanyData {
  basic_info: CompanyBasicInfo;
  details?: CompanyDetails;
  timestamp: number;
  expiresAt: number; // Tuesday-based expiration
  last_accessed: number;
}

interface CompanyCacheStats {
  total_companies: number;
  companies_with_details: number;
  cache_size_mb: number;
  oldest_entry_age: number;
  most_accessed_companies: Array<{
    company_id: string;
    company_name: string;
    access_count: number;
  }>;
}
// Company cache uses Tuesday-based expiration like other caches
const MAX_CACHE_ENTRIES = 500; // Limit to prevent localStorage bloat
const CLEANUP_THRESHOLD = 0.8; // Clean up when cache is 80% full

export function useCompanyCache() {
  const [cache, setCache] = useState<Map<string, CachedCompanyData>>(new Map());
  const [accessCounts, setAccessCounts] = useState<Map<string, number>>(
    new Map()
  );

  // Load cache from localStorage on mount
  useEffect(() => {
    const loadCache = () => {
      const newCache = new Map<string, CachedCompanyData>();
      const newAccessCounts = new Map<string, number>();

      try {
        // Load main cache
        const cacheData = localStorage.getItem('company_cache_main');
        if (cacheData) {
          const parsed = JSON.parse(cacheData);
          Object.entries(parsed).forEach(([companyId, data]) => {
            const cachedData = data as CachedCompanyData;

            // Check if cache is still valid (Tuesday-based expiration)
            if (
              cachedData.expiresAt &&
              isCacheValidUntilTuesday(cachedData.expiresAt)
            ) {
              newCache.set(companyId, cachedData);
            }
          });
        }

        // Load access counts
        const accessData = localStorage.getItem('company_cache_access_counts');
        if (accessData) {
          const parsed = JSON.parse(accessData);
          Object.entries(parsed).forEach(([companyId, count]) => {
            newAccessCounts.set(companyId, count as number);
          });
        }

        setCache(newCache);
        setAccessCounts(newAccessCounts);
      } catch (error) {
        console.warn('Failed to load company cache:', error);
        // Clear corrupted cache
        localStorage.removeItem('company_cache_main');
        localStorage.removeItem('company_cache_access_counts');
      }
    };

    loadCache();
  }, []);

  // Persist cache to localStorage
  const persistCache = useCallback(
    (
      newCache: Map<string, CachedCompanyData>,
      newAccessCounts: Map<string, number>
    ) => {
      try {
        // Convert Map to object for JSON serialization
        const cacheObject = Object.fromEntries(newCache);
        const accessObject = Object.fromEntries(newAccessCounts);

        localStorage.setItem('company_cache_main', JSON.stringify(cacheObject));
        localStorage.setItem(
          'company_cache_access_counts',
          JSON.stringify(accessObject)
        );
      } catch (error) {
        console.warn('Failed to persist company cache:', error);
      }
    },
    []
  );

  // Clean up old or least accessed entries when cache is full
  const cleanupCache = useCallback(
    (
      currentCache: Map<string, CachedCompanyData>,
      currentAccessCounts: Map<string, number>
    ) => {
      if (currentCache.size < MAX_CACHE_ENTRIES * CLEANUP_THRESHOLD) {
        return { cache: currentCache, accessCounts: currentAccessCounts };
      }

      // Sort by last accessed time and access count (least recently used + least accessed first)
      const entries = Array.from(currentCache.entries()).sort((a, b) => {
        const aAccess = currentAccessCounts.get(a[0]) || 0;
        const bAccess = currentAccessCounts.get(b[0]) || 0;

        // First sort by access count (ascending)
        if (aAccess !== bAccess) {
          return aAccess - bAccess;
        }

        // Then by last accessed time (ascending)
        return a[1].last_accessed - b[1].last_accessed;
      });

      // Keep only the most accessed/recent entries
      const keepCount = Math.floor(MAX_CACHE_ENTRIES * 0.7); // Keep 70% of max
      const newCache = new Map(entries.slice(-keepCount));
      const newAccessCounts = new Map();

      // Update access counts to only include kept entries
      newCache.forEach((_, companyId) => {
        const count = currentAccessCounts.get(companyId) || 0;
        newAccessCounts.set(companyId, count);
      });

      console.log(
        `Company cache cleaned up: ${currentCache.size} -> ${newCache.size} entries`
      );

      return { cache: newCache, accessCounts: newAccessCounts };
    },
    []
  );

  // Cache companies from rankings (bulk operation)
  const cacheCompaniesFromRankings = useCallback(
    (
      rankings: Array<{
        items: Array<{
          company_id: string;
          cvr_number: string;
          company_name: string;
          municipality?: string;
        }>;
      }>
    ) => {
      const newCache = new Map(cache);
      const newAccessCounts = new Map(accessCounts);
      let hasChanges = false;

      rankings.forEach((ranking) => {
        ranking.items.forEach((item) => {
          if (!newCache.has(item.company_id)) {
            const companyData: CachedCompanyData = {
              basic_info: {
                company_id: item.company_id,
                cvr_number: item.cvr_number,
                company_name: item.company_name,
                municipality: item.municipality,
              },
              timestamp: Date.now(),
              expiresAt: getNextTuesdayExpiration(),
              last_accessed: Date.now(),
            };

            newCache.set(item.company_id, companyData);
            hasChanges = true;
          }
        });
      });

      if (hasChanges) {
        // Clean up if necessary
        const { cache: cleanedCache, accessCounts: cleanedAccessCounts } =
          cleanupCache(newCache, newAccessCounts);

        setCache(cleanedCache);
        setAccessCounts(cleanedAccessCounts);
        persistCache(cleanedCache, cleanedAccessCounts);
      }
    },
    [cache, accessCounts, cleanupCache, persistCache]
  );

  // Get cached company basic info
  const getCachedCompanyBasicInfo = useCallback(
    (companyId: string): CompanyBasicInfo | null => {
      const cached = cache.get(companyId);

      if (!cached) {
        return null;
      }

      // Check if cache is still valid (Tuesday-based expiration)
      if (!cached.expiresAt || !isCacheValidUntilTuesday(cached.expiresAt)) {
        return null;
      }

      // Update last accessed time and increment access count
      const newCache = new Map(cache);
      const newAccessCounts = new Map(accessCounts);

      cached.last_accessed = Date.now();
      newCache.set(companyId, cached);

      const currentCount = newAccessCounts.get(companyId) || 0;
      newAccessCounts.set(companyId, currentCount + 1);

      setCache(newCache);
      setAccessCounts(newAccessCounts);

      // Persist asynchronously to avoid blocking
      setTimeout(() => persistCache(newCache, newAccessCounts), 0);

      return cached.basic_info;
    },
    [cache, accessCounts, persistCache]
  );

  // Cache detailed company information
  const setCachedCompanyDetails = useCallback(
    (companyId: string, details: CompanyDetails) => {
      const newCache = new Map(cache);
      const cached = newCache.get(companyId);

      if (cached) {
        // Update existing cache with details
        cached.details = details;
        cached.timestamp = Date.now();
        cached.expiresAt = getNextTuesdayExpiration();
        cached.last_accessed = Date.now();
      } else {
        // Create new cache entry
        const companyData: CachedCompanyData = {
          basic_info: {
            company_id: details.company_id,
            cvr_number: details.cvr_number,
            company_name: details.company_name,
            municipality: details.municipality,
          },
          details,
          timestamp: Date.now(),
          expiresAt: getNextTuesdayExpiration(),
          last_accessed: Date.now(),
        };
        newCache.set(companyId, companyData);
      }

      const { cache: cleanedCache, accessCounts: cleanedAccessCounts } =
        cleanupCache(newCache, accessCounts);

      setCache(cleanedCache);
      setAccessCounts(cleanedAccessCounts);
      persistCache(cleanedCache, cleanedAccessCounts);
    },
    [cache, accessCounts, cleanupCache, persistCache]
  );

  // Get cached company details
  const getCachedCompanyDetails = useCallback(
    (companyId: string): CompanyDetails | null => {
      const cached = cache.get(companyId);

      if (!cached || !cached.details) {
        return null;
      }

      // Check if cache is still valid
      if (!cached.expiresAt || !isCacheValidUntilTuesday(cached.expiresAt)) {
        return null;
      }

      // Update access statistics
      const newCache = new Map(cache);
      const newAccessCounts = new Map(accessCounts);

      cached.last_accessed = Date.now();
      newCache.set(companyId, cached);

      const currentCount = newAccessCounts.get(companyId) || 0;
      newAccessCounts.set(companyId, currentCount + 1);

      setCache(newCache);
      setAccessCounts(newAccessCounts);

      setTimeout(() => persistCache(newCache, newAccessCounts), 0);

      return cached.details;
    },
    [cache, accessCounts, persistCache]
  );

  // Clear cache
  const clearCompanyCache = useCallback(
    (companyId?: string) => {
      if (companyId) {
        // Clear specific company
        const newCache = new Map(cache);
        const newAccessCounts = new Map(accessCounts);

        newCache.delete(companyId);
        newAccessCounts.delete(companyId);

        setCache(newCache);
        setAccessCounts(newAccessCounts);
        persistCache(newCache, newAccessCounts);
      } else {
        // Clear all
        setCache(new Map());
        setAccessCounts(new Map());
        localStorage.removeItem('company_cache_main');
        localStorage.removeItem('company_cache_access_counts');
      }
    },
    [cache, accessCounts, persistCache]
  );

  // Get cache statistics
  const getCacheStats = useCallback((): CompanyCacheStats => {
    const totalCompanies = cache.size;
    const companiesWithDetails = Array.from(cache.values()).filter(
      (c) => c.details
    ).length;

    // Calculate cache size (rough estimate)
    const cacheString = JSON.stringify(Object.fromEntries(cache));
    const cacheSizeMB = new Blob([cacheString]).size / (1024 * 1024);

    // Find oldest entry
    const oldestEntry = Array.from(cache.values()).reduce(
      (oldest, current) => {
        return current.timestamp < oldest.timestamp ? current : oldest;
      },
      { timestamp: Date.now() } as CachedCompanyData
    );

    const oldestEntryAge = Date.now() - oldestEntry.timestamp;

    // Get most accessed companies
    const mostAccessed = Array.from(accessCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10)
      .map(([companyId, count]) => {
        const cached = cache.get(companyId);
        return {
          company_id: companyId,
          company_name: cached?.basic_info.company_name || 'Unknown',
          access_count: count,
        };
      });

    return {
      total_companies: totalCompanies,
      companies_with_details: companiesWithDetails,
      cache_size_mb: Math.round(cacheSizeMB * 100) / 100,
      oldest_entry_age: oldestEntryAge,
      most_accessed_companies: mostAccessed,
    };
  }, [cache, accessCounts]);

  // Check if company is cached
  const isCompanyCached = useCallback(
    (companyId: string, includeDetails: boolean = false): boolean => {
      const cached = cache.get(companyId);
      if (!cached) return false;

      // Check if cache is still valid
      if (!cached.expiresAt || !isCacheValidUntilTuesday(cached.expiresAt)) {
        return false;
      }

      return includeDetails ? !!cached.details : true;
    },
    [cache]
  );

  // Get company details for UI display (combines basic info with any cached details)
  const getCompanyForDisplay = useCallback(
    (companyId: string) => {
      const cached = cache.get(companyId);

      if (!cached) {
        return null;
      }

      // Check if cache is still valid
      if (!cached.expiresAt || !isCacheValidUntilTuesday(cached.expiresAt)) {
        return null;
      }

      // Return the most complete information available
      return cached.details || cached.basic_info;
    },
    [cache]
  );

  return {
    cacheCompaniesFromRankings,
    getCachedCompanyBasicInfo,
    setCachedCompanyDetails,
    getCachedCompanyDetails,
    getCompanyForDisplay,
    clearCompanyCache,
    getCacheStats,
    isCompanyCached,
    cacheSize: cache.size,
  };
}

'use client';

import { useEffect, useCallback } from 'react';
import { useMapStore } from '@/stores/map-store';
import { useDataStore } from '@/stores/data-store';

export function useDataFetching() {
  const { selectedYear, cumulativeMode } = useMapStore();
  const { 
    fetchH3Data, 
    fetchBNBOData, 
    fetchBBRData, 
    preloadAdjacentYears,
    isLoading,
    error,
    setLoading,
    setError
  } = useDataStore();
  
  // Main data fetching function
  const fetchAllData = useCallback(async () => {
    setLoading(true);
    setError(null);
    
    try {
      // Fetch H3 data based on current settings
      const h3DataPromise = fetchH3Data(selectedYear, cumulativeMode);
      
      // Fetch BNBO and BBR data (these don't depend on year/mode)
      const bnboDataPromise = fetchBNBOData();
      const bbrDataPromise = fetchBBRData();
      
      // Wait for all data to load
      await Promise.all([
        h3DataPromise,
        bnboDataPromise,
        bbrDataPromise
      ]);
      
      // Preload adjacent years for smooth transitions
      preloadAdjacentYears(selectedYear);
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch data');
      console.error('Data fetching error:', err);
    } finally {
      setLoading(false);
    }
  }, [selectedYear, cumulativeMode, fetchH3Data, fetchBNBOData, fetchBBRData, preloadAdjacentYears, setLoading, setError]);
  
  // Fetch data when dependencies change
  useEffect(() => {
    fetchAllData();
  }, [fetchAllData]);
  
  // Refresh data manually
  const refreshData = useCallback(() => {
    fetchAllData();
  }, [fetchAllData]);
  
  return {
    isLoading,
    error,
    refreshData,
    hasData: !isLoading && !error
  };
} 
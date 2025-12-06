'use client';

import { useEffect, useCallback } from 'react';

interface PerformanceMetrics {
  renderTime: number;
  dataFetchTime: number;
  interactionTime: number;
  memoryUsage: number;
}

export function usePerformance() {
  const trackPerformance = useCallback((event: string, metrics: Partial<PerformanceMetrics>) => {
    // Send to analytics service (placeholder for actual implementation)
    if (typeof window !== 'undefined' && (window as unknown as Record<string, unknown>).gtag) {
      ((window as unknown as Record<string, unknown>).gtag as (...args: unknown[]) => void)('event', event, {
        custom_parameter_1: metrics.renderTime,
        custom_parameter_2: metrics.dataFetchTime,
        custom_parameter_3: metrics.interactionTime,
      });
    }
    
    // Send to PostHog for detailed analysis (placeholder)
    if (typeof window !== 'undefined' && (window as unknown as Record<string, unknown>).posthog) {
      ((window as unknown as Record<string, unknown>).posthog as Record<string, (...args: unknown[]) => void>).capture(event, metrics);
    }
    
    // Log to console in development
    if (process.env.NODE_ENV === 'development') {
      console.log(`Performance: ${event}`, metrics);
    }
  }, []);
  
  const measureRenderTime = useCallback((componentName: string) => {
    const startTime = performance.now();
    
    return () => {
      const endTime = performance.now();
      trackPerformance(`${componentName}_render`, {
        renderTime: endTime - startTime,
      });
    };
  }, [trackPerformance]);
  
  const measureDataFetch = useCallback(async <T>(
    fetchFn: () => Promise<T>,
    operationName: string
  ): Promise<T> => {
    const startTime = performance.now();
    
    try {
      const result = await fetchFn();
      const endTime = performance.now();
      
      trackPerformance(`${operationName}_fetch`, {
        dataFetchTime: endTime - startTime,
      });
      
      return result;
    } catch (error) {
      const endTime = performance.now();
      
      trackPerformance(`${operationName}_fetch_error`, {
        dataFetchTime: endTime - startTime,
      });
      
      throw error;
    }
  }, [trackPerformance]);
  
  const measureInteraction = useCallback((interactionName: string) => {
    const startTime = performance.now();
    
    return () => {
      const endTime = performance.now();
      trackPerformance(`${interactionName}_interaction`, {
        interactionTime: endTime - startTime,
      });
    };
  }, [trackPerformance]);
  
  // Monitor memory usage
  useEffect(() => {
    if (typeof window !== 'undefined' && 'memory' in performance) {
      const interval = setInterval(() => {
        const memory = (performance as unknown as { memory: { usedJSHeapSize: number } }).memory;
        trackPerformance('memory_usage', {
          memoryUsage: memory.usedJSHeapSize / 1024 / 1024, // MB
        });
      }, 30000); // Every 30 seconds
      
      return () => clearInterval(interval);
    }
  }, [trackPerformance]);
  
  // Monitor Core Web Vitals
  useEffect(() => {
    if (typeof window !== 'undefined') {
      // Largest Contentful Paint (LCP)
      const observer = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        const lastEntry = entries[entries.length - 1];
        trackPerformance('lcp', { renderTime: lastEntry.startTime });
      });
      
      try {
        observer.observe({ entryTypes: ['largest-contentful-paint'] });
      } catch {
        // Fallback for browsers that don't support LCP
      }
      
      // First Input Delay (FID)
      const fidObserver = new PerformanceObserver((list) => {
        const entries = list.getEntries();
        entries.forEach((entry) => {
          const fidEntry = entry as unknown as { processingStart: number; startTime: number };
          trackPerformance('fid', { interactionTime: fidEntry.processingStart - fidEntry.startTime });
        });
      });
      
      try {
        fidObserver.observe({ entryTypes: ['first-input'] });
      } catch {
        // Fallback for browsers that don't support FID
      }
      
      return () => {
        observer.disconnect();
        fidObserver.disconnect();
      };
    }
  }, [trackPerformance]);
  
  return {
    trackPerformance,
    measureRenderTime,
    measureDataFetch,
    measureInteraction,
  };
} 
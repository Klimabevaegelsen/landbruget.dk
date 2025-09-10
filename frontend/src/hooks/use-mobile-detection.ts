'use client';

import { useState, useEffect } from 'react';

/**
 * Hook to detect mobile devices and screen sizes
 * Following Midday's mobile-first approach
 */
export function useMobileDetection() {
  const [isMobile, setIsMobile] = useState(false);
  const [isTablet, setIsTablet] = useState(false);
  const [screenSize, setScreenSize] = useState<'mobile' | 'tablet' | 'desktop'>(
    'desktop'
  );

  useEffect(() => {
    const checkScreenSize = () => {
      const width = window.innerWidth;

      // Mobile: < 768px
      // Tablet: 768px - 1024px
      // Desktop: > 1024px
      const mobile = width < 768;
      const tablet = width >= 768 && width < 1024;

      setIsMobile(mobile);
      setIsTablet(tablet);
      setScreenSize(mobile ? 'mobile' : tablet ? 'tablet' : 'desktop');
    };

    // Check on mount
    checkScreenSize();

    // Listen for resize events
    window.addEventListener('resize', checkScreenSize);

    // Listen for orientation changes on mobile
    window.addEventListener('orientationchange', () => {
      // Delay to allow for orientation change to complete
      setTimeout(checkScreenSize, 100);
    });

    return () => {
      window.removeEventListener('resize', checkScreenSize);
      window.removeEventListener('orientationchange', checkScreenSize);
    };
  }, []);

  return {
    isMobile,
    isTablet,
    isDesktop: !isMobile && !isTablet,
    screenSize,
  };
}

/**
 * Hook for media queries with SSR support
 */
export function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(false);

  useEffect(() => {
    const media = window.matchMedia(query);

    if (media.matches !== matches) {
      setMatches(media.matches);
    }

    const listener = () => setMatches(media.matches);
    media.addEventListener('change', listener);

    return () => media.removeEventListener('change', listener);
  }, [matches, query]);

  return matches;
}

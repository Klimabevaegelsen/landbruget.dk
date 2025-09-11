'use client';

import { useTheme } from '@/components/theme/theme-provider';
import { useEffect, useState } from 'react';

/**
 * Available map style themes
 */
export type MapStyleTheme = 'light' | 'dark';

/**
 * Map style URLs for different themes
 */
const MAP_STYLES = {
  light: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
  dark: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
} as const;

/**
 * Hook to get theme-aware map styles for MapLibre/react-map-gl
 *
 * @returns Object containing the current map style URL and theme info
 */
export function useMapTheme() {
  const { theme } = useTheme();
  const [mapStyleTheme, setMapStyleTheme] = useState<MapStyleTheme>('light');

  useEffect(() => {
    // Determine the effective theme
    let effectiveTheme: MapStyleTheme = 'light';

    if (theme === 'dark') {
      effectiveTheme = 'dark';
    } else if (theme === 'system') {
      // Check system preference
      if (typeof window !== 'undefined') {
        const systemPrefersDark = window.matchMedia(
          '(prefers-color-scheme: dark)'
        ).matches;
        effectiveTheme = systemPrefersDark ? 'dark' : 'light';
      }
    }

    setMapStyleTheme(effectiveTheme);
  }, [theme]);

  // Listen for system theme changes when theme is 'system'
  useEffect(() => {
    if (theme !== 'system' || typeof window === 'undefined') return;

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    const handleChange = (e: MediaQueryListEvent) => {
      setMapStyleTheme(e.matches ? 'dark' : 'light');
    };

    mediaQuery.addEventListener('change', handleChange);

    return () => {
      mediaQuery.removeEventListener('change', handleChange);
    };
  }, [theme]);

  return {
    /**
     * The current map style URL based on the theme
     */
    mapStyle: MAP_STYLES[mapStyleTheme],

    /**
     * The current effective map theme ('light' or 'dark')
     */
    mapStyleTheme,

    /**
     * Whether the map is currently using dark theme
     */
    isDarkMode: mapStyleTheme === 'dark',

    /**
     * Get a specific map style URL
     */
    getMapStyle: (styleTheme: MapStyleTheme) => MAP_STYLES[styleTheme],
  };
}

/**
 * Alternative map style options for different use cases
 */
export const ALTERNATIVE_MAP_STYLES = {
  light: {
    voyager: 'https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json',
    positron: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
    osm: 'https://tiles.stadiamaps.com/styles/osm_bright.json',
  },
  dark: {
    darkMatter:
      'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
    osmDark: 'https://tiles.stadiamaps.com/styles/alidade_smooth_dark.json',
  },
} as const;

/**
 * Hook for advanced map theming with custom style options
 */
export function useAdvancedMapTheme(
  lightStyle: keyof typeof ALTERNATIVE_MAP_STYLES.light = 'voyager',
  darkStyle: keyof typeof ALTERNATIVE_MAP_STYLES.dark = 'darkMatter'
) {
  const { theme } = useTheme();
  const [mapStyleTheme, setMapStyleTheme] = useState<MapStyleTheme>('light');

  useEffect(() => {
    let effectiveTheme: MapStyleTheme = 'light';

    if (theme === 'dark') {
      effectiveTheme = 'dark';
    } else if (theme === 'system') {
      if (typeof window !== 'undefined') {
        const systemPrefersDark = window.matchMedia(
          '(prefers-color-scheme: dark)'
        ).matches;
        effectiveTheme = systemPrefersDark ? 'dark' : 'light';
      }
    }

    setMapStyleTheme(effectiveTheme);
  }, [theme]);

  useEffect(() => {
    if (theme !== 'system' || typeof window === 'undefined') return;

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

    const handleChange = (e: MediaQueryListEvent) => {
      setMapStyleTheme(e.matches ? 'dark' : 'light');
    };

    mediaQuery.addEventListener('change', handleChange);

    return () => {
      mediaQuery.removeEventListener('change', handleChange);
    };
  }, [theme]);

  const mapStyle =
    mapStyleTheme === 'dark'
      ? ALTERNATIVE_MAP_STYLES.dark[darkStyle]
      : ALTERNATIVE_MAP_STYLES.light[lightStyle];

  return {
    mapStyle,
    mapStyleTheme,
    isDarkMode: mapStyleTheme === 'dark',
  };
}

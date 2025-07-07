'use client';

import { useState, useCallback, useEffect, useMemo } from 'react';
import { DEFAULT_VIEWPORT } from '@/lib/utils';

export interface ViewState {
  latitude: number;
  longitude: number;
  zoom: number;
  bearing?: number;
  pitch?: number;
  transitionDuration?: number;
  transitionInterpolator?: unknown;
}

export interface ViewportBounds {
  north: number;
  south: number;
  east: number;
  west: number;
}

export interface ViewportInfo {
  viewport: ViewState;
  bounds: ViewportBounds | null;
  zoomLevel: 'country' | 'region' | 'city' | 'street';
  isTransitioning: boolean;
  center: [number, number];
}

// Default viewport for Denmark
const DENMARK_BOUNDS = {
  north: 57.75,
  south: 54.56,
  east: 15.16,
  west: 8.08
};

export function useViewport() {
  const [viewport, setViewport] = useState<ViewState>(DEFAULT_VIEWPORT);
  const [bounds, setBounds] = useState<ViewportBounds | null>(null);
  const [isMoving, setIsMoving] = useState(false);
  const [isTransitioning, setIsTransitioning] = useState(false);
  const [transitionStartTime, setTransitionStartTime] = useState<number | null>(null);
  
  // Calculate bounds from viewport
  const calculateBounds = useCallback((vp: ViewState): ViewportBounds => {
    // Approximate bounds calculation based on zoom level
    // This is a simplified calculation - in a real implementation,
    // you'd use proper map projection calculations
    const latRange = 180 / Math.pow(2, vp.zoom);
    const lngRange = 360 / Math.pow(2, vp.zoom);
    
    return {
      north: Math.min(90, vp.latitude + latRange / 2),
      south: Math.max(-90, vp.latitude - latRange / 2),
      east: Math.min(180, vp.longitude + lngRange / 2),
      west: Math.max(-180, vp.longitude - lngRange / 2),
    };
  }, []);
  
  // Update bounds when viewport changes
  useEffect(() => {
    const newBounds = calculateBounds(viewport);
    setBounds(newBounds);
  }, [viewport, calculateBounds]);
  
  // Viewport change handler
  const handleViewportChange = useCallback((newViewport: ViewState) => {
    setViewport(newViewport);
  }, []);
  
  // Movement handlers
  const handleMoveStart = useCallback(() => {
    setIsMoving(true);
  }, []);
  
  const handleMoveEnd = useCallback(() => {
    setIsMoving(false);
  }, []);
  
  // Zoom to specific location
  const zoomTo = useCallback((latitude: number, longitude: number, zoom: number = 12) => {
    setViewport(prev => ({
      ...prev,
      latitude,
      longitude,
      zoom,
    }));
  }, []);
  
  // Zoom to bounds
  const zoomToBounds = useCallback((targetBounds: ViewportBounds, padding: number = 0.1) => {
    const centerLat = (targetBounds.north + targetBounds.south) / 2;
    const centerLng = (targetBounds.east + targetBounds.west) / 2;
    
    // Calculate zoom level to fit bounds
    const latRange = targetBounds.north - targetBounds.south + padding;
    const lngRange = targetBounds.east - targetBounds.west + padding;
    const maxRange = Math.max(latRange, lngRange);
    const zoom = Math.max(1, Math.min(20, Math.log2(360 / maxRange)));
    
    setViewport(prev => ({
      ...prev,
      latitude: centerLat,
      longitude: centerLng,
      zoom,
    }));
  }, []);
  
  // Reset to default viewport
  const resetViewport = useCallback(() => {
    setViewport(DEFAULT_VIEWPORT);
  }, []);
  
  // Check if a point is in current viewport
  const isInViewport = useCallback((lat: number, lng: number, buffer: number = 0): boolean => {
    if (!bounds) return false;
    
    return (
      lat >= bounds.south - buffer &&
      lat <= bounds.north + buffer &&
      lng >= bounds.west - buffer &&
      lng <= bounds.east + buffer
    );
  }, [bounds]);
  
  // Get zoom level category for performance optimization
  const zoomLevel = useMemo(() => {
    const zoom = viewport.zoom;
    if (zoom < 8) return 'country';
    if (zoom < 12) return 'region';
    if (zoom < 16) return 'city';
    return 'street';
  }, [viewport.zoom]);

  // Check if viewport is within Denmark bounds
  const isWithinDenmark = useMemo(() => {
    const { latitude, longitude } = viewport;
    return (
      latitude >= DENMARK_BOUNDS.south &&
      latitude <= DENMARK_BOUNDS.north &&
      longitude >= DENMARK_BOUNDS.west &&
      longitude <= DENMARK_BOUNDS.east
    );
  }, [viewport]);

  // Update viewport with validation and transition handling
  const updateViewport = useCallback((newViewport: Partial<ViewState>) => {
    setViewport(prev => {
      const updated = { ...prev, ...newViewport };
      
      // Validate bounds (keep within reasonable limits)
      updated.latitude = Math.max(-85, Math.min(85, updated.latitude));
      updated.longitude = ((updated.longitude + 180) % 360) - 180; // Normalize to -180 to 180
      updated.zoom = Math.max(1, Math.min(20, updated.zoom));
      
      return updated;
    });

    // Handle transition state
    if (newViewport.transitionDuration && newViewport.transitionDuration > 0) {
      setIsTransitioning(true);
      setTransitionStartTime(Date.now());
      
      setTimeout(() => {
        setIsTransitioning(false);
        setTransitionStartTime(null);
      }, newViewport.transitionDuration);
    }
  }, []);

  // Fly to specific location with smooth transition
  const flyTo = useCallback((
    latitude: number, 
    longitude: number, 
    zoom?: number, 
    duration: number = 1000
  ) => {
    updateViewport({
      latitude,
      longitude,
      zoom: zoom || viewport.zoom,
      transitionDuration: duration,
      bearing: 0,
      pitch: 0
    });
  }, [viewport.zoom, updateViewport]);

  // Zoom in/out by steps
  const zoomIn = useCallback((steps: number = 1) => {
    zoomTo(viewport.latitude, viewport.longitude, viewport.zoom + steps);
  }, [viewport.latitude, viewport.longitude, viewport.zoom, zoomTo]);

  const zoomOut = useCallback((steps: number = 1) => {
    zoomTo(viewport.latitude, viewport.longitude, viewport.zoom - steps);
  }, [viewport.latitude, viewport.longitude, viewport.zoom, zoomTo]);

  // Fit bounds to show specific area
  const fitBounds = useCallback((
    bounds: ViewportBounds, 
    padding: number = 0.1,
    duration: number = 1000
  ) => {
    const { north, south, east, west } = bounds;
    
    // Calculate center
    const centerLat = (north + south) / 2;
    const centerLng = (east + west) / 2;
    
    // Calculate zoom level to fit bounds (simplified)
    const latDiff = north - south;
    const lngDiff = east - west;
    const maxDiff = Math.max(latDiff, lngDiff);
    
    // Approximate zoom calculation (would need proper implementation)
    const zoom = Math.max(1, Math.min(18, Math.log2(360 / (maxDiff * (1 + padding)))));
    
    flyTo(centerLat, centerLng, zoom, duration);
  }, [flyTo]);

  // Get viewport info for performance monitoring
  const getViewportInfo = useCallback((): ViewportInfo => ({
    viewport,
    bounds,
    zoomLevel: zoomLevel as 'country' | 'region' | 'city' | 'street',
    isTransitioning,
    center: [viewport.longitude, viewport.latitude]
  }), [viewport, bounds, zoomLevel, isTransitioning]);

  // Calculate distance between two points (Haversine formula)
  const calculateDistance = useCallback((
    lat1: number, 
    lng1: number, 
    lat2: number, 
    lng2: number
  ): number => {
    const R = 6371; // Earth's radius in kilometers
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLng = (lng2 - lng1) * Math.PI / 180;
    
    const a = 
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
      Math.sin(dLng / 2) * Math.sin(dLng / 2);
    
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    return R * c;
  }, []);

  // Performance monitoring
  useEffect(() => {
    if (transitionStartTime) {
      const checkTransition = () => {
        const elapsed = Date.now() - transitionStartTime;
        if (elapsed > 100) { // Check every 100ms during transition
          // Could emit performance metrics here
          console.log(`Viewport transition: ${elapsed}ms elapsed`);
        }
      };
      
      const interval = setInterval(checkTransition, 100);
      return () => clearInterval(interval);
    }
  }, [transitionStartTime]);

  return {
    viewport,
    bounds,
    isMoving,
    isTransitioning,
    isWithinDenmark,
    zoomLevel,
    handleViewportChange,
    handleMoveStart,
    handleMoveEnd,
    zoomTo,
    zoomToBounds,
    resetViewport,
    isInViewport,
    getViewportInfo,
    calculateDistance,
    zoomIn,
    zoomOut,
    fitBounds,
    DEFAULT_VIEWPORT,
    DENMARK_BOUNDS
  };
} 
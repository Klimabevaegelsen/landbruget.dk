'use client';

import { useState, useEffect, useCallback } from 'react';
import { DEFAULT_VIEWPORT } from '@/lib/utils';

interface SimpleViewportState {
  latitude: number;
  longitude: number;
  zoom: number;
}

interface SimpleViewportBounds {
  north: number;
  south: number;
  east: number;
  west: number;
}

export function useSimpleViewport() {
  const [viewport, setViewport] = useState<SimpleViewportState>(DEFAULT_VIEWPORT);
  const [bounds, setBounds] = useState<SimpleViewportBounds | null>(null);
  const [isMoving, setIsMoving] = useState(false);
  
  // Calculate bounds from viewport
  const calculateBounds = useCallback((vp: SimpleViewportState): SimpleViewportBounds => {
    // Simplified bounds calculation based on zoom level
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
  const handleViewportChange = useCallback((newViewport: SimpleViewportState) => {
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
    setViewport({
      latitude,
      longitude,
      zoom,
    });
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
  
  return {
    viewport,
    bounds,
    isMoving,
    handleViewportChange,
    handleMoveStart,
    handleMoveEnd,
    zoomTo,
    resetViewport,
    isInViewport,
  };
} 
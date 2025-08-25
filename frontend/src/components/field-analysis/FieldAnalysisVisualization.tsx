"use client";

import React, { useState, useEffect, useCallback } from "react";
import dynamic from "next/dynamic";
import { LayerControlPanel } from "./LayerControlPanel";
import { FieldDetailsPanel } from "./FieldDetailsPanel";
import { LoadingState } from "./LoadingState";
import { FieldAnalysisData, LayerVisibility, FilterState } from "./types";

// Dynamically import the map component to avoid SSR issues
const FieldAnalysisMap = dynamic(() => import("./FieldAnalysisMap"), {
  ssr: false,
  loading: () => <LoadingState message="Indlæser kort..." />,
});



export default function FieldAnalysisVisualization() {
  const [isClient, setIsClient] = useState(false);

  // State management
  const [layerVisibility, setLayerVisibility] = useState<LayerVisibility>({
    fields: true,
    bnbo: true,
    wetlands: false,
    water_projects: false,
    buildings: true,
  });

  const [filterState, setFilterState] = useState<FilterState>({
    kommune: [],
    cropTypes: [],
    organicOnly: false,
    areaRange: [0, 1000],
    pesticideThreshold: 0,
    pfasThreshold: 0,
    diquatThreshold: 0,
    glyphosateThreshold: 0,
    chemicalFilter: 'all',
    visualizationMode: 'total_pesticide_belastning',
    colorUnit: 'belastning',
    useDecileColoring: true,
  });

  const [selectedField, setSelectedField] = useState<FieldAnalysisData | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [mobileControlsOpen, setMobileControlsOpen] = useState(false);

  // PMTiles URLs from environment variables (Cloudflare R2)
  const pmtilesUrls = {
    fields: process.env.NEXT_PUBLIC_FIELD_ANALYSIS_PMTILES_URL || '',
    bnbo: process.env.NEXT_PUBLIC_BNBO_PMTILES_URL || '',
    wetlands: process.env.NEXT_PUBLIC_WETLANDS_PMTILES_URL || '',
    water_projects: process.env.NEXT_PUBLIC_WATER_PROJECTS_PMTILES_URL || '',
    buildings: process.env.NEXT_PUBLIC_BUILDINGS_PMTILES_URL || '',
  };

  // Ensure client-side only rendering
  useEffect(() => {
    setIsClient(true);
  }, []);

  // Handle orientation changes on mobile
  useEffect(() => {
    const handleOrientationChange = () => {
      // Close mobile panels on orientation change for better UX
      setMobileControlsOpen(false);
    };

    window.addEventListener('orientationchange', handleOrientationChange);
    return () => window.removeEventListener('orientationchange', handleOrientationChange);
  }, []);

  // Initialize visualization
  useEffect(() => {
    if (!isClient) return;

    const initializeVisualization = async () => {
      try {
        setIsLoading(true);
        // Kepler.gl will handle PMTiles loading
        setIsLoading(false);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Fejl ved indlæsning af data");
        setIsLoading(false);
      }
    };

    initializeVisualization();
  }, [isClient]);

  // Handle layer visibility changes
  const handleLayerToggle = useCallback((layerName: keyof LayerVisibility) => {
    setLayerVisibility(prev => ({
      ...prev,
      [layerName]: !prev[layerName],
    }));
  }, []);

  // Handle filter changes
  const handleFilterChange = useCallback((newFilters: Partial<FilterState>) => {
    setFilterState(prev => ({ ...prev, ...newFilters }));
  }, []);

  // Handle field selection
  const handleFieldSelect = useCallback((fieldData: FieldAnalysisData) => {
    setSelectedField(fieldData);
    // Auto-close mobile controls when field is selected for better UX
    setMobileControlsOpen(false);
  }, []);

  // Handle escape key and prevent body scroll
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (selectedField) {
          setSelectedField(null);
        } else if (mobileControlsOpen) {
          setMobileControlsOpen(false);
        }
      }
    };

    // Prevent body scroll when modals are open
    if (mobileControlsOpen || selectedField) {
      document.body.style.overflow = 'hidden';
      document.addEventListener('keydown', handleKeyDown);
    } else {
      document.body.style.overflow = 'unset';
    }

    return () => {
      document.body.style.overflow = 'unset';
      document.removeEventListener('keydown', handleKeyDown);
    };
  }, [mobileControlsOpen, selectedField]);

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen bg-gray-50 p-4">
        <div className="text-center max-w-md">
          <div className="text-red-600 text-xl mb-2">⚠️ Fejl</div>
          <div className="text-gray-700 mb-4 text-sm lg:text-base">{error}</div>
          <button
            onClick={() => window.location.reload()}
            className="px-6 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 active:bg-blue-800 transition-colors text-base font-medium min-h-[44px]"
          >
            Genindlæs
          </button>
        </div>
      </div>
    );
  }

  // Prevent hydration mismatch by not rendering until client-side
  if (!isClient) {
    return <LoadingState message="Indlæser..." />;
  }

  return (
    <div className="relative h-screen flex flex-col lg:flex-row">
      {/* Mobile Control Panel Toggle */}
      <div className="lg:hidden absolute top-4 left-4 z-30" style={{ top: 'max(1rem, env(safe-area-inset-top))' }}>
        <button
          onClick={() => setMobileControlsOpen(!mobileControlsOpen)}
          className="bg-white shadow-lg rounded-lg p-3 hover:bg-gray-50 active:bg-gray-100 transition-colors min-h-[44px] min-w-[44px] flex items-center justify-center"
          aria-label="Toggle controls"
        >
          <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 6h16M4 12h16M4 18h16" />
          </svg>
        </button>
      </div>

      {/* Left Control Panel - Desktop: sidebar, Mobile: overlay */}
      <div className={`
        ${mobileControlsOpen ? 'block' : 'hidden'} lg:block
        fixed lg:relative inset-0 lg:inset-auto
        w-full lg:w-80 h-full lg:h-auto
        bg-white shadow-lg z-30 lg:z-10
        overflow-y-auto
        lg:shadow-lg
      `} style={{
        paddingTop: mobileControlsOpen ? 'env(safe-area-inset-top)' : undefined,
        paddingBottom: mobileControlsOpen ? 'env(safe-area-inset-bottom)' : undefined
      }}>
        {/* Mobile close button */}
        <div className="lg:hidden flex justify-between items-center p-4 border-b bg-white sticky top-0 z-10">
          <h2 className="text-lg font-semibold">Kortlag og filtre</h2>
          <button
            onClick={() => setMobileControlsOpen(false)}
            className="p-2 hover:bg-gray-100 active:bg-gray-200 rounded-full min-h-[44px] min-w-[44px] flex items-center justify-center"
            aria-label="Luk kontrolpanel"
          >
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>

        <LayerControlPanel
          layerVisibility={layerVisibility}
          filterState={filterState}
          onLayerToggle={handleLayerToggle}
          onFilterChange={handleFilterChange}
        />
      </div>

      {/* Mobile Controls Backdrop */}
      {mobileControlsOpen && (
        <div
          className="lg:hidden fixed inset-0 bg-black bg-opacity-50 z-20"
          onClick={() => setMobileControlsOpen(false)}
        />
      )}

      {/* Main Map Area */}
      <div className="flex-1 relative">
        {isLoading ? (
          <LoadingState message="Indlæser kortdata..." />
        ) : (
          <FieldAnalysisMap
            pmtilesUrls={pmtilesUrls}
            layerVisibility={layerVisibility}
            filterState={filterState}
            onFieldSelect={handleFieldSelect}
          />
        )}
      </div>

      {/* Right Details Panel - Desktop: sidebar, Mobile: overlay */}
      {selectedField && (
        <>
          <div className={`
            fixed lg:relative inset-0 lg:inset-auto
            w-full lg:w-96 h-full lg:h-auto
            bg-white shadow-lg z-30 lg:z-10
            overflow-y-auto
            lg:shadow-lg
          `} style={{
            paddingTop: 'env(safe-area-inset-top)',
            paddingBottom: 'env(safe-area-inset-bottom)'
          }}>
            <FieldDetailsPanel
              fieldData={selectedField}
              onClose={() => setSelectedField(null)}
            />
          </div>

          {/* Mobile Details Backdrop */}
          <div
            className="lg:hidden fixed inset-0 bg-black bg-opacity-50 z-20"
            onClick={() => setSelectedField(null)}
          />
        </>
      )}
    </div>
  );
}

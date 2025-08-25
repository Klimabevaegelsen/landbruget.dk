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
  }, []);

  if (error) {
    return (
      <div className="flex items-center justify-center h-screen bg-gray-50">
        <div className="text-center">
          <div className="text-red-600 text-xl mb-2">⚠️ Fejl</div>
          <div className="text-gray-700">{error}</div>
          <button
            onClick={() => window.location.reload()}
            className="mt-4 px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
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
    <div className="relative h-screen flex">
      {/* Left Control Panel */}
      <div className="w-80 bg-white shadow-lg z-10 overflow-y-auto">
        <LayerControlPanel
          layerVisibility={layerVisibility}
          filterState={filterState}
          onLayerToggle={handleLayerToggle}
          onFilterChange={handleFilterChange}
        />
      </div>

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

      {/* Right Details Panel */}
      {selectedField && (
        <div className="w-96 bg-white shadow-lg z-10 overflow-y-auto">
          <FieldDetailsPanel
            fieldData={selectedField}
            onClose={() => setSelectedField(null)}
          />
        </div>
      )}
    </div>
  );
}

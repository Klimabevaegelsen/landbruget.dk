'use client';

import React, { useEffect, useMemo, useCallback, useTransition, Suspense } from 'react';
import { KeplerGl } from 'kepler.gl';
import { addDataToMap } from 'kepler.gl/actions';
import { useDispatch } from 'react-redux';
import { ErrorBoundary } from 'react-error-boundary';
import { motion, AnimatePresence } from 'framer-motion';
import { generateKeplerConfig } from '@/lib/kepler-config';
import { useViewport } from '@/hooks/use-viewport';
import { LayerControls } from './LayerControls';
import { HoverTooltip } from '../overlays/HoverTooltip';

interface ViewState {
  latitude: number;
  longitude: number;
  zoom: number;
  bearing?: number;
  pitch?: number;
}

interface KeplerMapProps {
  initialViewState?: ViewState;
  h3Data?: any[];
  bnboData?: any[];
  bbrData?: any[];
  selectedYear: number;
  showPesticides: boolean;
  showPFAS: boolean;
  showBNBO: boolean;
  showBBR: boolean;
  cumulativeMode: boolean;
  onHover?: (info: any) => void;
  onClick?: (info: any) => void;
}

function MapErrorFallback({ error, resetErrorBoundary }: { error: Error; resetErrorBoundary: () => void }) {
  return (
    <div className="flex h-full w-full items-center justify-center bg-gray-50">
      <div className="text-center p-8">
        <h2 className="text-lg font-semibold text-gray-900 mb-2">Map Loading Error</h2>
        <p className="text-sm text-gray-600 mb-4">{error.message}</p>
        <button
          onClick={resetErrorBoundary}
          className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          Try Again
        </button>
      </div>
    </div>
  );
}

function MapLoadingFallback() {
  return (
    <div className="flex h-full w-full items-center justify-center bg-gray-50">
      <motion.div
        className="text-center"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ duration: 0.3 }}
      >
        <div className="w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full animate-spin mx-auto mb-4"></div>
        <p className="text-sm text-gray-600">Loading map visualization...</p>
      </motion.div>
    </div>
  );
}

export function KeplerMap({
  initialViewState = {
    latitude: 56.26392,  // Denmark center
    longitude: 9.501785,
    zoom: 7
  },
  h3Data = [],
  bnboData = [],
  bbrData = [],
  selectedYear,
  showPesticides,
  showPFAS,
  showBNBO,
  showBBR,
  cumulativeMode,
  onHover,
  onClick
}: KeplerMapProps) {
  const [isPending, startTransition] = useTransition();
  const dispatch = useDispatch();
  const { viewport, updateViewport } = useViewport();

  // Generate Kepler.gl configuration based on current state
  const keplerConfig = useMemo(() => {
    return generateKeplerConfig({
      selectedYear,
      showPesticides,
      showPFAS,
      showBNBO,
      showBBR,
      cumulativeMode
    });
  }, [selectedYear, showPesticides, showPFAS, showBNBO, showBBR, cumulativeMode]);

  // Prepare data for Kepler.gl
  const keplerData = useMemo(() => {
    const datasets = [];

    // H3 Data
    if (h3Data.length > 0) {
      datasets.push({
        info: {
          id: 'h3_data',
          label: 'H3 PFAS Exposure Data'
        },
        data: {
          fields: [
            { name: 'h3_id', type: 'string' },
            { name: 'year', type: 'integer' },
            { name: 'total_pesticide_load', type: 'real' },
            { name: 'total_pfas_grams', type: 'real' },
            { name: 'pesticide_application_count', type: 'integer' },
            { name: 'field_count', type: 'integer' },
            { name: 'agricultural_area_ha', type: 'real' },
            { name: 'avg_field_coverage', type: 'real' },
            { name: 'centroid_lon', type: 'real' },
            { name: 'centroid_lat', type: 'real' },
            { name: 'geometry', type: 'geojson' }
          ],
          rows: h3Data.map(row => [
            row.h3_id,
            row.year,
            row.total_pesticide_load || 0,
            row.total_pfas_grams || 0,
            row.pesticide_application_count || 0,
            row.field_count || 0,
            row.agricultural_area_ha || 0,
            row.avg_field_coverage || 0,
            row.centroid_lon,
            row.centroid_lat,
            row.geometry
          ])
        }
      });
    }

    // BNBO Data
    if (bnboData.length > 0 && showBNBO) {
      datasets.push({
        info: {
          id: 'bnbo_data',
          label: 'BNBO Protected Areas'
        },
        data: {
          fields: [
            { name: 'bnbo_id', type: 'string' },
            { name: 'status_code', type: 'string' },
            { name: 'status_description', type: 'string' },
            { name: 'area_ha', type: 'real' },
            { name: 'year', type: 'integer' },
            { name: 'geometry', type: 'geojson' }
          ],
          rows: bnboData.map(row => [
            row.bnbo_id,
            row.status_code,
            row.status_description,
            row.area_ha,
            row.year,
            row.geometry
          ])
        }
      });
    }

    // BBR Data
    if (bbrData.length > 0 && showBBR) {
      datasets.push({
        info: {
          id: 'bbr_data',
          label: 'BBR Buildings'
        },
        data: {
          fields: [
            { name: 'bbr_id', type: 'string' },
            { name: 'building_code', type: 'string' },
            { name: 'building_type', type: 'string' },
            { name: 'construction_year', type: 'integer' },
            { name: 'floor_area', type: 'real' },
            { name: 'address', type: 'string' },
            { name: 'geometry', type: 'geojson' }
          ],
          rows: bbrData.map(row => [
            row.bbr_id,
            row.building_code,
            row.building_type,
            row.construction_year,
            row.floor_area,
            row.address,
            row.geometry
          ])
        }
      });
    }

    return datasets;
  }, [h3Data, bnboData, bbrData, showBNBO, showBBR]);

  // Handle map interactions
  const handleHover = useCallback((info: any) => {
    if (onHover) {
      onHover(info);
    }
  }, [onHover]);

  const handleClick = useCallback((info: any) => {
    if (onClick) {
      onClick(info);
    }
  }, [onClick]);

  const handleViewStateChange = useCallback((viewState: ViewState) => {
    startTransition(() => {
      updateViewport(viewState);
    });
  }, [updateViewport]);

  return (
    <ErrorBoundary
      FallbackComponent={MapErrorFallback}
      onReset={() => window.location.reload()}
    >
      <div className="h-full w-full relative">
        <Suspense fallback={<MapLoadingFallback />}>
          <KeplerGl
            id="pfas_map"
            mapboxApiAccessToken={process.env.NEXT_PUBLIC_MAPBOX_TOKEN}
            width={typeof window !== 'undefined' ? window.innerWidth : 1200}
            height={typeof window !== 'undefined' ? window.innerHeight : 800}
            appName="PFAS Exposure Visualization"
            config={keplerConfig}
            data={keplerData}
            onHover={handleHover}
            onClick={handleClick}
            onViewStateChange={handleViewStateChange}
            uiState={{
              readOnly: false,
              currentModal: null
            }}
            visState={{
              ...keplerConfig.config.visState,
              mapState: {
                ...keplerConfig.config.mapState,
                ...viewport
              }
            }}
          />
        </Suspense>

        {/* Overlay Components */}
        <AnimatePresence>
          <LayerControls />
          <HoverTooltip />
        </AnimatePresence>

        {/* Loading Indicator */}
        {isPending && (
          <motion.div
            className="absolute top-4 right-4 bg-white rounded-lg shadow-lg p-3"
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, scale: 0.9 }}
          >
            <div className="flex items-center space-x-2">
              <div className="w-4 h-4 border-2 border-blue-600 border-t-transparent rounded-full animate-spin"></div>
              <span className="text-sm text-gray-600">Updating...</span>
            </div>
          </motion.div>
        )}
      </div>
    </ErrorBoundary>
  );
} 
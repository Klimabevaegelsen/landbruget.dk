'use client';

import { motion, AnimatePresence } from 'framer-motion';
import { BarChart3, Download, Filter, X, Info } from 'lucide-react';
import { useMapStore } from '@/stores/map-store';
import { useDataStore } from '@/stores/data-store';
import { useUIStore } from '@/stores/ui-store';
import { formatNumber } from '@/lib/utils';

export function DataPanel() {
  const { selectedYear, cumulativeMode, heatmapMode } = useMapStore();
  const { currentH3Data, currentBNBOData, currentBBRData, isLoading } = useDataStore();
  const { showDataPanel, setShowDataPanel } = useUIStore();
  
  if (!showDataPanel) {
    return (
      <motion.button
        onClick={() => setShowDataPanel(true)}
        className="absolute top-6 right-6 bg-white rounded-lg shadow-lg p-3 z-50 hover:shadow-xl transition-shadow"
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
      >
        <BarChart3 size={20} className="text-gray-600" />
      </motion.button>
    );
  }
  
  // Calculate statistics from current data
  const stats = {
    totalHexagons: currentH3Data.length,
    totalPesticideLoad: currentH3Data.reduce((sum, item) => sum + (item.total_pesticide_load || 0), 0),
    totalPFASMass: currentH3Data.reduce((sum, item) => sum + (item.total_pfas_grams || 0), 0),
    totalArea: currentH3Data.reduce((sum, item) => sum + (item.agricultural_area_ha || 0), 0),
    avgCoverage: currentH3Data.length > 0 
      ? currentH3Data.reduce((sum, item) => sum + (item.avg_field_coverage || 0), 0) / currentH3Data.length 
      : 0,
    protectedAreas: currentBNBOData.length,
    buildings: currentBBRData.length
  };
  
  const exportData = () => {
    const dataToExport = {
      metadata: {
        exportDate: new Date().toISOString(),
        selectedYear,
        cumulativeMode,
        heatmapMode,
        totalRecords: currentH3Data.length
      },
      h3Data: currentH3Data,
      bnboData: currentBNBOData,
      bbrData: currentBBRData,
      statistics: stats
    };
    
    const blob = new Blob([JSON.stringify(dataToExport, null, 2)], {
      type: 'application/json'
    });
    
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `pfas-data-${selectedYear}-${Date.now()}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };
  
  return (
    <AnimatePresence>
      <motion.div
        className="data-panel"
        initial={{ opacity: 0, x: 20 }}
        animate={{ opacity: 1, x: 0 }}
        exit={{ opacity: 0, x: 20 }}
        transition={{ duration: 0.3 }}
      >
        {/* Header */}
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center space-x-2">
            <BarChart3 size={16} className="text-gray-600" />
            <h3 className="text-sm font-medium text-gray-700">Data Overview</h3>
          </div>
          <button
            onClick={() => setShowDataPanel(false)}
            className="p-1 hover:bg-gray-100 rounded transition-colors"
          >
            <X size={16} className="text-gray-400" />
          </button>
        </div>
        
        {/* Current Selection Info */}
        <div className="mb-4 p-3 bg-blue-50 rounded-lg">
          <div className="text-sm font-medium text-blue-900 mb-1">
            Current View
          </div>
          <div className="text-xs text-blue-700 space-y-1">
            <div>Year: {cumulativeMode ? `2020-${selectedYear}` : selectedYear}</div>
            <div>Mode: {heatmapMode === 'pesticide' ? 'Pesticide Load' : 'PFAS Mass'}</div>
            <div>Type: {cumulativeMode ? 'Cumulative' : 'Single Year'}</div>
          </div>
        </div>
        
        {/* Statistics */}
        <div className="space-y-3 mb-4">
          <div className="text-sm font-medium text-gray-700 flex items-center space-x-1">
            <Info size={14} />
            <span>Statistics</span>
          </div>
          
          {isLoading ? (
            <div className="space-y-2">
              {[...Array(6)].map((_, i) => (
                <div key={i} className="h-4 bg-gray-200 rounded animate-pulse"></div>
              ))}
            </div>
          ) : (
            <div className="space-y-2 text-xs">
              <div className="flex justify-between">
                <span className="text-gray-600">H3 Hexagons:</span>
                <span className="font-medium">{formatNumber(stats.totalHexagons, 0)}</span>
              </div>
              
              <div className="flex justify-between">
                <span className="text-gray-600">Total Area:</span>
                <span className="font-medium">{formatNumber(stats.totalArea)} ha</span>
              </div>
              
              {heatmapMode === 'pesticide' ? (
                <div className="flex justify-between">
                  <span className="text-gray-600">Pesticide Load:</span>
                  <span className="font-medium">{formatNumber(stats.totalPesticideLoad)} kg</span>
                </div>
              ) : (
                <div className="flex justify-between">
                  <span className="text-gray-600">PFAS Mass:</span>
                  <span className="font-medium">{formatNumber(stats.totalPFASMass)} g</span>
                </div>
              )}
              
              <div className="flex justify-between">
                <span className="text-gray-600">Avg Coverage:</span>
                <span className="font-medium">{formatNumber(stats.avgCoverage * 100, 1)}%</span>
              </div>
              
              <div className="flex justify-between">
                <span className="text-gray-600">Protected Areas:</span>
                <span className="font-medium">{formatNumber(stats.protectedAreas, 0)}</span>
              </div>
              
              <div className="flex justify-between">
                <span className="text-gray-600">Buildings:</span>
                <span className="font-medium">{formatNumber(stats.buildings, 0)}</span>
              </div>
            </div>
          )}
        </div>
        
        {/* Quick Actions */}
        <div className="space-y-2">
          <motion.button
            onClick={exportData}
            disabled={isLoading || currentH3Data.length === 0}
            className="w-full flex items-center justify-center space-x-2 px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-300 disabled:cursor-not-allowed transition-colors text-sm"
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <Download size={14} />
            <span>Export Data</span>
          </motion.button>
          
          <motion.button
            className="w-full flex items-center justify-center space-x-2 px-3 py-2 bg-gray-100 text-gray-700 rounded-lg hover:bg-gray-200 transition-colors text-sm"
            whileHover={{ scale: 1.02 }}
            whileTap={{ scale: 0.98 }}
          >
            <Filter size={14} />
            <span>Advanced Filters</span>
          </motion.button>
        </div>
        
        {/* Data Quality Indicator */}
        <div className="mt-4 pt-3 border-t border-gray-200">
          <div className="text-xs text-gray-500">
            <div className="flex items-center justify-between mb-1">
              <span>Data Quality</span>
              <div className="flex items-center space-x-1">
                <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                <span className="text-green-600 font-medium">Good</span>
              </div>
            </div>
            <div className="text-gray-400">
              Last updated: {new Date().toLocaleDateString()}
            </div>
          </div>
        </div>
      </motion.div>
    </AnimatePresence>
  );
} 
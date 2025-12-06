'use client';

import { motion } from 'framer-motion';
import { useMapStore } from '@/stores/map-store';

export function HeatmapToggle() {
  const { heatmapMode, setHeatmapMode } = useMapStore();
  
  return (
    <motion.div 
      className="heatmap-toggle"
      initial={{ opacity: 0, x: -20 }}
      animate={{ opacity: 1, x: 0 }}
      transition={{ duration: 0.3 }}
    >
      <h3 className="text-sm font-medium text-gray-700 mb-3">Data Layer</h3>
      
      <div className="relative bg-gray-100 rounded-lg p-1 flex">
        <motion.div
          className="absolute top-1 bottom-1 bg-white rounded-md shadow-sm"
          initial={false}
          animate={{
            left: heatmapMode === 'pesticide' ? '4px' : '50%',
            width: heatmapMode === 'pesticide' ? 'calc(50% - 4px)' : 'calc(50% - 4px)',
          }}
          transition={{ type: 'spring', stiffness: 300, damping: 30 }}
        />
        
        <button
          onClick={() => setHeatmapMode('pesticide')}
          className={`relative z-10 flex-1 px-3 py-2 text-sm font-medium rounded-md transition-colors ${
            heatmapMode === 'pesticide'
              ? 'text-blue-700'
              : 'text-gray-600 hover:text-gray-800'
          }`}
        >
          <div className="flex items-center justify-center space-x-2">
            <div className="w-3 h-3 bg-blue-500 rounded-full"></div>
            <span>Pesticide Load</span>
          </div>
        </button>
        
        <button
          onClick={() => setHeatmapMode('pfas')}
          className={`relative z-10 flex-1 px-3 py-2 text-sm font-medium rounded-md transition-colors ${
            heatmapMode === 'pfas'
              ? 'text-red-700'
              : 'text-gray-600 hover:text-gray-800'
          }`}
        >
          <div className="flex items-center justify-center space-x-2">
            <div className="w-3 h-3 bg-red-500 rounded-full"></div>
            <span>PFAS Mass</span>
          </div>
        </button>
      </div>
      
      <div className="mt-3 text-xs text-gray-500">
        {heatmapMode === 'pesticide' ? (
          <div>
            <div className="font-medium">Pesticide Load (kg/ha)</div>
            <div>Standardized pesticide application intensity</div>
          </div>
        ) : (
          <div>
            <div className="font-medium">PFAS Mass (grams)</div>
            <div>Active ingredient PFAS mass per hexagon</div>
          </div>
        )}
      </div>
    </motion.div>
  );
} 
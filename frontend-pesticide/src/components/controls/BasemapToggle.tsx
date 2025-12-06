'use client';

import { useMapStore } from '@/stores/map-store';
import { Eye, EyeOff, Map } from 'lucide-react';

export function BasemapToggle() {
  const showBasemap = useMapStore((state) => state.showBasemap);
  const setShowBasemap = useMapStore((state) => state.setShowBasemap);

  return (
    <div className="flex items-center justify-between p-3 bg-gray-700/50 rounded-lg">
      <div className="flex items-center space-x-3">
        <Map className="w-4 h-4 text-gray-400" />
        <div>
          <div className="text-sm font-medium text-white">Basemap Details</div>
          <div className="text-xs text-gray-400">Buildings, Roads & Land Use</div>
        </div>
      </div>
      
      <button
        onClick={() => setShowBasemap(!showBasemap)}
        className={`p-2 rounded-md transition-colors ${
          showBasemap 
            ? 'bg-blue-600 text-white hover:bg-blue-700' 
            : 'bg-gray-600 text-gray-300 hover:bg-gray-500'
        }`}
      >
        {showBasemap ? (
          <Eye className="w-4 h-4" />
        ) : (
          <EyeOff className="w-4 h-4" />
        )}
      </button>
    </div>
  );
} 
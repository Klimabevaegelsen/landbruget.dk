'use client';

import { useResolutionStore } from '@/stores/resolution-store';
import { usePMTilesStore } from '@/stores/pmtiles-store';
import { Zap, ZapOff } from 'lucide-react';

interface ResolutionControlsProps {
  className?: string;
}

export function ResolutionControls({
  className = '',
}: ResolutionControlsProps) {
  const {
    currentResolution,
    autoResolution,
    setResolution,
    setAutoResolution,
  } = useResolutionStore();

  const { getAvailableResolutions } = usePMTilesStore();

  const availableResolutions = getAvailableResolutions();

  return (
    <div className={`${className}`}>
      {/* Auto Resolution Toggle */}
      <div className="mb-2 flex items-center space-x-2">
        <button
          onClick={() => setAutoResolution(!autoResolution)}
          className="flex items-center space-x-1 text-xs text-gray-300 transition-colors hover:text-white"
        >
          {autoResolution ? (
            <Zap className="h-3 w-3" />
          ) : (
            <ZapOff className="h-3 w-3" />
          )}
          <span>Auto</span>
        </button>
      </div>

      {/* Manual Resolution Selection */}
      {!autoResolution && (
        <div className="flex space-x-1">
          {availableResolutions.map((resolution) => (
            <button
              key={resolution}
              onClick={() => setResolution(resolution as 8 | 10 | 'kommune')}
              className={`rounded px-2 py-1 text-xs transition-colors ${
                resolution === currentResolution
                  ? 'bg-white/20 text-white'
                  : 'bg-white/5 text-gray-400 hover:bg-white/10 hover:text-white'
              }`}
            >
              {resolution}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

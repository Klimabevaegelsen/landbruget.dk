'use client'

import { useResolutionStore } from '@/stores/resolution-store'
import { usePMTilesStore } from '@/stores/pmtiles-store'
import { 
  Zap, 
  ZapOff
} from 'lucide-react'

interface ResolutionControlsProps {
  className?: string
}

export function ResolutionControls({ className = '' }: ResolutionControlsProps) {
  const {
    currentResolution,
    autoResolution,
    setResolution,
    setAutoResolution,
  } = useResolutionStore()
  
  const { getAvailableResolutions } = usePMTilesStore()
  
  const availableResolutions = getAvailableResolutions()
  
  return (
    <div className={`${className}`}>
      {/* Auto Resolution Toggle */}
      <div className="flex items-center space-x-2 mb-2">
        <button
          onClick={() => setAutoResolution(!autoResolution)}
          className="flex items-center space-x-1 text-xs text-gray-300 hover:text-white transition-colors"
        >
          {autoResolution ? <Zap className="w-3 h-3" /> : <ZapOff className="w-3 h-3" />}
          <span>Auto</span>
        </button>
      </div>
      
      {/* Manual Resolution Selection */}
      {!autoResolution && (
        <div className="flex space-x-1">
          {availableResolutions.map(resolution => (
            <button
              key={resolution}
              onClick={() => setResolution(resolution)}
              className={`px-2 py-1 rounded text-xs transition-colors ${
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
  )
} 
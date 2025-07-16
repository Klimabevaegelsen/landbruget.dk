'use client'

import { useEffect } from 'react'
import { useTemporalStore } from '@/stores/temporal-store'
import { usePMTilesStore } from '@/stores/pmtiles-store'
import { 
  Play, 
  Pause, 
  ChevronLeft, 
  ChevronRight
} from 'lucide-react'

interface TemporalControlsProps {
  className?: string
}

export function TemporalControls({ className = '' }: TemporalControlsProps) {
  const {
    currentYear,
    availableYears,
    isAnimating,
    startAnimation,
    stopAnimation,
    goToNextYear,
    goToPreviousYear,
    canGoNext,
    canGoPrevious,
    getYearRange,
  } = useTemporalStore()
  
  const { metadata } = usePMTilesStore()
  
  // Update available years from metadata
  useEffect(() => {
    if (metadata?.years) {
      useTemporalStore.getState().setAvailableYears(metadata.years)
    }
  }, [metadata])
  
  const yearRange = getYearRange()
  
  return (
    <div className={`${className}`}>
      {/* Simple Controls */}
      <div className="flex items-center justify-center space-x-4 mb-3">
        {/* Previous */}
        <button
          onClick={goToPreviousYear}
          disabled={!canGoPrevious() || isAnimating}
          className="p-2 rounded-full bg-white/10 hover:bg-white/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all"
        >
          <ChevronLeft className="w-4 h-4 text-white" />
        </button>
        
        {/* Play/Pause */}
        <button
          onClick={isAnimating ? stopAnimation : startAnimation}
          disabled={availableYears.length <= 1}
          className="p-2 rounded-full bg-white text-black hover:bg-gray-100 disabled:opacity-30 disabled:cursor-not-allowed transition-all"
        >
          {isAnimating ? <Pause className="w-4 h-4" /> : <Play className="w-4 h-4" />}
        </button>
        
        {/* Next */}
        <button
          onClick={goToNextYear}
          disabled={!canGoNext() || isAnimating}
          className="p-2 rounded-full bg-white/10 hover:bg-white/20 disabled:opacity-30 disabled:cursor-not-allowed transition-all"
        >
          <ChevronRight className="w-4 h-4 text-white" />
        </button>
      </div>
      
      {/* Year Progress Bar */}
      {yearRange && (
        <div>
          <div className="flex justify-between text-xs text-gray-400 mb-1">
            <span>{yearRange[0]}</span>
            <span>{yearRange[1]}</span>
          </div>
          <div className="w-full bg-white/20 rounded-full h-0.5">
            <div
              className="bg-white h-0.5 rounded-full transition-all duration-300"
              style={{
                width: `${((currentYear - yearRange[0]) / (yearRange[1] - yearRange[0])) * 100}%`
              }}
            />
          </div>
        </div>
      )}
    </div>
  )
} 
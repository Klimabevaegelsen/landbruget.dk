import { create } from 'zustand'
import { persist } from 'zustand/middleware'

interface TemporalState {
  // Current year
  currentYear: number
  
  // Available years (loaded from metadata)
  availableYears: number[]
  
  // Animation state
  isAnimating: boolean
  animationSpeed: number // milliseconds per year
  animationDirection: 'forward' | 'backward'
  
  // Comparison mode
  comparisonMode: boolean
  comparisonYear: number | null
  
  // Cumulative mode
  cumulativeMode: boolean
  cumulativeStartYear: number | null
  
  // Actions
  setCurrentYear: (year: number) => void
  setAvailableYears: (years: number[]) => void
  
  // Animation controls
  startAnimation: () => void
  stopAnimation: () => void
  setAnimationSpeed: (speed: number) => void
  setAnimationDirection: (direction: 'forward' | 'backward') => void
  
  // Comparison mode
  setComparisonMode: (enabled: boolean) => void
  setComparisonYear: (year: number | null) => void
  
  // Cumulative mode
  setCumulativeMode: (enabled: boolean) => void
  setCumulativeStartYear: (year: number | null) => void
  
  // Navigation
  goToNextYear: () => void
  goToPreviousYear: () => void
  goToFirstYear: () => void
  goToLastYear: () => void
  
  // Utility functions
  getYearIndex: (year: number) => number
  canGoNext: () => boolean
  canGoPrevious: () => boolean
  getYearRange: () => [number, number] | null
  
  // Animation interval management
  animationInterval: NodeJS.Timeout | null
}

export const useTemporalStore = create<TemporalState>()(
  persist(
    (set, get) => ({
      // Initial state
      currentYear: 2023, // Start with most recent year
      availableYears: [],
      isAnimating: false,
      animationSpeed: 1000, // 1 second per year
      animationDirection: 'forward',
      comparisonMode: false,
      comparisonYear: null,
      cumulativeMode: false,
      cumulativeStartYear: null,
      animationInterval: null,
      
      // Basic actions
      setCurrentYear: (year) => set({ currentYear: year }),
      setAvailableYears: (years) => set({ availableYears: years }),
      
      // Animation controls
      startAnimation: () => {
        const state = get()
        if (state.isAnimating) return
        
        set({ isAnimating: true })
        
        const interval = setInterval(() => {
          const currentState = get()
          if (!currentState.isAnimating) {
            clearInterval(interval)
            return
          }
          
          if (currentState.animationDirection === 'forward') {
            if (currentState.canGoNext()) {
              currentState.goToNextYear()
            } else {
              // Loop back to first year
              currentState.goToFirstYear()
            }
          } else {
            if (currentState.canGoPrevious()) {
              currentState.goToPreviousYear()
            } else {
              // Loop back to last year
              currentState.goToLastYear()
            }
          }
        }, state.animationSpeed)
        
        set({ animationInterval: interval })
      },
      
      stopAnimation: () => {
        const state = get()
        if (state.animationInterval) {
          clearInterval(state.animationInterval)
        }
        set({ isAnimating: false, animationInterval: null })
      },
      
      setAnimationSpeed: (speed) => set({ animationSpeed: speed }),
      setAnimationDirection: (direction) => set({ animationDirection: direction }),
      
      // Comparison mode
      setComparisonMode: (enabled) => set({ 
        comparisonMode: enabled,
        comparisonYear: enabled ? get().currentYear : null,
      }),
      
      setComparisonYear: (year) => set({ comparisonYear: year }),
      
      // Cumulative mode
      setCumulativeMode: (enabled) => set({ 
        cumulativeMode: enabled,
        cumulativeStartYear: enabled ? get().availableYears[0] : null,
      }),
      
      setCumulativeStartYear: (year) => set({ cumulativeStartYear: year }),
      
      // Navigation
      goToNextYear: () => {
        const state = get()
        const currentIndex = state.getYearIndex(state.currentYear)
        if (currentIndex < state.availableYears.length - 1) {
          set({ currentYear: state.availableYears[currentIndex + 1] })
        }
      },
      
      goToPreviousYear: () => {
        const state = get()
        const currentIndex = state.getYearIndex(state.currentYear)
        if (currentIndex > 0) {
          set({ currentYear: state.availableYears[currentIndex - 1] })
        }
      },
      
      goToFirstYear: () => {
        const state = get()
        if (state.availableYears.length > 0) {
          set({ currentYear: state.availableYears[0] })
        }
      },
      
      goToLastYear: () => {
        const state = get()
        if (state.availableYears.length > 0) {
          set({ currentYear: state.availableYears[state.availableYears.length - 1] })
        }
      },
      
      // Utility functions
      getYearIndex: (year) => {
        const state = get()
        return state.availableYears.indexOf(year)
      },
      
      canGoNext: () => {
        const state = get()
        const currentIndex = state.getYearIndex(state.currentYear)
        return currentIndex < state.availableYears.length - 1
      },
      
      canGoPrevious: () => {
        const state = get()
        const currentIndex = state.getYearIndex(state.currentYear)
        return currentIndex > 0
      },
      
      getYearRange: () => {
        const state = get()
        if (state.availableYears.length === 0) return null
        
        const sortedYears = [...state.availableYears].sort((a, b) => a - b)
        return [sortedYears[0], sortedYears[sortedYears.length - 1]]
      },
    }),
    {
      name: 'temporal-store',
      // Persist temporal preferences
      partialize: (state) => ({
        currentYear: state.currentYear,
        animationSpeed: state.animationSpeed,
        animationDirection: state.animationDirection,
        comparisonMode: state.comparisonMode,
        comparisonYear: state.comparisonYear,
        cumulativeMode: state.cumulativeMode,
        cumulativeStartYear: state.cumulativeStartYear,
      }),
    }
  )
) 
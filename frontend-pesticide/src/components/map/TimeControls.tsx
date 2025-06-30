'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { Play, Pause, RotateCcw } from 'lucide-react';
import { useMapStore } from '@/stores/map-store';

export function TimeControls() {
  const { 
    selectedYear, 
    setSelectedYear, 
    cumulativeMode, 
    setCumulativeMode,
    availableYears 
  } = useMapStore();
  
  const [isPlaying, setIsPlaying] = useState(false);
  const [playbackSpeed, setPlaybackSpeed] = useState(1000); // ms per year
  
  // Auto-play functionality
  useEffect(() => {
    if (!isPlaying) return;
    
    const interval = setInterval(() => {
      const currentIndex = availableYears.indexOf(selectedYear);
      const nextIndex = (currentIndex + 1) % availableYears.length;
      setSelectedYear(availableYears[nextIndex]);
    }, playbackSpeed);
    
    return () => clearInterval(interval);
  }, [isPlaying, playbackSpeed, availableYears, setSelectedYear, selectedYear]);
  
  const resetToStart = () => {
    setSelectedYear(availableYears[0]);
    setIsPlaying(false);
  };
  
  return (
    <motion.div 
      className="control-panel"
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.3 }}
    >
      <div className="time-controls">
        {/* Year Selection */}
        <div>
          <div className="flex items-center justify-between mb-2">
            <label className="text-sm font-medium">
              {cumulativeMode ? `Years 2020-${selectedYear}` : `Year ${selectedYear}`}
            </label>
            <motion.button
              onClick={() => setCumulativeMode(!cumulativeMode)}
              className={`px-3 py-1 rounded text-xs transition-colors ${
                cumulativeMode 
                  ? 'bg-blue-100 text-blue-800' 
                  : 'bg-gray-100 text-gray-800'
              }`}
              whileHover={{ scale: 1.05 }}
              whileTap={{ scale: 0.95 }}
            >
              {cumulativeMode ? 'Cumulative' : 'Single Year'}
            </motion.button>
          </div>
          
          <div className="relative">
            <input
              type="range"
              min={Math.min(...availableYears)}
              max={Math.max(...availableYears)}
              value={selectedYear}
              onChange={(e) => setSelectedYear(parseInt(e.target.value))}
              className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer slider"
            />
            <div className="flex justify-between text-xs text-gray-500 mt-1">
              {availableYears.map(year => (
                <span key={year}>{year}</span>
              ))}
            </div>
          </div>
        </div>
        
        {/* Playback Controls */}
        <div className="flex items-center justify-center space-x-4">
          <motion.button
            onClick={() => setIsPlaying(!isPlaying)}
            className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            {isPlaying ? <Pause size={16} /> : <Play size={16} />}
            <span>{isPlaying ? 'Pause' : 'Play'}</span>
          </motion.button>
          
          <motion.button
            onClick={resetToStart}
            className="flex items-center space-x-2 px-3 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700 transition-colors"
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
          >
            <RotateCcw size={16} />
            <span>Reset</span>
          </motion.button>
          
          <select
            value={playbackSpeed}
            onChange={(e) => setPlaybackSpeed(parseInt(e.target.value))}
            className="px-3 py-2 border rounded-lg text-sm bg-white"
          >
            <option value={2000}>Slow (2s/year)</option>
            <option value={1000}>Normal (1s/year)</option>
            <option value={500}>Fast (0.5s/year)</option>
          </select>
        </div>
      </div>
      
      <style jsx>{`
        .slider::-webkit-slider-thumb {
          appearance: none;
          height: 20px;
          width: 20px;
          border-radius: 50%;
          background: #3b82f6;
          cursor: pointer;
          border: 2px solid white;
          box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }
        
        .slider::-moz-range-thumb {
          height: 20px;
          width: 20px;
          border-radius: 50%;
          background: #3b82f6;
          cursor: pointer;
          border: 2px solid white;
          box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        }
      `}</style>
    </motion.div>
  );
} 
'use client';

import { useState, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Layers,
  Eye,
  EyeOff,
  Settings,
  ChevronDown,
  ChevronUp,
  Palette,
} from 'lucide-react';

interface LayerControlsProps {
  showH3?: boolean;
  showBNBO?: boolean;
  showBBR?: boolean;
  showKommune?: boolean;
  onToggleH3?: (visible: boolean) => void;
  onToggleBNBO?: (visible: boolean) => void;
  onToggleBBR?: (visible: boolean) => void;
  onToggleKommune?: (visible: boolean) => void;
  onOpacityChange?: (layer: string, opacity: number) => void;
}

interface LayerState {
  visible: boolean;
  opacity: number;
  name: string;
  color: string;
  description: string;
}

export function LayerControls({
  showH3 = true,
  showBNBO = false,
  showBBR = true,
  showKommune = false,
  onToggleH3,
  onToggleBNBO,
  onToggleBBR,
  onToggleKommune,
  onOpacityChange,
}: LayerControlsProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [activeLayer, setActiveLayer] = useState<string | null>(null);

  const [layers, setLayers] = useState<Record<string, LayerState>>({
    h3: {
      visible: showH3,
      opacity: 0.7,
      name: 'H3 Heatmap',
      color: '#4292c6',
      description: 'PFAS exposure and pesticide load by hexagon',
    },
    bnbo: {
      visible: showBNBO,
      opacity: 0.4,
      name: 'BNBO Areas',
      color: '#2d8659',
      description: 'Protected environmental areas',
    },
    bbr: {
      visible: showBBR,
      opacity: 0.8,
      name: 'Buildings',
      color: '#4a90e2',
      description: 'Building registry data',
    },
    kommune: {
      visible: showKommune,
      opacity: 0.6,
      name: 'Municipalities',
      color: '#8b5a3c',
      description: 'Municipal boundaries with aggregated PFAS/pesticide data',
    },
  });

  const handleLayerToggle = useCallback(
    (layerId: string) => {
      setLayers((prev) => ({
        ...prev,
        [layerId]: {
          ...prev[layerId],
          visible: !prev[layerId].visible,
        },
      }));

      // Call appropriate callback
      const newVisibility = !layers[layerId].visible;
      switch (layerId) {
        case 'h3':
          onToggleH3?.(newVisibility);
          break;
        case 'bnbo':
          onToggleBNBO?.(newVisibility);
          break;
        case 'bbr':
          onToggleBBR?.(newVisibility);
          break;
        case 'kommune':
          onToggleKommune?.(newVisibility);
          break;
      }
    },
    [layers, onToggleH3, onToggleBNBO, onToggleBBR, onToggleKommune]
  );

  const handleOpacityChange = useCallback(
    (layerId: string, opacity: number) => {
      setLayers((prev) => ({
        ...prev,
        [layerId]: {
          ...prev[layerId],
          opacity,
        },
      }));

      onOpacityChange?.(layerId, opacity);
    },
    [onOpacityChange]
  );

  const toggleExpanded = useCallback(() => {
    setIsExpanded(!isExpanded);
  }, [isExpanded]);

  const handleLayerSettings = useCallback(
    (layerId: string) => {
      setActiveLayer(activeLayer === layerId ? null : layerId);
    },
    [activeLayer]
  );

  return (
    <div className="absolute top-4 left-4 z-10">
      <motion.div
        className="overflow-hidden rounded-lg border border-gray-200 bg-white shadow-lg"
        initial={{ opacity: 0, x: -20 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.3 }}
      >
        {/* Header */}
        <div className="flex items-center justify-between border-b border-gray-100 p-3">
          <div className="flex items-center space-x-2">
            <Layers className="h-4 w-4 text-gray-600" />
            <span className="text-sm font-medium text-gray-900">Layers</span>
          </div>
          <button
            onClick={toggleExpanded}
            className="rounded p-1 transition-colors hover:bg-gray-100"
          >
            {isExpanded ? (
              <ChevronUp className="h-4 w-4 text-gray-600" />
            ) : (
              <ChevronDown className="h-4 w-4 text-gray-600" />
            )}
          </button>
        </div>

        {/* Layer List */}
        <AnimatePresence>
          {isExpanded && (
            <motion.div
              initial={{ height: 0 }}
              animate={{ height: 'auto' }}
              exit={{ height: 0 }}
              transition={{ duration: 0.2 }}
              className="overflow-hidden"
            >
              <div className="space-y-2 p-2">
                {Object.entries(layers).map(([layerId, layer]) => (
                  <div key={layerId} className="space-y-2">
                    {/* Layer Toggle */}
                    <div className="flex items-center justify-between rounded p-2 hover:bg-gray-50">
                      <div className="flex items-center space-x-3">
                        <button
                          onClick={() => handleLayerToggle(layerId)}
                          className={`rounded p-1 transition-colors ${
                            layer.visible
                              ? 'text-blue-600 hover:bg-blue-50'
                              : 'text-gray-400 hover:bg-gray-100'
                          }`}
                        >
                          {layer.visible ? (
                            <Eye className="h-4 w-4" />
                          ) : (
                            <EyeOff className="h-4 w-4" />
                          )}
                        </button>

                        <div className="flex items-center space-x-2">
                          <div
                            className="h-3 w-3 rounded-full"
                            style={{ backgroundColor: layer.color }}
                          />
                          <div>
                            <div className="text-sm font-medium text-gray-900">
                              {layer.name}
                            </div>
                            <div className="text-xs text-gray-500">
                              {layer.description}
                            </div>
                          </div>
                        </div>
                      </div>

                      <button
                        onClick={() => handleLayerSettings(layerId)}
                        className="rounded p-1 transition-colors hover:bg-gray-100"
                      >
                        <Settings className="h-4 w-4 text-gray-400" />
                      </button>
                    </div>

                    {/* Layer Settings */}
                    <AnimatePresence>
                      {activeLayer === layerId && (
                        <motion.div
                          initial={{ height: 0, opacity: 0 }}
                          animate={{ height: 'auto', opacity: 1 }}
                          exit={{ height: 0, opacity: 0 }}
                          transition={{ duration: 0.2 }}
                          className="ml-6 overflow-hidden rounded border bg-gray-50 p-3"
                        >
                          <div className="space-y-3">
                            {/* Opacity Control */}
                            <div>
                              <div className="mb-2 flex items-center justify-between">
                                <label className="text-xs font-medium text-gray-700">
                                  Opacity
                                </label>
                                <span className="text-xs text-gray-500">
                                  {Math.round(layer.opacity * 100)}%
                                </span>
                              </div>
                              <input
                                type="range"
                                min="0"
                                max="1"
                                step="0.1"
                                value={layer.opacity}
                                onChange={(e) =>
                                  handleOpacityChange(
                                    layerId,
                                    parseFloat(e.target.value)
                                  )
                                }
                                className="slider h-2 w-full cursor-pointer appearance-none rounded-lg bg-gray-200"
                              />
                            </div>

                            {/* Layer-specific controls */}
                            {layerId === 'h3' && (
                              <div className="space-y-2">
                                <div className="flex items-center justify-between">
                                  <span className="text-xs font-medium text-gray-700">
                                    3D Elevation
                                  </span>
                                  <input
                                    type="checkbox"
                                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                                    defaultChecked
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs font-medium text-gray-700">
                                    Show Borders
                                  </span>
                                  <input
                                    type="checkbox"
                                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                                    defaultChecked
                                  />
                                </div>
                              </div>
                            )}

                            {layerId === 'bnbo' && (
                              <div className="space-y-2">
                                <div className="flex items-center justify-between">
                                  <span className="text-xs font-medium text-gray-700">
                                    Show Labels
                                  </span>
                                  <input
                                    type="checkbox"
                                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs font-medium text-gray-700">
                                    Stroke Width
                                  </span>
                                  <input
                                    type="range"
                                    min="0"
                                    max="3"
                                    step="0.5"
                                    defaultValue="1"
                                    className="h-1 w-16 cursor-pointer appearance-none rounded-lg bg-gray-200"
                                  />
                                </div>
                              </div>
                            )}

                            {layerId === 'bbr' && (
                              <div className="space-y-2">
                                <div className="flex items-center justify-between">
                                  <span className="text-xs font-medium text-gray-700">
                                    Point Size
                                  </span>
                                  <input
                                    type="range"
                                    min="1"
                                    max="10"
                                    step="1"
                                    defaultValue="3"
                                    className="h-1 w-16 cursor-pointer appearance-none rounded-lg bg-gray-200"
                                  />
                                </div>
                                <div className="flex items-center justify-between">
                                  <span className="text-xs font-medium text-gray-700">
                                    Cluster Points
                                  </span>
                                  <input
                                    type="checkbox"
                                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                                    defaultChecked
                                  />
                                </div>
                              </div>
                            )}
                          </div>
                        </motion.div>
                      )}
                    </AnimatePresence>
                  </div>
                ))}
              </div>
            </motion.div>
          )}
        </AnimatePresence>

        {/* Quick Actions */}
        {!isExpanded && (
          <div className="flex items-center justify-center space-x-1 p-2">
            {Object.entries(layers).map(([layerId, layer]) => (
              <button
                key={layerId}
                onClick={() => handleLayerToggle(layerId)}
                className={`rounded p-1 transition-colors ${
                  layer.visible
                    ? 'text-blue-600 hover:bg-blue-50'
                    : 'text-gray-400 hover:bg-gray-100'
                }`}
                title={`Toggle ${layer.name}`}
              >
                {layer.visible ? (
                  <Eye className="h-4 w-4" />
                ) : (
                  <EyeOff className="h-4 w-4" />
                )}
              </button>
            ))}
          </div>
        )}
      </motion.div>

      {/* Layer Legend */}
      <AnimatePresence>
        {isExpanded && (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 10 }}
            transition={{ duration: 0.2, delay: 0.1 }}
            className="mt-2 rounded-lg border border-gray-200 bg-white p-3 shadow-lg"
          >
            <div className="mb-2 flex items-center space-x-2">
              <Palette className="h-4 w-4 text-gray-600" />
              <span className="text-sm font-medium text-gray-900">Legend</span>
            </div>

            <div className="space-y-2 text-xs">
              {layers.h3.visible && (
                <div className="flex items-center space-x-2">
                  <div className="h-2 w-4 rounded bg-gradient-to-r from-blue-100 to-blue-800"></div>
                  <span className="text-gray-700">
                    PFAS/Pesticide Intensity
                  </span>
                </div>
              )}

              {layers.bnbo.visible && (
                <div className="space-y-1">
                  <div className="flex items-center space-x-2">
                    <div className="h-3 w-3 rounded bg-green-700"></div>
                    <span className="text-gray-700">Protected</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className="h-3 w-3 rounded bg-green-500"></div>
                    <span className="text-gray-700">Buffer Zone</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className="h-3 w-3 rounded bg-yellow-400"></div>
                    <span className="text-gray-700">Agricultural</span>
                  </div>
                </div>
              )}

              {layers.bbr.visible && (
                <div className="space-y-1">
                  <div className="flex items-center space-x-2">
                    <div className="h-3 w-3 rounded-full bg-blue-500"></div>
                    <span className="text-gray-700">Residential</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className="h-3 w-3 rounded-full bg-green-500"></div>
                    <span className="text-gray-700">Agricultural</span>
                  </div>
                  <div className="flex items-center space-x-2">
                    <div className="h-3 w-3 rounded-full bg-orange-500"></div>
                    <span className="text-gray-700">Industrial</span>
                  </div>
                </div>
              )}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

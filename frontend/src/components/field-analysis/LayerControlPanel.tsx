'use client';

import React from 'react';
import { LayerVisibility, FilterState } from './types';
import {
  Wheat,
  Droplets,
  Wind,
  Waves,
  Home,
  TestTube,
  Leaf,
} from 'lucide-react';

interface LayerControlPanelProps {
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  onLayerToggle: (layerName: keyof LayerVisibility) => void;
  onFilterChange: (filters: Partial<FilterState>) => void;
}

export function LayerControlPanel({
  layerVisibility,
  filterState,
  onLayerToggle,
  onFilterChange,
}: LayerControlPanelProps) {
  const layerConfigs = [
    {
      key: 'fields' as const,
      name: 'Landbrugsmarker',
      description: '617.774 marker med pesticidforbrug og miljødata',
      icon: Wheat,
      color: 'bg-green-500',
    },
    {
      key: 'bnbo' as const,
      name: 'BNBO Områder',
      description: '2.761 boringsnære beskyttelsesområder',
      icon: Droplets,
      color: 'bg-blue-600',
    },
    {
      key: 'wetlands' as const,
      name: 'Lavbundsområder',
      description: '768.646 lavbundsjorder med tørvindhold',
      icon: Wind,
      color: 'bg-muted-foreground',
    },
    {
      key: 'water_projects' as const,
      name: 'Vandprojekter',
      description: '2.138 vandprojekter til miljøgenopretning',
      icon: Waves,
      color: 'bg-teal-500',
    },
    {
      key: 'buildings' as const,
      name: 'Bygninger',
      description: '268.260 bygninger inden for 100m af pesticidmarker',
      icon: Home,
      color: 'bg-muted-foreground',
    },
  ];

  return (
    <div className="h-full overflow-y-auto">
      <div className="space-y-4 p-4 lg:space-y-6 lg:p-4">
        {/* Header */}
        <div>
          <h2 className="mb-2 text-lg font-bold text-gray-900 lg:text-xl">
            Kortlag
          </h2>
          <p className="text-sm text-gray-600">
            Vælg hvilke data der skal vises på kortet
          </p>
        </div>

        {/* Layer Toggles */}
        <div className="space-y-3">
          {layerConfigs.map((layer) => (
            <div key={layer.key} className="flex items-start space-x-3">
              <div className="mt-1 flex-shrink-0">
                <button
                  onClick={() => onLayerToggle(layer.key)}
                  className={`flex min-h-[44px] min-w-[44px] items-center justify-center rounded border-2 lg:h-5 lg:min-h-0 lg:w-5 lg:min-w-0 ${
                    layerVisibility[layer.key]
                      ? 'border-blue-600 bg-blue-600'
                      : 'border-gray-300 hover:border-gray-400 active:border-gray-500'
                  } transition-colors`}
                >
                  {layerVisibility[layer.key] && (
                    <svg
                      className="h-4 w-4 text-white lg:h-3 lg:w-3"
                      fill="currentColor"
                      viewBox="0 0 20 20"
                    >
                      <path
                        fillRule="evenodd"
                        d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                        clipRule="evenodd"
                      />
                    </svg>
                  )}
                </button>
              </div>

              <div className="min-w-0 flex-1">
                <div className="flex items-center space-x-2">
                  <layer.icon className="h-5 w-5 lg:h-4 lg:w-4" />
                  <h3 className="text-base font-medium text-gray-900 lg:text-sm">
                    {layer.name}
                  </h3>
                </div>
                <p className="mt-1 text-xs text-gray-500">
                  {layer.description}
                </p>
              </div>
            </div>
          ))}
        </div>

        {/* Filters Section */}
        <div className="border-t pt-6">
          <h3 className="mb-4 text-lg font-semibold text-gray-900">Filtre</h3>
          {/* Organic Filter */}
          <div className="mb-4">
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={filterState.organicOnly}
                onChange={(e) =>
                  onFilterChange({ organicOnly: e.target.checked })
                }
                className="h-5 w-5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 lg:h-4 lg:w-4"
              />
              <span className="ml-3 text-base text-gray-700 lg:ml-2 lg:text-sm">
                Kun økologiske marker
              </span>
            </label>
          </div>
          {/* Visualization Mode */}
          <div className="mb-4">
            <label className="mb-2 block text-sm font-medium text-gray-700">
              Visualiseringsmodus
            </label>
            <select
              value={filterState.visualizationMode}
              onChange={(e) =>
                onFilterChange({
                  visualizationMode: e.target
                    .value as FilterState['visualizationMode'],
                })
              }
              className="w-full rounded-md border border-gray-300 px-3 py-3 text-base focus:border-blue-500 focus:ring-2 focus:ring-blue-500 focus:outline-none lg:py-2 lg:text-sm"
            >
              <option value="total_pesticide_belastning">
                Total pesticidbelastning
              </option>
              <option value="pfas_belastning">PFAS belastning</option>
              <option value="diquat_belastning">Diquat belastning</option>
              <option value="glyphosate_belastning">
                Glyphosate belastning
              </option>
              <option value="applications_count">Antal applikationer</option>
              <option value="organic_status">Økologisk status</option>
              <option value="area_size">Markareal</option>
            </select>
            <p className="mt-1 text-xs text-gray-500">
              Belastning bruges til farvning for sammenlignelighed. Faktiske
              mængder (L, kg, tabletter) vises i detaljer.
            </p>
          </div>

          {/* Color Unit */}
          <div className="mb-4">
            <label className="mb-2 block text-sm font-medium text-gray-700">
              Farveskala enhed
            </label>
            <select
              value={filterState.colorUnit}
              onChange={(e) =>
                onFilterChange({
                  colorUnit: e.target.value as FilterState['colorUnit'],
                })
              }
              className="w-full rounded-md border border-gray-300 px-3 py-3 text-base focus:border-blue-500 focus:ring-2 focus:ring-blue-500 focus:outline-none lg:py-2 lg:text-sm"
            >
              <option value="total">Total mængde (kg/L)</option>
              <option value="per_hectare">Per hektar</option>
              <option value="belastning">Belastning (anbefalet)</option>
              <option value="applications">Antal applikationer</option>
            </select>
          </div>

          {/* Decile Coloring Toggle */}
          <div className="mb-4">
            <label className="flex items-center">
              <input
                type="checkbox"
                checked={filterState.useDecileColoring}
                onChange={(e) =>
                  onFilterChange({ useDecileColoring: e.target.checked })
                }
                className="h-5 w-5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 lg:h-4 lg:w-4"
              />
              <span className="ml-3 text-base text-gray-700 lg:ml-2 lg:text-sm">
                Brug decil-baseret farvning
              </span>
            </label>
            <p className="mt-1 text-xs text-gray-500">
              Fordeler data i 10 lige store grupper for bedre sammenligning
            </p>
          </div>
          {/* Reset Filters */}
          <button
            onClick={() =>
              onFilterChange({
                organicOnly: false,
                visualizationMode: 'total_pesticide_belastning',
                colorUnit: 'belastning',
                useDecileColoring: true,
              })
            }
            className="w-full rounded bg-gray-100 px-3 py-3 text-base text-gray-700 transition-colors hover:bg-gray-200 lg:py-2 lg:text-sm"
          >
            Nulstil filtre
          </button>
        </div>

        {/* Legend */}
        <div className="border-t pt-6">
          <h3 className="mb-4 text-lg font-semibold text-gray-900">
            Signaturforklaring
          </h3>

          {/* Decile Legend */}
          {filterState.useDecileColoring && (
            <div className="mb-4">
              <h4 className="mb-2 text-sm font-medium text-gray-800">
                Decil-baseret farvning
              </h4>
              <div className="mb-2 text-xs text-gray-600">
                Baseret på faktiske data fra{' '}
                {filterState.visualizationMode === 'pfas_belastning'
                  ? '156.025 marker med PFAS'
                  : filterState.visualizationMode === 'diquat_belastning'
                    ? '471 marker med diquat'
                    : filterState.visualizationMode === 'glyphosate_belastning'
                      ? '105.511 marker med glyphosate'
                      : '617.774 marker'}
              </div>
              <div className="space-y-1 text-xs">
                {Array.from({ length: 10 }, (_, i) => (
                  <div key={i} className="flex items-center space-x-2">
                    <div
                      className="h-3 w-3 rounded"
                      style={{
                        backgroundColor: `hsl(${240 - i * 24}, 70%, ${50 + i * 3}%)`,
                      }}
                    ></div>
                    <span>Decil {i + 1} (10% af data)</span>
                  </div>
                ))}
              </div>
              <div className="mt-2 text-xs text-gray-500 italic">
                Belastning anbefales til sammenligning mellem forskellige
                pesticider
              </div>
            </div>
          )}

          {/* Chemical-specific legend */}
          {filterState.visualizationMode.includes('pfas') && (
            <div className="mb-4">
              <h4 className="mb-2 flex items-center text-sm font-medium text-red-800">
                <TestTube className="mr-1 h-4 w-4" />
                PFAS Pesticider
              </h4>
              <div className="space-y-1 text-xs text-red-700">
                <div>Per- og polyfluorerede alkylstoffer</div>
                <div>Potentielt sundhedsskadelige</div>
                <div>Bioakkumulerende og persistente</div>
              </div>
            </div>
          )}

          {filterState.visualizationMode.includes('diquat') && (
            <div className="mb-4">
              <h4 className="mb-2 flex items-center text-sm font-medium text-blue-800">
                <Droplets className="mr-1 h-4 w-4" />
                Diquat Pesticider
              </h4>
              <div className="space-y-1 text-xs text-blue-700">
                <div>Kontakt herbicid</div>
                <div>Bruges til ukrudtsbekæmpelse</div>
                <div>Kan påvirke vandmiljøet</div>
              </div>
            </div>
          )}

          {filterState.visualizationMode.includes('glyphosate') && (
            <div className="mb-4">
              <h4 className="mb-2 flex items-center text-sm font-medium text-green-800">
                <Leaf className="mr-1 h-4 w-4" />
                Glyphosate Pesticider
              </h4>
              <div className="space-y-1 text-xs text-green-700">
                <div>Systemisk herbicid</div>
                <div>Mest anvendte ukrudtsmiddel</div>
                <div>Hæmmer planters aminosyresyntese</div>
              </div>
            </div>
          )}

          {/* Standard legend */}
          <div className="space-y-2 text-xs">
            <div className="mb-3 space-y-1">
              <div className="mb-1 text-sm font-medium text-gray-700">
                Boringsnære beskyttelsesområder:
              </div>
              <div className="ml-2 flex items-center space-x-2">
                <div className="h-4 w-4 rounded bg-yellow-500"></div>
                <span>Handling påkrævet</span>
              </div>
              <div className="ml-2 flex items-center space-x-2">
                <div className="h-4 w-4 rounded bg-green-500"></div>
                <span>Gennemført</span>
              </div>
              <div className="ml-2 flex items-center space-x-2">
                <div className="h-4 w-4 rounded bg-blue-600"></div>
                <span>Generelle BNBO områder</span>
              </div>
            </div>
            <div className="flex items-center space-x-2">
              <div className="h-4 w-4 rounded bg-gray-600"></div>
              <span>Lavbundsjorder (tørvindhold)</span>
            </div>
            <div className="flex items-center space-x-2">
              <div className="h-4 w-4 rounded bg-teal-500"></div>
              <span>Vandprojekter</span>
            </div>
            <div className="flex items-center space-x-2">
              <div className="h-4 w-4 rounded bg-emerald-600"></div>
              <span>Økologiske marker</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

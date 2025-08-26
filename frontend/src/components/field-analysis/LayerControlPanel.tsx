"use client";

import React from "react";
import { LayerVisibility, FilterState } from "./types";

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
      key: "fields" as const,
      name: "Landbrugsmarker",
      description: "617.774 marker med pesticidforbrug og miljødata",
      icon: "🌾",
      color: "bg-green-500",
    },
    {
      key: "bnbo" as const,
      name: "BNBO Områder",
      description: "2.761 boringsnære beskyttelsesområder",
      icon: "💧",
      color: "bg-blue-600",
    },
    {
      key: "wetlands" as const,
      name: "Lavbundsområder",
      description: "768.646 lavbundsjorder med tørvindhold",
      icon: "💨",
      color: "bg-gray-600",
    },
    {
      key: "water_projects" as const,
      name: "Vandprojekter",
      description: "2.138 vandprojekter til miljøgenopretning",
      icon: "🌊",
      color: "bg-teal-500",
    },
    {
      key: "buildings" as const,
      name: "Bygninger",
      description: "268.260 bygninger inden for 100m af pesticidmarker",
      icon: "🏠",
      color: "bg-gray-500",
    },
  ];

  return (
    <div className="p-4 lg:p-4 space-y-4 lg:space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-lg lg:text-xl font-bold text-gray-900 mb-2">Kortlag</h2>
        <p className="text-sm text-gray-600">
          Vælg hvilke data der skal vises på kortet
        </p>
      </div>

      {/* Layer Toggles */}
      <div className="space-y-3">
        {layerConfigs.map((layer) => (
          <div key={layer.key} className="flex items-start space-x-3">
            <div className="flex-shrink-0 mt-1">
              <button
                onClick={() => onLayerToggle(layer.key)}
                className={`min-h-[44px] min-w-[44px] lg:w-5 lg:h-5 lg:min-h-0 lg:min-w-0 rounded border-2 flex items-center justify-center ${
                  layerVisibility[layer.key]
                    ? "bg-blue-600 border-blue-600"
                    : "border-gray-300 hover:border-gray-400 active:border-gray-500"
                } transition-colors`}
              >
                {layerVisibility[layer.key] && (
                  <svg className="w-4 h-4 lg:w-3 lg:h-3 text-white" fill="currentColor" viewBox="0 0 20 20">
                    <path
                      fillRule="evenodd"
                      d="M16.707 5.293a1 1 0 010 1.414l-8 8a1 1 0 01-1.414 0l-4-4a1 1 0 011.414-1.414L8 12.586l7.293-7.293a1 1 0 011.414 0z"
                      clipRule="evenodd"
                    />
                  </svg>
                )}
              </button>
            </div>

            <div className="flex-1 min-w-0">
              <div className="flex items-center space-x-2">
                <span className="text-xl lg:text-lg">{layer.icon}</span>
                <h3 className="font-medium text-gray-900 text-base lg:text-sm">{layer.name}</h3>
              </div>
              <p className="text-xs text-gray-500 mt-1">{layer.description}</p>
            </div>
          </div>
        ))}
      </div>

      {/* Filters Section */}
      <div className="border-t pt-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Filtre</h3>



        {/* Organic Filter */}
        <div className="mb-4">
          <label className="flex items-center">
            <input
              type="checkbox"
              checked={filterState.organicOnly}
              onChange={(e) => onFilterChange({ organicOnly: e.target.checked })}
              className="h-5 w-5 lg:h-4 lg:w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <span className="ml-3 lg:ml-2 text-base lg:text-sm text-gray-700">Kun økologiske marker</span>
          </label>
        </div>



        {/* Visualization Mode */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Visualiseringsmodus
          </label>
          <select
            value={filterState.visualizationMode}
            onChange={(e) => onFilterChange({ visualizationMode: e.target.value as FilterState['visualizationMode'] })}
            className="w-full px-3 py-3 lg:py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-base lg:text-sm"
          >
            <option value="total_pesticide_belastning">Total pesticidbelastning</option>
            <option value="pfas_belastning">PFAS belastning</option>
            <option value="diquat_belastning">Diquat belastning</option>
            <option value="glyphosate_belastning">Glyphosate belastning</option>
            <option value="applications_count">Antal applikationer</option>
            <option value="organic_status">Økologisk status</option>
            <option value="area_size">Markareal</option>
          </select>
          <p className="text-xs text-gray-500 mt-1">
            Belastning bruges til farvning for sammenlignelighed. Faktiske mængder (L, kg, tabletter) vises i detaljer.
          </p>
        </div>

        {/* Color Unit */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Farveskala enhed
          </label>
          <select
            value={filterState.colorUnit}
            onChange={(e) => onFilterChange({ colorUnit: e.target.value as FilterState['colorUnit'] })}
            className="w-full px-3 py-3 lg:py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-base lg:text-sm"
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
              onChange={(e) => onFilterChange({ useDecileColoring: e.target.checked })}
              className="h-5 w-5 lg:h-4 lg:w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <span className="ml-3 lg:ml-2 text-base lg:text-sm text-gray-700">Brug decil-baseret farvning</span>
          </label>
          <p className="text-xs text-gray-500 mt-1">
            Fordeler data i 10 lige store grupper for bedre sammenligning
          </p>
        </div>





        {/* Reset Filters */}
        <button
          onClick={() => onFilterChange({
            organicOnly: false,
            visualizationMode: 'total_pesticide_belastning',
            colorUnit: 'belastning',
            useDecileColoring: true,
          })}
          className="w-full px-3 py-3 lg:py-2 text-base lg:text-sm bg-gray-100 text-gray-700 rounded hover:bg-gray-200 transition-colors"
        >
          Nulstil filtre
        </button>
      </div>

      {/* Legend */}
      <div className="border-t pt-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Signaturforklaring</h3>

        {/* Decile Legend */}
        {filterState.useDecileColoring && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-gray-800 mb-2">Decil-baseret farvning</h4>
            <div className="text-xs text-gray-600 mb-2">
              Baseret på faktiske data fra {filterState.visualizationMode === 'pfas_belastning' ? '156.025 marker med PFAS' :
              filterState.visualizationMode === 'diquat_belastning' ? '471 marker med diquat' :
              filterState.visualizationMode === 'glyphosate_belastning' ? '105.511 marker med glyphosate' : '617.774 marker'}
            </div>
            <div className="space-y-1 text-xs">
              {Array.from({ length: 10 }, (_, i) => (
                <div key={i} className="flex items-center space-x-2">
                  <div
                    className="w-3 h-3 rounded"
                    style={{
                      backgroundColor: `hsl(${240 - (i * 24)}, 70%, ${50 + (i * 3)}%)`
                    }}
                  ></div>
                  <span>Decil {i + 1} (10% af data)</span>
                </div>
              ))}
            </div>
            <div className="text-xs text-gray-500 mt-2 italic">
              Belastning anbefales til sammenligning mellem forskellige pesticider
            </div>
          </div>
        )}

        {/* Chemical-specific legend */}
        {filterState.visualizationMode.includes('pfas') && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-red-800 mb-2">🧪 PFAS Pesticider</h4>
            <div className="space-y-1 text-xs text-red-700">
              <div>Per- og polyfluorerede alkylstoffer</div>
              <div>Potentielt sundhedsskadelige</div>
              <div>Bioakkumulerende og persistente</div>
            </div>
          </div>
        )}

        {filterState.visualizationMode.includes('diquat') && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-blue-800 mb-2">💧 Diquat Pesticider</h4>
            <div className="space-y-1 text-xs text-blue-700">
              <div>Kontakt herbicid</div>
              <div>Bruges til ukrudtsbekæmpelse</div>
              <div>Kan påvirke vandmiljøet</div>
            </div>
          </div>
        )}

        {filterState.visualizationMode.includes('glyphosate') && (
          <div className="mb-4">
            <h4 className="text-sm font-medium text-green-800 mb-2">🌿 Glyphosate Pesticider</h4>
            <div className="space-y-1 text-xs text-green-700">
              <div>Systemisk herbicid</div>
              <div>Mest anvendte ukrudtsmiddel</div>
              <div>Hæmmer planters aminosyresyntese</div>
            </div>
          </div>
        )}

        {/* Standard legend */}
        <div className="space-y-2 text-xs">
          <div className="space-y-1 mb-3">
            <div className="text-sm font-medium text-gray-700 mb-1">Boringsnære beskyttelsesområder:</div>
            <div className="flex items-center space-x-2 ml-2">
              <div className="w-4 h-4 bg-yellow-500 rounded"></div>
              <span>Handling påkrævet</span>
            </div>
            <div className="flex items-center space-x-2 ml-2">
              <div className="w-4 h-4 bg-green-500 rounded"></div>
              <span>Gennemført</span>
            </div>
            <div className="flex items-center space-x-2 ml-2">
              <div className="w-4 h-4 bg-blue-600 rounded"></div>
              <span>Generelle BNBO områder</span>
            </div>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-gray-600 rounded"></div>
            <span>Lavbundsjorder (tørvindhold)</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-teal-500 rounded"></div>
            <span>Vandprojekter</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-emerald-600 rounded"></div>
            <span>Økologiske marker</span>
          </div>
        </div>
      </div>
    </div>
  );
}

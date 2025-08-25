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
    <div className="p-4 space-y-6">
      {/* Header */}
      <div>
        <h2 className="text-xl font-bold text-gray-900 mb-2">Kortlag</h2>
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
                className={`w-5 h-5 rounded border-2 flex items-center justify-center ${
                  layerVisibility[layer.key]
                    ? "bg-blue-600 border-blue-600"
                    : "border-gray-300 hover:border-gray-400"
                }`}
              >
                {layerVisibility[layer.key] && (
                  <svg className="w-3 h-3 text-white" fill="currentColor" viewBox="0 0 20 20">
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
                <span className="text-lg">{layer.icon}</span>
                <h3 className="font-medium text-gray-900">{layer.name}</h3>
              </div>
              <p className="text-xs text-gray-500 mt-1">{layer.description}</p>
            </div>
          </div>
        ))}
      </div>

      {/* Filters Section */}
      <div className="border-t pt-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Filtre</h3>

        {/* Kommune Filter */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Kommune
          </label>
          <select
            multiple
            value={filterState.kommune}
            onChange={(e) => {
              const values = Array.from(e.target.selectedOptions, (option) => option.value);
              onFilterChange({ kommune: values });
            }}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">Alle kommuner</option>
            <option value="copenhagen">København</option>
            <option value="aarhus">Aarhus</option>
            <option value="aalborg">Aalborg</option>
            {/* Add more municipalities */}
          </select>
        </div>

        {/* Organic Filter */}
        <div className="mb-4">
          <label className="flex items-center">
            <input
              type="checkbox"
              checked={filterState.organicOnly}
              onChange={(e) => onFilterChange({ organicOnly: e.target.checked })}
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <span className="ml-2 text-sm text-gray-700">Kun økologiske marker</span>
          </label>
        </div>

        {/* Area Range Filter */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Markareal (hektar)
          </label>
          <div className="flex items-center space-x-2">
            <input
              type="number"
              placeholder="Min"
              value={filterState.areaRange[0]}
              onChange={(e) => {
                const newMin = parseInt(e.target.value) || 0;
                onFilterChange({ areaRange: [newMin, filterState.areaRange[1]] });
              }}
              className="w-20 px-2 py-1 border border-gray-300 rounded text-sm"
            />
            <span className="text-gray-500">-</span>
            <input
              type="number"
              placeholder="Max"
              value={filterState.areaRange[1]}
              onChange={(e) => {
                const newMax = parseInt(e.target.value) || 1000;
                onFilterChange({ areaRange: [filterState.areaRange[0], newMax] });
              }}
              className="w-20 px-2 py-1 border border-gray-300 rounded text-sm"
            />
          </div>
        </div>

        {/* Visualization Mode */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Visualiseringsmodus
          </label>
          <select
            value={filterState.visualizationMode}
            onChange={(e) => onFilterChange({ visualizationMode: e.target.value as FilterState['visualizationMode'] })}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
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
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
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
              className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
            />
            <span className="ml-2 text-sm text-gray-700">Brug decil-baseret farvning</span>
          </label>
          <p className="text-xs text-gray-500 mt-1">
            Fordeler data i 10 lige store grupper for bedre sammenligning
          </p>
        </div>

        {/* Chemical Filter */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Kemisk filter
          </label>
          <select
            value={filterState.chemicalFilter}
            onChange={(e) => onFilterChange({ chemicalFilter: e.target.value as FilterState['chemicalFilter'] })}
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="all">Alle pesticider</option>
            <option value="pfas">Kun PFAS</option>
            <option value="diquat">Kun Diquat</option>
            <option value="glyphosate">Kun Glyphosate</option>
            <option value="none">Ingen pesticider</option>
          </select>
        </div>

        {/* Pesticide Threshold */}
        <div className="mb-4">
          <label className="block text-sm font-medium text-gray-700 mb-2">
            Min. pesticidbelastning
          </label>
          <input
            type="range"
            min="0"
            max="100"
            value={filterState.pesticideThreshold}
            onChange={(e) => onFilterChange({ pesticideThreshold: parseInt(e.target.value) })}
            className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer"
          />
          <div className="flex justify-between text-xs text-gray-500 mt-1">
            <span>0</span>
            <span>{filterState.pesticideThreshold}</span>
            <span>100+</span>
          </div>
        </div>

        {/* PFAS Threshold */}
        {(filterState.chemicalFilter === 'all' || filterState.chemicalFilter === 'pfas') && (
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Min. PFAS indhold (g)
            </label>
            <input
              type="range"
              min="0"
              max="50"
              value={filterState.pfasThreshold}
              onChange={(e) => onFilterChange({ pfasThreshold: parseInt(e.target.value) })}
              className="w-full h-2 bg-red-200 rounded-lg appearance-none cursor-pointer"
            />
            <div className="flex justify-between text-xs text-gray-500 mt-1">
              <span>0g</span>
              <span>{filterState.pfasThreshold}g</span>
              <span>50+g</span>
            </div>
          </div>
        )}

        {/* Diquat Threshold */}
        {(filterState.chemicalFilter === 'all' || filterState.chemicalFilter === 'diquat') && (
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Min. Diquat indhold (g)
            </label>
            <input
              type="range"
              min="0"
              max="100"
              value={filterState.diquatThreshold}
              onChange={(e) => onFilterChange({ diquatThreshold: parseInt(e.target.value) })}
              className="w-full h-2 bg-blue-200 rounded-lg appearance-none cursor-pointer"
            />
            <div className="flex justify-between text-xs text-gray-500 mt-1">
              <span>0g</span>
              <span>{filterState.diquatThreshold}g</span>
              <span>100+g</span>
            </div>
          </div>
        )}

        {/* Glyphosate Threshold */}
        {(filterState.chemicalFilter === 'all' || filterState.chemicalFilter === 'glyphosate') && (
          <div className="mb-4">
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Min. Glyphosate indhold (g)
            </label>
            <input
              type="range"
              min="0"
              max="500"
              value={filterState.glyphosateThreshold}
              onChange={(e) => onFilterChange({ glyphosateThreshold: parseInt(e.target.value) })}
              className="w-full h-2 bg-green-200 rounded-lg appearance-none cursor-pointer"
            />
            <div className="flex justify-between text-xs text-gray-500 mt-1">
              <span>0g</span>
              <span>{filterState.glyphosateThreshold}g</span>
              <span>500+g</span>
            </div>
          </div>
        )}

        {/* Reset Filters */}
        <button
          onClick={() => onFilterChange({
            kommune: [],
            cropTypes: [],
            organicOnly: false,
            areaRange: [0, 1000],
            pesticideThreshold: 0,
            pfasThreshold: 0,
            diquatThreshold: 0,
            glyphosateThreshold: 0,
            chemicalFilter: 'all',
            visualizationMode: 'total_pesticide_belastning',
            colorUnit: 'belastning',
            useDecileColoring: true,
          })}
          className="w-full px-3 py-2 text-sm bg-gray-100 text-gray-700 rounded hover:bg-gray-200 transition-colors"
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

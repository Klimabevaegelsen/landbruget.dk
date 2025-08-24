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
      description: "2.761 biodiversitets- og naturgenopretningsområder",
      icon: "🌱",
      color: "bg-emerald-500",
    },
    {
      key: "wetlands" as const,
      name: "Vådområder",
      description: "768.646 vådområder med fugtighedsniveauer",
      icon: "💧",
      color: "bg-blue-500",
    },
    {
      key: "waterProjects" as const,
      name: "Vandprojekter",
      description: "2.138 vandprojekter til miljøgenopretning",
      icon: "🌊",
      color: "bg-teal-500",
    },
    {
      key: "buildings" as const,
      name: "Bygninger",
      description: "Bygninger inden for 100m af marker",
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

        {/* Reset Filters */}
        <button
          onClick={() => onFilterChange({
            kommune: [],
            cropTypes: [],
            organicOnly: false,
            areaRange: [0, 1000],
            pesticideThreshold: 0,
          })}
          className="w-full px-3 py-2 text-sm bg-gray-100 text-gray-700 rounded hover:bg-gray-200 transition-colors"
        >
          Nulstil filtre
        </button>
      </div>

      {/* Legend */}
      <div className="border-t pt-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Signaturforklaring</h3>
        <div className="space-y-2 text-xs">
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-red-500 rounded"></div>
            <span>Høj pesticidbelastning</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-green-500 rounded"></div>
            <span>BNBO - Gennemført</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-orange-500 rounded"></div>
            <span>BNBO - Handling påkrævet</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-blue-500 rounded"></div>
            <span>Vådområder</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-4 h-4 bg-teal-500 rounded"></div>
            <span>Vandprojekter</span>
          </div>
        </div>
      </div>
    </div>
  );
}

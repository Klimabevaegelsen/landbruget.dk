"use client";

import React, { useEffect, useRef, useState, useCallback } from "react";
import Map, {
  MapLayerMouseEvent,
  NavigationControl,
} from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { LayerVisibility, FilterState, FieldAnalysisData } from "./types";
import { getDecileBreakpoints, getColorScheme } from "./colorUtils";

// Type for MapLibre map instance
interface MapInstance {
  getSource: (id: string) => unknown;
  getLayer: (id: string) => unknown;
  addLayer: (layer: unknown) => void;
  setLayoutProperty: (id: string, prop: string, value: string) => void;
  setPaintProperty: (id: string, prop: string, value: unknown) => void;
  addSource: (id: string, source: unknown) => void;
  addImage: (id: string, image: HTMLCanvasElement | ImageBitmap | ImageData) => void;
}

interface FieldAnalysisMapProps {
  pmtilesUrls: {
    fields: string;
    bnbo: string;
    wetlands: string;
    water_projects: string;
    buildings: string;
  };
  layerVisibility: LayerVisibility;
  filterState: FilterState;
  onFieldSelect: (fieldData: FieldAnalysisData) => void;
}

interface TooltipInfo {
  x: number;
  y: number;
  properties: Record<string, unknown>;
  layerName: string;
  visualizationMode: FilterState['visualizationMode'];
  colorUnit: FilterState['colorUnit'];
}

function MapTooltip({ x, y, properties, layerName, visualizationMode, colorUnit }: TooltipInfo) {
  const formatValue = (value: unknown, unit?: string): string => {
    if (typeof value === "number") {
      const formatted = value.toLocaleString("da-DK", { maximumFractionDigits: 2 });
      return unit ? `${formatted} ${unit}` : formatted;
    }
    return String(value);
  };

  const getRelevantData = () => {
    const data: Array<{ label: string; value: unknown; unit?: string }> = [];

    // Always show basic field info
    if (properties.crop_name) {
      data.push({ label: "Afgrøde", value: properties.crop_name });
    }

    if (properties.area_hectares) {
      data.push({ label: "Areal", value: properties.area_hectares, unit: "ha" });
    }

    if (properties.is_organic !== undefined) {
      data.push({ label: "Økologisk", value: properties.is_organic ? "Ja" : "Nej" });
    }

    if (properties.kommune) {
      data.push({ label: "Kommune", value: properties.kommune });
    }

    // Show BNBO status if available
    if (properties.status_category) {
      const statusLabel = properties.status_category === "Action Required"
        ? "BNBO handling påkrævet"
        : properties.status_category === "Completed"
        ? "BNBO gennemført"
        : "BNBO status";
      data.push({ label: statusLabel, value: properties.status_category });
    }

    // Show data relevant to current visualization mode
    switch (visualizationMode) {
      case 'total_pesticide_belastning':
        if (properties.total_pesticide_belastning) {
          data.push({
            label: "Total pesticidbelastning",
            value: properties.total_pesticide_belastning,
            unit: colorUnit === 'per_hectare' ? 'per ha' : ''
          });
        }
        if (properties.total_pesticide_applications) {
          data.push({ label: "Antal applikationer", value: properties.total_pesticide_applications });
        }
        break;

      case 'pfas_belastning':
        if (properties.total_pfas_belastning) {
          data.push({
            label: "PFAS belastning",
            value: properties.total_pfas_belastning,
            unit: colorUnit === 'per_hectare' ? 'per ha' : ''
          });
        }
        if (properties.total_pfas_active_ingredient_kg) {
          data.push({ label: "PFAS aktivstof", value: properties.total_pfas_active_ingredient_kg, unit: "kg" });
        }
        if (properties.pfas_applications) {
          data.push({ label: "PFAS applikationer", value: properties.pfas_applications });
        }
        break;

      case 'diquat_belastning':
        if (properties.total_diquat_belastning) {
          data.push({
            label: "Diquat belastning",
            value: properties.total_diquat_belastning,
            unit: colorUnit === 'per_hectare' ? 'per ha' : ''
          });
        }
        if (properties.diquat_applications) {
          data.push({ label: "Diquat applikationer", value: properties.diquat_applications });
        }
        break;

      case 'glyphosate_belastning':
        if (properties.total_glyphosate_belastning) {
          data.push({
            label: "Glyphosate belastning",
            value: properties.total_glyphosate_belastning,
            unit: colorUnit === 'per_hectare' ? 'per ha' : ''
          });
        }
        if (properties.total_glyphosate_active_ingredient_kg) {
          data.push({ label: "Glyphosate aktivstof", value: properties.total_glyphosate_active_ingredient_kg, unit: "kg" });
        }
        if (properties.glyphosate_applications) {
          data.push({ label: "Glyphosate applikationer", value: properties.glyphosate_applications });
        }
        break;

      case 'applications_count':
        if (properties.total_pesticide_applications) {
          data.push({ label: "Total applikationer", value: properties.total_pesticide_applications });
        }
        if (properties.unique_pesticide_products) {
          data.push({ label: "Unikke produkter", value: properties.unique_pesticide_products });
        }
        break;

      case 'area_size':
        if (properties.area_hectares) {
          data.push({ label: "Markareal", value: properties.area_hectares, unit: "ha" });
        }
        break;
    }

    return data.slice(0, 6); // Limit to 6 items
  };

  const relevantData = getRelevantData();

  return (
    <div
      className="absolute p-3 bg-white rounded-lg shadow-lg border border-gray-200 z-50 max-w-xs"
      style={{
        left: x,
        top: y,
        transform: "translate(-50%, -100%)",
        marginTop: -10,
      }}
    >
      <p className="text-sm font-semibold text-gray-900 mb-2">{layerName}</p>
      <div className="space-y-1 text-xs">
        {relevantData.map(({ label, value, unit }, index) => (
          <div key={index} className="flex justify-between">
            <span className="text-gray-600">
              {label}:
            </span>
            <span className="font-medium text-gray-900 ml-2">
              {formatValue(value, unit)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

export default function FieldAnalysisMap({
  pmtilesUrls,
  layerVisibility,
  filterState,
  onFieldSelect,
}: FieldAnalysisMapProps) {
  const mapRef = useRef<{ getMap: () => MapInstance } | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hoverInfo, setHoverInfo] = useState<TooltipInfo | null>(null);

  // Initialize PMTiles protocol
  useEffect(() => {
    const initializePMTiles = async () => {
      try {
        // Import MapLibre GL dynamically to avoid SSR issues
        const [maplibregl, { Protocol }] = await Promise.all([
          import('maplibre-gl'),
          import('pmtiles')
        ]);

        console.log('✅ MapLibre and PMTiles loaded successfully');

        // Register PMTiles protocol with MapLibre (only once globally)
        if (!(window as unknown as { __pmtiles_protocol_registered?: boolean }).__pmtiles_protocol_registered) {
          const protocol = new Protocol();
          maplibregl.default.addProtocol('pmtiles', protocol.tile);
          (window as unknown as { __pmtiles_protocol_registered?: boolean }).__pmtiles_protocol_registered = true;
          console.log('✅ PMTiles protocol registered');
        }

        setIsLoading(false);
      } catch (err) {
        console.error("❌ Failed to initialize PMTiles:", err);
        setError("Kunne ikke indlæse kortdata");
        setIsLoading(false);
      }
    };

    initializePMTiles();
  }, []);

  // Generate dynamic paint properties based on visualization mode
  const generateFieldsPaint = useCallback(() => {
    const { visualizationMode, colorUnit, useDecileColoring } = filterState;
    const colorScheme = getColorScheme(visualizationMode);

    // Handle organic status visualization with symbols
    if (visualizationMode === 'organic_status') {
      return {
        "fill-color": [
          "case",
          ["==", ["get", "is_organic"], true],
          "transparent", // Transparent fill for organic fields - will use symbols instead
          "#f3f4f6" // Light gray for non-organic
        ],
        "fill-opacity": 0.6,
      };
    }

    // Get the appropriate field name for the visualization mode
    const getFieldName = (mode: FilterState['visualizationMode']) => {
      switch (mode) {
        case 'total_pesticide_belastning': return 'total_pesticide_belastning';
        case 'pfas_belastning': return 'total_pfas_belastning';
        case 'diquat_belastning': return 'total_diquat_belastning';
        case 'glyphosate_belastning': return 'total_glyphosate_belastning';
        case 'applications_count': return 'total_pesticide_applications';
        case 'area_size': return 'area_hectares';
        default: return 'total_pesticide_belastning';
      }
    };

    const fieldName = getFieldName(visualizationMode);
    console.log('🎨 Color generation:', { visualizationMode, fieldName, colorScheme: colorScheme.name });

            if (useDecileColoring) {
      // Use decile-based coloring with step function
      const breakpoints = getDecileBreakpoints(visualizationMode, colorUnit);
      const colors = colorScheme.colors;

      return {
        "fill-color": [
          "case",
          ["<=", ["coalesce", ["get", fieldName], 0], 0],
          "#f3f4f6", // Light gray for zero/negative values
          [
            "step",
            ["coalesce", ["get", fieldName], 0],
            colors[0], // Base color for lowest values
            breakpoints[0], colors[1],
            breakpoints[1], colors[2],
            breakpoints[2], colors[3],
            breakpoints[3], colors[4],
            breakpoints[4], colors[5],
            breakpoints[5], colors[6],
            breakpoints[6], colors[7],
            breakpoints[7], colors[8],
            breakpoints[8], colors[9]
          ]
        ],
        "fill-opacity": 0.7,
      };
    } else {
      // Use linear interpolation with proper structure
      const colors = colorScheme.colors;
      return {
        "fill-color": [
          "case",
          ["<=", ["coalesce", ["get", fieldName], 0], 0],
          "#f3f4f6", // Light gray for zero/negative values
          [
            "interpolate",
            ["linear"],
            ["coalesce", ["get", fieldName], 0],
            0.1, colors[0],
            1, colors[2],
            10, colors[4],
            50, colors[6],
            100, colors[8],
            500, colors[9]
          ]
        ],
        "fill-opacity": 0.7,
      };
    }
  }, [filterState]);

  // Add field analysis layers
  const addFieldsLayers = useCallback((map: MapInstance) => {
    if (map.getSource("fields") && !map.getLayer("fields-fill")) {
      const paintProps = generateFieldsPaint();

      // Main fields layer
      map.addLayer({
        id: "fields-fill",
        source: "fields",
        "source-layer": "fields",
        type: "fill",
        paint: paintProps,
        layout: {
          visibility: layerVisibility.fields ? "visible" : "none",
        },
      });

      // Fields outline
      map.addLayer({
        id: "fields-outline",
        source: "fields",
        "source-layer": "fields",
        type: "line",
        paint: {
          "line-color": "#374151",
          "line-width": 0.5,
          "line-opacity": 0.8,
        },
        layout: {
          visibility: layerVisibility.fields ? "visible" : "none",
        },
      });

      // Add organic symbols layer
      if (filterState.visualizationMode === 'organic_status') {
        map.addLayer({
          id: "organic-symbols",
          source: "fields",
          "source-layer": "fields",
          type: "symbol",
          filter: ["==", ["get", "is_organic"], true],
          paint: {
            "text-color": "#16a34a",
            "text-halo-color": "#ffffff",
            "text-halo-width": 1,
          },
          layout: {
            "text-field": "🌿",
            "text-size": 16,
            "text-allow-overlap": false,
            "text-ignore-placement": false,
            visibility: layerVisibility.fields ? "visible" : "none",
          },
        });
      }
    }
  }, [layerVisibility.fields, generateFieldsPaint, filterState.visualizationMode]);

  // Add BNBO layers with cross-hatch pattern
  const addBNBOLayers = useCallback((map: MapInstance) => {
    if (map.getSource("bnbo") && !map.getLayer("bnbo-fill")) {
      // Create status-based patterns for BNBO
      const createBNBOPatterns = async () => {
        try {
          // Create completed pattern (green with diagonal lines)
          const completedCanvas = document.createElement('canvas');
          const completedCtx = completedCanvas.getContext('2d');
          completedCanvas.width = 32;
          completedCanvas.height = 32;

          if (completedCtx) {
            completedCtx.fillStyle = '#10B981'; // Green background
            completedCtx.fillRect(0, 0, 32, 32);
            completedCtx.strokeStyle = 'rgba(255, 255, 255, 0.3)';
            completedCtx.lineWidth = 2;
            completedCtx.beginPath();
            // Diagonal lines
            for (let i = -32; i <= 64; i += 8) {
              completedCtx.moveTo(i, 0);
              completedCtx.lineTo(i + 32, 32);
            }
            completedCtx.stroke();

            const completedBitmap = await createImageBitmap(completedCanvas);
            map.addImage('bnbo-completed-pattern', completedBitmap);
          }

          // Create action required pattern (red with cross-hatch)
          const actionCanvas = document.createElement('canvas');
          const actionCtx = actionCanvas.getContext('2d');
          actionCanvas.width = 32;
          actionCanvas.height = 32;

          if (actionCtx) {
            actionCtx.fillStyle = '#EF4444'; // Red background
            actionCtx.fillRect(0, 0, 32, 32);
            actionCtx.strokeStyle = 'rgba(255, 255, 255, 0.4)';
            actionCtx.lineWidth = 2;
            actionCtx.beginPath();
            // Cross-hatch pattern
            for (let i = -32; i <= 64; i += 8) {
              actionCtx.moveTo(i, 0);
              actionCtx.lineTo(i + 32, 32);
              actionCtx.moveTo(i + 32, 0);
              actionCtx.lineTo(i, 32);
            }
            actionCtx.stroke();

            const actionBitmap = await createImageBitmap(actionCanvas);
            map.addImage('bnbo-action-pattern', actionBitmap);
          }
        } catch (error) {
          console.warn('Failed to create BNBO patterns:', error);
        }
      };

      map.addLayer({
        id: "bnbo-fill",
        source: "bnbo",
        "source-layer": "bnbo",
        type: "fill",
        paint: {
                    "fill-color": [
            "case",
            // If action is required (red)
            ["==", ["get", "status_category"], "Action Required"],
            "#EF4444",
            // If completed (green)
            ["==", ["get", "status_category"], "Completed"],
            "#10B981",
            // Default blue for general BNBO areas
            "#2563EB"
          ],
          "fill-opacity": 0.6,
        },
        layout: {
          visibility: layerVisibility.bnbo ? "visible" : "none",
        },
      });

      // Create patterns after layer is added
      createBNBOPatterns().then(() => {
        // Apply patterns based on status
        if (map.getLayer("bnbo-fill")) {
          map.setPaintProperty("bnbo-fill", "fill-pattern", [
            "case",
            ["==", ["get", "status_category"], "Action Required"],
            "bnbo-action-pattern",
            ["==", ["get", "status_category"], "Completed"],
            "bnbo-completed-pattern",
            "" // No pattern for general areas
          ]);
        }
      });

      map.addLayer({
        id: "bnbo-outline",
        source: "bnbo",
        "source-layer": "bnbo",
        type: "line",
        paint: {
          "line-color": [
            "case",
            ["==", ["get", "status_category"], "Action Required"],
            "#DC2626", // Darker red outline
            ["==", ["get", "status_category"], "Completed"],
            "#059669", // Darker green outline
            "#1D4ED8"  // Darker blue outline
          ],
          "line-width": 1.5,
          "line-opacity": 0.9,
        },
        layout: {
          visibility: layerVisibility.bnbo ? "visible" : "none",
        },
      });
    }
  }, [layerVisibility.bnbo]);

  // Add wetlands layers with wave pattern
  const addWetlandsLayers = useCallback((map: MapInstance) => {
    if (map.getSource("wetlands") && !map.getLayer("wetlands-fill")) {
      // Create wave pattern for wetlands
      const createWetlandsPattern = async () => {
        try {
          const canvas = document.createElement('canvas');
          const ctx = canvas.getContext('2d');
          canvas.width = 24;
          canvas.height = 16;

          if (ctx) {
            // Fill with blue background
            ctx.fillStyle = '#3B82F6';
            ctx.fillRect(0, 0, 24, 16);

            // Add wave pattern
            ctx.strokeStyle = 'rgba(255, 255, 255, 0.5)';
            ctx.lineWidth = 1.5;
            ctx.beginPath();
            // Top wave
            ctx.moveTo(0, 4);
            ctx.quadraticCurveTo(6, 2, 12, 4);
            ctx.quadraticCurveTo(18, 6, 24, 4);
            // Middle wave
            ctx.moveTo(0, 8);
            ctx.quadraticCurveTo(6, 6, 12, 8);
            ctx.quadraticCurveTo(18, 10, 24, 8);
            // Bottom wave
            ctx.moveTo(0, 12);
            ctx.quadraticCurveTo(6, 10, 12, 12);
            ctx.quadraticCurveTo(18, 14, 24, 12);
            ctx.stroke();

            const imageBitmap = await createImageBitmap(canvas);
            map.addImage('wetlands-pattern', imageBitmap);

            // Update layer to use pattern
            if (map.getLayer("wetlands-fill")) {
              map.setPaintProperty("wetlands-fill", "fill-pattern", "wetlands-pattern");
              map.setPaintProperty("wetlands-fill", "fill-opacity", 0.4);
            }
          }
        } catch (error) {
          console.warn('Failed to create wetlands pattern:', error);
        }
      };

      map.addLayer({
        id: "wetlands-fill",
        source: "wetlands",
        "source-layer": "wetlands",
        type: "fill",
        paint: {
          "fill-color": "#3B82F6", // Fallback color
          "fill-opacity": 0.4,
        },
        layout: {
          visibility: layerVisibility.wetlands ? "visible" : "none",
        },
      });

      // Create pattern after layer is added
      createWetlandsPattern();

      map.addLayer({
        id: "wetlands-outline",
        source: "wetlands",
        "source-layer": "wetlands",
        type: "line",
        paint: {
          "line-color": "#1E40AF",
          "line-width": 1.5,
          "line-opacity": 0.8,
        },
        layout: {
          visibility: layerVisibility.wetlands ? "visible" : "none",
        },
      });
    }
  }, [layerVisibility.wetlands]);

  // Add water projects layers with dot pattern
  const addWaterProjectsLayers = useCallback((map: MapInstance) => {
    if (map.getSource("water_projects") && !map.getLayer("water-projects-fill")) {
      // Create dot pattern for water projects
      const createWaterProjectsPattern = async () => {
        try {
          const canvas = document.createElement('canvas');
          const ctx = canvas.getContext('2d');
          canvas.width = 20;
          canvas.height = 20;

          if (ctx) {
            // Fill with teal background
            ctx.fillStyle = '#14B8A6';
            ctx.fillRect(0, 0, 20, 20);

            // Add dot pattern
            ctx.fillStyle = 'rgba(255, 255, 255, 0.6)';
            // Create dots in a grid pattern
            const dotSize = 2;
            const spacing = 6;
            for (let x = spacing/2; x < 20; x += spacing) {
              for (let y = spacing/2; y < 20; y += spacing) {
                ctx.beginPath();
                ctx.arc(x, y, dotSize, 0, 2 * Math.PI);
                ctx.fill();
              }
            }

            const imageBitmap = await createImageBitmap(canvas);
            map.addImage('water-projects-pattern', imageBitmap);

            // Update layer to use pattern
            if (map.getLayer("water-projects-fill")) {
              map.setPaintProperty("water-projects-fill", "fill-pattern", "water-projects-pattern");
              map.setPaintProperty("water-projects-fill", "fill-opacity", 0.5);
            }
          }
        } catch (error) {
          console.warn('Failed to create water projects pattern:', error);
        }
      };

      map.addLayer({
        id: "water-projects-fill",
        source: "water_projects",
        "source-layer": "water_projects",
        type: "fill",
        paint: {
          "fill-color": "#14B8A6", // Fallback color
          "fill-opacity": 0.5,
        },
        layout: {
          visibility: layerVisibility.water_projects ? "visible" : "none",
        },
      });

      // Create pattern after layer is added
      createWaterProjectsPattern();

      map.addLayer({
        id: "water-projects-outline",
        source: "water_projects",
        "source-layer": "water_projects",
        type: "line",
        paint: {
          "line-color": "#0F766E",
          "line-width": 2,
          "line-opacity": 0.9,
        },
        layout: {
          visibility: layerVisibility.water_projects ? "visible" : "none",
        },
      });
    }
  }, [layerVisibility.water_projects]);

  // Handle map load and add sources
  const onMapLoad = useCallback(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    try {
      // Add PMTiles sources
      Object.entries(pmtilesUrls).forEach(([layerName, url]) => {
        if (url && !map.getSource(layerName)) {
          map.addSource(layerName, {
            type: "vector",
            url: `pmtiles://${url}`,
          });
          console.log(`✅ Added ${layerName} source:`, url);
        }
      });

      // Add layers
      addFieldsLayers(map);
      addBNBOLayers(map);
      addWetlandsLayers(map);
      addWaterProjectsLayers(map);

    } catch (err) {
      console.error("Error adding map sources/layers:", err);
    }
  }, [pmtilesUrls, addFieldsLayers, addBNBOLayers, addWetlandsLayers, addWaterProjectsLayers]);

  // Update layer visibility and styling when props change
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    // Update fields layers
    if (map.getLayer("fields-fill")) {
      map.setLayoutProperty("fields-fill", "visibility", layerVisibility.fields ? "visible" : "none");
      map.setLayoutProperty("fields-outline", "visibility", layerVisibility.fields ? "visible" : "none");

      // Update organic symbols visibility
      if (map.getLayer("organic-symbols")) {
        map.setLayoutProperty("organic-symbols", "visibility",
          layerVisibility.fields && filterState.visualizationMode === 'organic_status' ? "visible" : "none"
        );
      }
    }

    // Update BNBO layers
    if (map.getLayer("bnbo-fill")) {
      map.setLayoutProperty("bnbo-fill", "visibility", layerVisibility.bnbo ? "visible" : "none");
      map.setLayoutProperty("bnbo-outline", "visibility", layerVisibility.bnbo ? "visible" : "none");
    }

    // Update wetlands layers
    if (map.getLayer("wetlands-fill")) {
      map.setLayoutProperty("wetlands-fill", "visibility", layerVisibility.wetlands ? "visible" : "none");
      map.setLayoutProperty("wetlands-outline", "visibility", layerVisibility.wetlands ? "visible" : "none");
    }

    // Update water projects layers
    if (map.getLayer("water-projects-fill")) {
      map.setLayoutProperty("water-projects-fill", "visibility", layerVisibility.water_projects ? "visible" : "none");
      map.setLayoutProperty("water-projects-outline", "visibility", layerVisibility.water_projects ? "visible" : "none");
    }
  }, [layerVisibility, filterState.visualizationMode]);

  // Update field visualization when filterState changes
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    if (map.getLayer("fields-fill")) {
      const paintProps = generateFieldsPaint();

      // Update the fill color
      map.setPaintProperty("fields-fill", "fill-color", paintProps["fill-color"]);
      map.setPaintProperty("fields-fill", "fill-opacity", paintProps["fill-opacity"]);

      // Handle organic symbols layer
      if (filterState.visualizationMode === 'organic_status') {
        // Add organic symbols if not exists
        if (!map.getLayer("organic-symbols")) {
          map.addLayer({
            id: "organic-symbols",
            source: "fields",
            "source-layer": "fields",
            type: "symbol",
            filter: ["==", ["get", "is_organic"], true],
            paint: {
              "text-color": "#16a34a",
              "text-halo-color": "#ffffff",
              "text-halo-width": 1,
            },
            layout: {
              "text-field": "🌿",
              "text-size": 16,
              "text-allow-overlap": false,
              "text-ignore-placement": false,
              visibility: layerVisibility.fields ? "visible" : "none",
            },
          });
        } else {
          map.setLayoutProperty("organic-symbols", "visibility", layerVisibility.fields ? "visible" : "none");
        }
      } else {
        // Hide organic symbols for other modes
        if (map.getLayer("organic-symbols")) {
          map.setLayoutProperty("organic-symbols", "visibility", "none");
        }
      }
    }
  }, [filterState, layerVisibility.fields, generateFieldsPaint]);

  // Handle hover events
  const onHover = useCallback((event: MapLayerMouseEvent) => {
    const feature = event.features && event.features[0];
    if (feature) {
      const layerName = getLayerDisplayName(feature.layer.id);

      setHoverInfo({
        x: event.point.x,
        y: event.point.y,
        properties: feature.properties || {},
        layerName,
        visualizationMode: filterState.visualizationMode,
        colorUnit: filterState.colorUnit,
      });
    } else {
      setHoverInfo(null);
    }
  }, [filterState.visualizationMode, filterState.colorUnit]);

  // Handle click events for field selection
  const onClick = useCallback((event: MapLayerMouseEvent) => {
    const feature = event.features && event.features[0];
    if (feature && feature.layer.id.startsWith("fields-")) {
      onFieldSelect(feature.properties as FieldAnalysisData);
    }
  }, [onFieldSelect]);

  // Get display name for layer
  const getLayerDisplayName = (layerId: string): string => {
    if (layerId.startsWith("fields-")) return "Landbrugsmark";
    if (layerId.startsWith("bnbo-")) return "BNBO Område";
    if (layerId.startsWith("wetlands-")) return "Vådområde";
    if (layerId.startsWith("water-projects-")) return "Vandprojekt";
    return "Ukendt lag";
  };

  if (error) {
    return (
      <div className="flex items-center justify-center h-full bg-red-50">
        <div className="text-center">
          <div className="text-red-600 text-xl mb-2">⚠️ Fejl</div>
          <div className="text-gray-700">{error}</div>
        </div>
      </div>
    );
  }

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full bg-gray-50">
        <div className="text-center">
          <div className="text-lg font-medium text-gray-900 mb-2">Indlæser kortdata...</div>
          <div className="w-8 h-8 border-4 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto"></div>
        </div>
      </div>
    );
  }

  // Interactive layer IDs for hover/click events
  const interactiveLayerIds = [
    "fields-fill",
    "bnbo-fill",
    "wetlands-fill",
    "water-projects-fill"
  ];

  return (
    <div className="relative w-full h-full">
      <Map
        ref={mapRef}
        initialViewState={{
          longitude: 9.501785,
          latitude: 56.26392,
          zoom: 7,
        }}
        style={{ width: "100%", height: "100%" }}
        mapStyle="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
        interactiveLayerIds={interactiveLayerIds}
        onLoad={onMapLoad}
        onMouseMove={onHover}
        onMouseLeave={() => setHoverInfo(null)}
        onClick={onClick}
        cursor="default"
      >
        <NavigationControl position="top-right" />

        {/* PMTiles sources and layers are added programmatically in onMapLoad */}
      </Map>

      {hoverInfo && <MapTooltip {...hoverInfo} />}
    </div>
  );
}

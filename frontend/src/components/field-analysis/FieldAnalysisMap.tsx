"use client";

import React, { useEffect, useRef, useState, useCallback } from "react";
import Map, {
  MapLayerMouseEvent,
  NavigationControl,
} from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { LayerVisibility, FilterState, FieldAnalysisData } from "./FieldAnalysisVisualization";
import { PMTiles, Protocol } from "pmtiles";

// Type for MapLibre map instance
interface MapInstance {
  getSource: (id: string) => unknown;
  getLayer: (id: string) => unknown;
  addLayer: (layer: unknown) => void;
  setLayoutProperty: (id: string, prop: string, value: string) => void;
  addSource: (id: string, source: unknown) => void;
}

interface FieldAnalysisMapProps {
  pmtilesUrls: {
    fields: string;
    bnbo: string;
    wetlands: string;
    waterProjects: string;
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
}

function MapTooltip({ x, y, properties, layerName }: TooltipInfo) {
  const formatValue = (value: unknown): string => {
    if (typeof value === "number") {
      return value.toLocaleString("da-DK", { maximumFractionDigits: 2 });
    }
    return String(value);
  };

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
        {Object.entries(properties)
          .filter(([key]) => !key.startsWith("_") && key !== "geometry")
          .slice(0, 6) // Limit to 6 properties to avoid huge tooltips
          .map(([key, value]) => (
            <div key={key} className="flex justify-between">
              <span className="text-gray-600 capitalize">
                {key.replace(/_/g, " ")}:
              </span>
              <span className="font-medium text-gray-900 ml-2">
                {formatValue(value)}
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
        // Register PMTiles protocol with MapLibre
        const protocol = new Protocol();

        // Add PMTiles sources to the protocol
        Object.entries(pmtilesUrls).forEach(([, url]) => {
          if (url) {
            const pmtiles = new PMTiles(url);
            protocol.add(pmtiles);
          }
        });

        setIsLoading(false);
      } catch (err) {
        console.error("Failed to initialize PMTiles:", err);
        setError("Kunne ikke indlæse kortdata");
        setIsLoading(false);
      }
    };

    initializePMTiles();
  }, [pmtilesUrls]);

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

  // Add field analysis layers
  const addFieldsLayers = useCallback((map: MapInstance) => {
    if (map.getSource("fields") && !map.getLayer("fields-fill")) {
      // Main fields layer
      map.addLayer({
        id: "fields-fill",
        source: "fields",
        "source-layer": "fields",
        type: "fill",
        paint: {
          "fill-color": [
            "case",
            ["==", ["get", "is_organic"], true],
            "#10B981", // Green for organic
            [
              "interpolate",
              ["linear"],
              ["coalesce", ["get", "total_pesticide_belastning"], 0],
              0, "#F3F4F6",   // Light gray for no pesticides
              10, "#FEF3C7",  // Light yellow
              50, "#F59E0B",  // Orange
              100, "#DC2626"  // Red for high pesticide load
            ]
          ],
          "fill-opacity": 0.7,
        },
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
    }
  }, [layerVisibility.fields]);

  // Add BNBO layers
  const addBNBOLayers = useCallback((map: MapInstance) => {
    if (map.getSource("bnbo") && !map.getLayer("bnbo-fill")) {
      map.addLayer({
        id: "bnbo-fill",
        source: "bnbo",
        "source-layer": "bnbo",
        type: "fill",
        paint: {
          "fill-color": [
            "case",
            ["==", ["get", "status_category"], "Completed"],
            "#10B981", // Green for completed
            "#F59E0B"  // Orange for action required
          ],
          "fill-opacity": 0.8,
        },
        layout: {
          visibility: layerVisibility.bnbo ? "visible" : "none",
        },
      });

      map.addLayer({
        id: "bnbo-outline",
        source: "bnbo",
        "source-layer": "bnbo",
        type: "line",
        paint: {
          "line-color": "#065F46",
          "line-width": 1,
        },
        layout: {
          visibility: layerVisibility.bnbo ? "visible" : "none",
        },
      });
    }
  }, [layerVisibility.bnbo]);

  // Add wetlands layers
  const addWetlandsLayers = useCallback((map: MapInstance) => {
    if (map.getSource("wetlands") && !map.getLayer("wetlands-fill")) {
      map.addLayer({
        id: "wetlands-fill",
        source: "wetlands",
        "source-layer": "wetlands",
        type: "fill",
        paint: {
          "fill-color": [
            "case",
            ["==", ["get", "toerv_pct"], ">12"],
            "#1E40AF", // Dark blue for high moisture
            "#3B82F6"  // Light blue for medium moisture
          ],
          "fill-opacity": 0.6,
        },
        layout: {
          visibility: layerVisibility.wetlands ? "visible" : "none",
        },
      });

      map.addLayer({
        id: "wetlands-outline",
        source: "wetlands",
        "source-layer": "wetlands",
        type: "line",
        paint: {
          "line-color": "#1E3A8A",
          "line-width": 0.5,
        },
        layout: {
          visibility: layerVisibility.wetlands ? "visible" : "none",
        },
      });
    }
  }, [layerVisibility.wetlands]);

  // Add water projects layers
  const addWaterProjectsLayers = useCallback((map: MapInstance) => {
    if (map.getSource("water_projects") && !map.getLayer("water-projects-fill")) {
      map.addLayer({
        id: "water-projects-fill",
        source: "water_projects",
        "source-layer": "water_projects",
        type: "fill",
        paint: {
          "fill-color": "#14B8A6",
          "fill-opacity": 0.7,
        },
        layout: {
          visibility: layerVisibility.waterProjects ? "visible" : "none",
        },
      });

      map.addLayer({
        id: "water-projects-outline",
        source: "water_projects",
        "source-layer": "water_projects",
        type: "line",
        paint: {
          "line-color": "#0F766E",
          "line-width": 1,
        },
        layout: {
          visibility: layerVisibility.waterProjects ? "visible" : "none",
        },
      });
    }
  }, [layerVisibility.waterProjects]);

  // Update layer visibility when props change
  useEffect(() => {
    if (!mapRef.current) return;

    const map = mapRef.current.getMap();

    // Update fields layers
    if (map.getLayer("fields-fill")) {
      map.setLayoutProperty("fields-fill", "visibility", layerVisibility.fields ? "visible" : "none");
      map.setLayoutProperty("fields-outline", "visibility", layerVisibility.fields ? "visible" : "none");
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
      map.setLayoutProperty("water-projects-fill", "visibility", layerVisibility.waterProjects ? "visible" : "none");
      map.setLayoutProperty("water-projects-outline", "visibility", layerVisibility.waterProjects ? "visible" : "none");
    }
  }, [layerVisibility]);

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
      });
    } else {
      setHoverInfo(null);
    }
  }, []);

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

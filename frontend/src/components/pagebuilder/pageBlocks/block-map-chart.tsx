"use client";

import * as React from "react";
import Map, { Layer, Source } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { MapChart } from "@/services/supabase/types";
import { VizColors } from "@/lib/utils";

const getLayerStyle = (style: string, index: number) => {
  // if style contains marker, return the default marker style
  if (style.includes("marker")) {
    return {
      circleRadius: 6,
      circleColor: "#FF0000",
      circleStrokeWidth: 2,
      circleStrokeColor: "#FFFFFF",
    };
  }

  switch (style) {
    case "building":
      return {
        fillColor: "#4a90e2",
        fillOpacity: 0.7,
        strokeColor: "#2171c7",
        strokeWidth: 2,
      };
    case "field_detailed":
    case "field":
      return {
        fillColor: "#2A8B4E",
        fillOpacity: 0.4,
        strokeColor: "#2A8B4E",
        strokeWidth: 1,
      };
    default:
      return {
        fillColor: VizColors[index + 1],
        fillOpacity: 0.7,
        strokeColor: VizColors[index + 1],
        strokeWidth: 2,
        circleRadius: 6,
        circleColor: "#FF0000",
        circleStrokeWidth: 2,
        circleStrokeColor: "#FFFFFF",
      };
  }
};

export function BlockMapChart({ chart }: { chart: MapChart }) {
  const { center, zoom, layers } = chart.data;

  return (
    <div className="rounded overflow-hidden">
      <Map
        initialViewState={{
          longitude: center[0],
          latitude: center[1],
          zoom: zoom,
        }}
        style={{ width: "100%", height: 600 }}
        mapStyle="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
      >
        {layers.map((layer, index) => {
          const style = getLayerStyle(layer.style, index);

          return (
            <Source
              key={`${layer.name}-${index}`}
              type="geojson"
              data={layer.data as GeoJSON.FeatureCollection}
            >
              {layer.style.includes("marker") ? (
                <Layer
                  id={layer.name}
                  type="circle"
                  paint={{
                    "circle-radius": style.circleRadius,
                    "circle-color": style.circleColor,
                    "circle-stroke-width": style.circleStrokeWidth,
                    "circle-stroke-color": style.circleStrokeColor,
                  }}
                />
              ) : (
                <Layer
                  id={layer.name}
                  type="fill"
                  paint={{
                    "fill-color": style.fillColor,
                    "fill-opacity": style.fillOpacity,
                    "fill-outline-color": style.strokeColor,
                  }}
                />
              )}
            </Source>
          );
        })}
      </Map>

      {/* Custom legends */}
      <div className="flex flex-wrap gap-4 mt-2">
        {layers.map((layer, index) => {
          const style = getLayerStyle(layer.style, index);
          return (
            <button
              key={`${layer.name}-${index}`}
              className="flex items-center gap-2 rounded-md hover:bg-gray-50 transition-colors"
            >
              <div
                className="size-4 rounded-full"
                style={{
                  backgroundColor:
                    style.fillColor || style.strokeColor || style.circleColor,
                }}
              />
              <span className="text-xs font-medium">{layer.name}</span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

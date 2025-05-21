"use client";

import * as React from "react";
import Map from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { MapChart } from "@/services/supabase/types";

export function BlockMapChart({ chart }: { chart: MapChart }) {
  return (
    <div>
      <h2 className="text-2xl font-bold">{chart.title}</h2>
      <Map
        initialViewState={{
          longitude: -122.4,
          latitude: 37.8,
          zoom: 14,
        }}
        style={{ width: 600, height: 400 }}
        mapStyle="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
      />
    </div>
  );
}

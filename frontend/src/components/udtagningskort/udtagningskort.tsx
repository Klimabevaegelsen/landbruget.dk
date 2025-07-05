"use client";

import Map, { Source, Layer, NavigationControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useState } from "react";

const pageSize = 1000;

async function fetchAllRows() {
  let allRows = [];
  let page = 0;
  let totalRows = null;

  while (true) {
    console.log('Range:', `${page * pageSize}-${(page + 1) * pageSize - 1}`,);
    const response = await fetch(
      `${process.env.NEXT_PUBLIC_API_URL}/rest/v1/nature_report_area`,
      {
        headers: {
          Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
          Range: `${page * pageSize}-${(page + 1) * pageSize - 1}`,
          Prefer: "count=exact",
        },
      }
    );
    const data = await response.json();
    allRows = allRows.concat(data);

    // Get total rows from Content-Range header (e.g., "0-999/300000")
    if (totalRows === null) {
      const contentRange = response.headers.get("content-range");
      if (contentRange) {
        totalRows = parseInt(contentRange.split("/")[1], 10);
      }
    }

    // Stop if we've fetched all rows
    if (allRows.length >= totalRows || data.length < pageSize) {
      break;
    }
    page += 1;
  }

  return allRows;
}

export function UdtagningsKort() {
    const [geojson, setGeojson] = useState<any[] | null>(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        setLoading(true);
        fetchAllRows()
            .then(data => {
                setGeojson(data);
                setLoading(false);
            })
            .catch(() => setLoading(false));
    }, []);
    console.log('geojson', geojson);
    if (loading) return <div>Loading...</div>;
    if (!geojson || !Array.isArray(geojson)) return <div>No data found.</div>;

    return (
        <div style={{height: "80vh" }}>
            <Map
                initialViewState={{
                    latitude: 56.3,
                    longitude: 11.5,
                    zoom: 6,
                }}
                mapStyle="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
                >
                <NavigationControl position="top-right" />

                {geojson.length > 0 && (
                    <Source
                        type="geojson"
                        data={{
                            type: "FeatureCollection",
                            features: geojson.map((item: any) => ({
                                type: "Feature",
                                geometry: item.geom,
                                properties: {
                                    id: item.id,
                                    report_category: item.report_category,
                                },
                            })),
                        }}
                    >
                        <Layer
                            id="big-data-layer"
                            type="fill"
                            paint={{
                                "fill-color": "#088",
                                "fill-opacity": 0.5,
                            }}
                        />
                    </Source>
                )}
                {/* Add more layers, popups, legends, etc. as needed */}
            </Map>
        </div>
    );
}

export function CriteriaSelector() {
    return (
        <div style={({height: "80vh"})}>
            <h2 className="text-xl font bold mb-4">Kriterier</h2>
            Naturen giver værdi på mange måder. 
            Nedenfor kan du vælge at fokusere på en eller flere af følgende kriterier for udtagning af landbrugsjord:
            <form>
                <div><input type="checkbox" name="biodiversitet" id="biodiversitet" />
                <label htmlFor="biodiversitet">Biodiversitet</label></div>
                <div><input type="checkbox" name="klima" id="klima" />
                <label htmlFor="klima">Klima</label></div>
                <div><input type="checkbox" name="nitrogen" id="nitrogen" />
                <label htmlFor="nitrogen">Nitrogenudvaskning</label></div>
                <div><input type="checkbox" name="rekreation" id="rekreation" />
                <label htmlFor="rekreation">Rekreation</label></div>
                <button type="submit" className="bg-blue-500 text-white px-4 py-2 rounded-md">Opdater</button>
            </form>
        </div>
    );
}
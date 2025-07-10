"use client";

import Map, { Source, Layer, NavigationControl } from "react-map-gl/maplibre";
import "maplibre-gl/dist/maplibre-gl.css";
import { useEffect, useState } from "react";
import { report } from "process";

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

export function NaturKort({criteriaValues}) {
  const [geojson, setGeojson] = useState<any[] | null>(null);
  const [report_categories, setCategories] = useState<any[] | null>(null);
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

  useEffect(() => {
      fetch(
        `${process.env.NEXT_PUBLIC_API_URL}/rest/v1/nature_report_category`,
        {
          headers: {
            Authorization: `Bearer ${process.env.NEXT_PUBLIC_API_KEY}`,
          },
        }
      )
        .then(response => response.json())
        .then(data => setCategories(data)

      )
      
    }, []);
    if (loading) return <div>Loading...</div>;
    if (!geojson || !Array.isArray(geojson)) return <div>No data found.</div>;



  console.log('MAP', criteriaValues)
  // var priority_scores = new Array(geojson.length).fill(0.5)
  if (report_categories) {
    const category_scores = report_categories.reduce((acc, row) => {
      const score = (row['score_biodiversity'] * criteriaValues['biodiversitet'] +
                    row['score_climate'] * criteriaValues['klima'] +
                    row['score_nitrogen'] * criteriaValues['nitrogen'] +
                    row['score_recreation'] * criteriaValues['rekreation']) / (
                      criteriaValues['biodiversitet'] +
                      criteriaValues['klima'] +
                      criteriaValues['nitrogen'] +
                      criteriaValues['rekreation']
                    );
      acc[row['id']] = score;
      return acc;
    }, {});
    console.log('category_scores', category_scores);
    console.log(geojson)

    // const priority_scores = geojson.map(row => {
    //   const p_score = category_scores[row['nature_report_category']];

    //   return p_score;
    // }
    geojson.map(row => {
      row["priority_score"] = category_scores[row['nature_report_category']];
    }
 
  );
  // console.log(priority_scores)
  console.log(geojson)
  }

  
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
                                    nature_report_category: item.nature_report_category,
                                    priority_score: item.priority_score, 
                                },
                            })),
                        }}
                    >
                        <Layer
                            id="big-data-layer"
                            type="fill"
                            paint={{
                                // "fill-color": "#088",
                                "fill-color": [
                                  "interpolate",
                                  ['linear'],
                                  ["get", "priority_score"],
                                  0, "#ff0000",
                                  1, "#00ff00",
                                ],
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

export function CriteriaSelector({criteriaValues, handleCriteriaChange}) {    
    return (
        <div style={({height: "80vh"})}>
            <h2 className="text-xl font bold mb-4">Kriterier</h2>
            Naturen giver værdi på mange måder. 
            Nedenfor kan du vælge at fokusere på en eller flere af følgende kriterier for udtagning af landbrugsjord:
            <form>
                <CriteriaSlider
                    id="biodiversitet"
                    name="Biodiversitet"
                    value={criteriaValues.biodiversitet}
                    onChange={val => handleCriteriaChange("biodiversitet", val)}
                />
                <CriteriaSlider
                    id="klima"
                    name="Klima"
                    value={criteriaValues.klima}
                    onChange={val => handleCriteriaChange("klima", val)}
                />
                <CriteriaSlider
                    id="nitrogen"
                    name="Nitrogenudvaskning"
                    value={criteriaValues.nitrogen}
                    onChange={val => handleCriteriaChange("nitrogen", val)}
                />
                <CriteriaSlider
                    id="rekreation"
                    name="Rekreativ værdi"
                    value={criteriaValues.rekreation}
                    onChange={val => handleCriteriaChange("rekreation", val)}
                />
                <button type="submit" className="bg-blue-500 text-white px-4 py-2 rounded-md">Opdater</button>
            </form>
        </div>
    );
}

function CriteriaSlider({id, name, value, onChange}) {
    // const [value, setValue] = useState(1); // initial value
  
    return (
      <div className="p-3">
        <label htmlFor={id} className="block mb-2 text-sm font-medium text-gray-900 dark:text-white">{name}: {value}</label>
        <input
          id={id}
          type="range"
          min="0"
          max="1"
          step="0.1"
          value={value}
          onChange={e => onChange(Number(e.target.value))}
          className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer dark:bg-gray-700"
        />
      </div>
    );
  }
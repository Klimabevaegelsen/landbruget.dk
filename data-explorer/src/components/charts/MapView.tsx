'use client';

import { useEffect, useRef, useMemo } from 'react';
import maplibregl from 'maplibre-gl';
import 'maplibre-gl/dist/maplibre-gl.css';

interface MapViewProps {
  data: Record<string, unknown>[];
  latKey: string;
  lngKey: string;
  labelKey?: string | null;
  popupKeys?: string[] | null;
  title?: string | null;
  height?: number | null;
}

export function MapView({
  data,
  latKey,
  lngKey,
  labelKey,
  popupKeys,
  title,
  height = 400,
}: MapViewProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);

  const validPoints = useMemo(() => {
    return data.filter((row) => {
      const lat = Number(row[latKey]);
      const lng = Number(row[lngKey]);
      return (
        !isNaN(lat) &&
        !isNaN(lng) &&
        lat >= -90 &&
        lat <= 90 &&
        lng >= -180 &&
        lng <= 180
      );
    });
  }, [data, latKey, lngKey]);

  useEffect(() => {
    if (!containerRef.current || validPoints.length === 0) return;

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: 'https://basemaps.cartocdn.com/gl/positron-gl-style/style.json',
      center: [10.4, 56.0], // Denmark center
      zoom: 6,
    });

    map.addControl(new maplibregl.NavigationControl(), 'top-right');
    mapRef.current = map;

    map.on('load', () => {
      // Build GeoJSON
      const geojson: GeoJSON.FeatureCollection = {
        type: 'FeatureCollection',
        features: validPoints.map((row, i) => ({
          type: 'Feature',
          id: i,
          geometry: {
            type: 'Point',
            coordinates: [Number(row[lngKey]), Number(row[latKey])],
          },
          properties: Object.fromEntries(
            Object.entries(row).map(([k, v]) => [k, String(v ?? '')])
          ),
        })),
      };

      map.addSource('points', {
        type: 'geojson',
        data: geojson,
        cluster: validPoints.length > 200,
        clusterMaxZoom: 14,
        clusterRadius: 50,
      });

      // Cluster circles
      if (validPoints.length > 200) {
        map.addLayer({
          id: 'clusters',
          type: 'circle',
          source: 'points',
          filter: ['has', 'point_count'],
          paint: {
            'circle-color': [
              'step',
              ['get', 'point_count'],
              '#2d5a3d',
              50,
              '#4a7c5e',
              200,
              '#7aab8e',
            ],
            'circle-radius': [
              'step',
              ['get', 'point_count'],
              18,
              50,
              24,
              200,
              32,
            ],
          },
        });

        map.addLayer({
          id: 'cluster-count',
          type: 'symbol',
          source: 'points',
          filter: ['has', 'point_count'],
          layout: {
            'text-field': '{point_count_abbreviated}',
            'text-size': 12,
          },
          paint: {
            'text-color': '#ffffff',
          },
        });
      }

      // Individual points
      map.addLayer({
        id: 'unclustered-point',
        type: 'circle',
        source: 'points',
        filter:
          validPoints.length > 200 ? ['!', ['has', 'point_count']] : ['all'],
        paint: {
          'circle-color': '#2d5a3d',
          'circle-radius': 6,
          'circle-stroke-width': 2,
          'circle-stroke-color': '#fdfaf6',
        },
      });

      // Popup on click
      map.on('click', 'unclustered-point', (e) => {
        if (!e.features?.[0]) return;
        const props = e.features[0].properties;
        if (!props) return;

        const keys = popupKeys ?? Object.keys(props).slice(0, 6);
        const html = keys
          .map((k) => `<b>${k}:</b> ${props[k] ?? ''}`)
          .join('<br/>');

        new maplibregl.Popup({ maxWidth: '300px' })
          .setLngLat(e.lngLat)
          .setHTML(html)
          .addTo(map);
      });

      // Zoom to expand clusters
      if (validPoints.length > 200) {
        map.on('click', 'clusters', (e) => {
          const features = map.queryRenderedFeatures(e.point, {
            layers: ['clusters'],
          });
          if (!features[0]) return;
          const clusterId = features[0].properties?.cluster_id;
          const source = map.getSource('points') as maplibregl.GeoJSONSource;
          source.getClusterExpansionZoom(clusterId).then((zoom) => {
            map.easeTo({
              center: (features[0].geometry as GeoJSON.Point).coordinates as [
                number,
                number,
              ],
              zoom,
            });
          });
        });
      }

      // Fit bounds
      if (validPoints.length > 0) {
        const bounds = new maplibregl.LngLatBounds();
        validPoints.forEach((row) => {
          bounds.extend([Number(row[lngKey]), Number(row[latKey])]);
        });
        map.fitBounds(bounds, { padding: 50, maxZoom: 14 });
      }

      // Cursor
      map.on('mouseenter', 'unclustered-point', () => {
        map.getCanvas().style.cursor = 'pointer';
      });
      map.on('mouseleave', 'unclustered-point', () => {
        map.getCanvas().style.cursor = '';
      });
    });

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, [validPoints, latKey, lngKey, labelKey, popupKeys]);

  if (validPoints.length === 0) {
    return (
      <div className="bg-muted/30 flex items-center justify-center rounded-lg border p-8">
        <p className="text-muted-foreground text-sm">
          No valid geographic coordinates found in the data.
        </p>
      </div>
    );
  }

  return (
    <div>
      {title && (
        <h3 className="text-foreground mb-3 text-sm font-semibold">{title}</h3>
      )}
      <div
        ref={containerRef}
        className="w-full overflow-hidden rounded-lg border"
        style={{ height: height ?? 400 }}
      />
      <p className="text-muted-foreground mt-1 text-xs">
        {validPoints.length.toLocaleString('da-DK')} points
      </p>
    </div>
  );
}

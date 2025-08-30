'use client';

import * as React from 'react';
import Map, {
  Layer,
  Source,
  MapLayerMouseEvent,
  NavigationControl,
} from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { MapChart, GeoJSONLayer } from '@/services/supabase/types';
import { VizColors } from '@/lib/utils';
import { MapErrorBoundary } from './MapErrorBoundary';
import { shouldShowPlaceholder } from './chart-utils';
import { PlaceholderChart } from './placeholder-chart';
import { NoDataPlaceholder } from './no-data-placeholder';
import { useCategoryDataContext } from './CategoryDataContext';

const getLayerStyle = (style: string | undefined, index: number) => {
  // if style contains marker, return the default marker style
  if (style && style.includes('marker')) {
    return {
      circleRadius: 6,
      circleColor: '#FF0000',
      circleStrokeWidth: 2,
      circleStrokeColor: '#FFFFFF',
    };
  }

  switch (style) {
    case 'building':
      return {
        fillColor: '#4a90e2',
        fillOpacity: 0.7,
        strokeColor: '#2171c7',
        strokeWidth: 2,
      };
    case 'field_detailed':
    case 'field':
      return {
        fillColor: '#2A8B4E',
        fillOpacity: 0.4,
        strokeColor: '#2A8B4E',
        strokeWidth: 1,
      };
    default:
      return {
        fillColor: VizColors[index + 1],
        fillOpacity: 0.7,
        strokeColor: VizColors[index + 1],
        strokeWidth: 2,
        circleRadius: 6,
        circleColor: '#FF0000',
        circleStrokeWidth: 2,
        circleStrokeColor: '#FFFFFF',
      };
  }
};

interface TooltipProps {
  x: number;
  y: number;
  properties: Record<string, string | number | boolean>;
  layerName: string;
}

function Tooltip({ x, y, properties, layerName }: TooltipProps) {
  return (
    <div
      className="absolute z-50 rounded-lg border border-gray-200 bg-white p-4 shadow-md"
      style={{
        left: x,
        top: y,
        transform: 'translate(-50%, -100%)',
        marginTop: -10,
      }}
    >
      <p className="text-base font-semibold">{layerName}</p>
      {Object.entries(properties).map(([key, value]) => (
        <p key={key} className="mt-1 text-sm font-medium">
          <span className="font-medium">{key}:</span>{' '}
          {typeof value === 'number'
            ? value.toLocaleString('da-DK')
            : String(value)}
        </p>
      ))}
    </div>
  );
}

// https://geojson.io
function BlockMapChartInner({ chart }: { chart: MapChart }) {
  const { isInCategoryWithData } = useCategoryDataContext();
  const [hoverInfo, setHoverInfo] = React.useState<{
    x: number;
    y: number;
    properties: Record<string, string | number | boolean>;
    layerName: string;
  } | null>(null);

  // Extract and validate chart data
  const chartData = React.useMemo(():
    | { isValid: false; error: string }
    | {
        isValid: true;
        center: [number, number];
        zoom: number;
        validLayers: GeoJSONLayer[];
      } => {
    // Validate chart data
    if (!chart?.data) {
      console.warn('BlockMapChart: Missing chart data');
      return { isValid: false, error: 'No map data available' };
    }

    const { center, zoom, layers } = chart.data;

    if (!center || center.length !== 2 || !Array.isArray(layers)) {
      console.warn('BlockMapChart: Invalid chart data structure', {
        center,
        layers: layers?.length,
      });
      return { isValid: false, error: 'Invalid map configuration' };
    }

    // Filter out layers with invalid data
    const validLayers = layers.filter((layer, index) => {
      if (!layer?.data) {
        console.warn(
          `BlockMapChart: Layer ${index} (${layer?.name || 'unnamed'}) has no data`,
          layer
        );
        return false;
      }

      // Basic GeoJSON validation
      const geojsonData = layer.data as GeoJSON.FeatureCollection;
      if (
        !geojsonData.type ||
        geojsonData.type !== 'FeatureCollection' ||
        !Array.isArray(geojsonData.features)
      ) {
        console.warn(
          `BlockMapChart: Layer ${index} (${layer?.name || 'unnamed'}) has invalid GeoJSON data`,
          geojsonData
        );
        return false;
      }

      // Log successful validation
      console.log(
        `BlockMapChart: Layer ${index} (${layer.name}) validated successfully with ${geojsonData.features.length} features`
      );
      return true;
    });

    if (validLayers.length === 0) {
      console.warn('BlockMapChart: No valid layers found');
      return { isValid: false, error: 'No valid map layers available' };
    }

    return {
      isValid: true,
      center: center as [number, number],
      zoom,
      validLayers,
    };
  }, [chart]);

  const onHover = React.useCallback(
    (event: MapLayerMouseEvent) => {
      const feature = event.features && event.features[0];
      if (feature && chartData.isValid) {
        // Find the layer name from the layer ID
        const layerIndex = parseInt(feature.layer.id.split('-')[1]);
        const layerName =
          chartData.validLayers[layerIndex]?.name || 'Unknown Layer';

        setHoverInfo({
          x: event.point.x,
          y: event.point.y,
          properties: feature.properties,
          layerName,
        });
      } else {
        setHoverInfo(null);
      }
    },
    [chartData]
  );

  // Return error state if validation failed
  if (!chartData.isValid) {
    if (!isInCategoryWithData) {
      return <NoDataPlaceholder />;
    } else {
      return (
        <div className="py-8 text-center text-gray-500">
          Ingen kortdata tilgængelig
        </div>
      );
    }
  }

  return (
    <div className="relative overflow-hidden rounded">
      <Map
        initialViewState={{
          longitude: chartData.center[0],
          latitude: chartData.center[1],
          zoom: chartData.zoom,
        }}
        style={{ width: '100%', height: 600 }}
        mapStyle="https://basemaps.cartocdn.com/gl/voyager-gl-style/style.json"
        interactiveLayerIds={chartData.validLayers.map(
          (_, index) => `layer-${index}`
        )}
        onMouseMove={onHover}
        onMouseLeave={() => setHoverInfo(null)}
        scrollZoom={false}
      >
        <NavigationControl position="top-right" />
        {chartData.validLayers.map((layer, index) => {
          const style = getLayerStyle(layer.style, index);

          return (
            <Source
              key={`${layer.name}-${index}`}
              id={`source-${layer.name}-${index}`}
              type="geojson"
              data={layer.data as GeoJSON.FeatureCollection}
            >
              {layer.style && layer.style.includes('marker') ? (
                <Layer
                  id={`layer-${index}`}
                  type="circle"
                  paint={{
                    'circle-radius': style.circleRadius,
                    'circle-color': style.circleColor,
                    'circle-stroke-width': style.circleStrokeWidth,
                    'circle-stroke-color': style.circleStrokeColor,
                  }}
                />
              ) : (
                <Layer
                  id={`layer-${index}`}
                  type="fill"
                  paint={{
                    'fill-color': style.fillColor,
                    'fill-opacity': style.fillOpacity,
                    'fill-outline-color': style.strokeColor,
                  }}
                />
              )}
            </Source>
          );
        })}
      </Map>

      {hoverInfo && <Tooltip {...hoverInfo} />}

      {/* Custom legends */}
      <div className="mt-2 flex flex-wrap gap-4">
        {chartData.validLayers.map((layer, index) => {
          const style = getLayerStyle(layer.style, index);
          return (
            <button
              key={`${layer.name}-${index}`}
              className="flex items-center gap-2 rounded-md transition-colors hover:bg-gray-50"
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

export function BlockMapChart({ chart }: { chart: MapChart }) {
  // Check if this chart should show a placeholder
  const placeholderDataType = shouldShowPlaceholder(chart._key);
  if (placeholderDataType) {
    return <PlaceholderChart dataType={placeholderDataType} />;
  }

  return (
    <MapErrorBoundary>
      <BlockMapChartInner chart={chart} />
    </MapErrorBoundary>
  );
}

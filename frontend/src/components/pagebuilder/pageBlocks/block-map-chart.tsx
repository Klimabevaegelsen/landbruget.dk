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
  // Handle specific marker types with distinct colors
  if (style === 'production_site_marker') {
    return {
      circleRadius: 8,
      circleColor: '#FF6B35', // Orange for production sites
      circleStrokeWidth: 2,
      circleStrokeColor: '#FFFFFF',
    };
  }

  if (style === 'hq_marker') {
    return {
      circleRadius: 8,
      circleColor: '#4A90E2', // Blue for company headquarters/addresses
      circleStrokeWidth: 2,
      circleStrokeColor: '#FFFFFF',
    };
  }

  // Generic fallback for other marker styles
  if (style && style.includes('marker')) {
    return {
      circleRadius: 6,
      circleColor: '#FF0000', // Red as fallback
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
  const formatValue = (value: unknown, unit?: string): string => {
    if (typeof value === 'number') {
      const formatted = value.toLocaleString('da-DK', {
        maximumFractionDigits: 2,
      });
      return unit ? `${formatted} ${unit}` : formatted;
    }
    return String(value);
  };

  // Danish translations for common field names
  const fieldTranslations: Record<string, string> = {
    // Basic field info
    crop_name: 'Afgrøde',
    crop_code: 'Afgrødekode',
    area_hectares: 'Areal',
    area_ha: 'Areal',
    area: 'Areal',
    is_organic: 'Økologisk',
    organic: 'Økologisk',

    // Location info
    kommune: 'Kommune',
    municipality: 'Kommune',
    region: 'Region',
    address: 'Adresse',
    postal_code: 'Postnummer',
    city: 'By',

    // Company info
    company_name: 'Virksomhedsnavn',
    cvr_number: 'CVR-nummer',
    cvr: 'CVR-nummer',
    p_number: 'P-nummer',

    // Field identifiers
    field_id: 'Mark ID',
    block_id: 'Blok ID',
    field_uuid: 'Mark UUID',

    // Production data
    production_site_name: 'Produktionssted',
    site_name: 'Stedsnavn',
    site_type: 'Stedtype',

    // Building data
    building_type: 'Bygningstype',
    building_usage: 'Bygningsanvendelse',
    bbr_usage_code: 'BBR anvendelseskode',
    category_group: 'Kategori',

    // Environmental data
    status_category: 'Status',
    environmental_zone: 'Miljøzone',
    natura2000: 'Natura 2000',

    // Dates
    year: 'År',
    created_at: 'Oprettet',
    updated_at: 'Opdateret',
  };

  // Fields to exclude from display (technical/internal fields)
  const excludedFields = new Set([
    'id',
    'uuid',
    'geom',
    'geometry',
    'wkt',
    'created_at',
    'updated_at',
    'version',
    'source',
    'processed_at',
    'import_id',
    'raw_data',
    'metadata',
    'internal_id',
    'fid',
    'ogc_fid',
    'gml_id',
    'objectid',
  ]);

  // Get relevant data with proper formatting and filtering
  const getRelevantData = () => {
    const data: Array<{ label: string; value: unknown; unit?: string }> = [];

    // Process properties in a meaningful order
    const orderedKeys = Object.keys(properties).sort((a, b) => {
      // Prioritize important fields first
      const importantFields = [
        'crop_name',
        'area_hectares',
        'company_name',
        'address',
        'kommune',
      ];
      const aIndex = importantFields.indexOf(a);
      const bIndex = importantFields.indexOf(b);

      if (aIndex !== -1 && bIndex !== -1) return aIndex - bIndex;
      if (aIndex !== -1) return -1;
      if (bIndex !== -1) return 1;
      return a.localeCompare(b);
    });

    for (const key of orderedKeys) {
      // Skip excluded technical fields
      if (excludedFields.has(key.toLowerCase())) continue;

      const value = properties[key];

      // Skip null, undefined, or empty string values
      if (value === null || value === undefined || value === '') continue;

      // Get Danish label or use cleaned up key
      const label =
        fieldTranslations[key] ||
        key.replace(/_/g, ' ').replace(/\b\w/g, (l) => l.toUpperCase());

      // Determine unit based on field name
      let unit: string | undefined;
      if (key.includes('area') || key.includes('hectare')) {
        unit = 'ha';
      } else if (key.includes('distance') || key.includes('radius')) {
        unit = 'm';
      }

      // Special formatting for specific fields
      if (key === 'is_organic' || key === 'organic') {
        data.push({
          label,
          value: value ? 'Ja' : 'Nej',
        });
      } else if (key === 'status_category') {
        // Format status categories nicely
        let statusValue = String(value);
        if (statusValue === 'Action Required')
          statusValue = 'Handling påkrævet';
        else if (statusValue === 'Completed') statusValue = 'Gennemført';
        data.push({ label, value: statusValue });
      } else if (key === 'category_group') {
        // Format category groups with Danish labels
        const categoryLabels: Record<string, string> = {
          residential: 'Bolig',
          agricultural: 'Landbrug',
          publicServices: 'Offentlig service',
          commercial: 'Erhverv',
          industrial: 'Industri',
        };
        const categoryLabel = categoryLabels[String(value)] || String(value);
        data.push({ label, value: categoryLabel });
      } else {
        data.push({ label, value, unit });
      }
    }

    return data;
  };

  const relevantData = getRelevantData();

  // Don't show tooltip if no relevant data
  if (relevantData.length === 0) return null;

  return (
    <div
      className="absolute z-50 max-w-xs rounded-lg border border-gray-200 bg-white p-3 shadow-lg"
      style={{
        left: x,
        top: y,
        transform: 'translate(-50%, -100%)',
        marginTop: -10,
      }}
    >
      <p className="mb-2 text-sm font-semibold text-gray-900">{layerName}</p>
      <div className="space-y-1 text-xs">
        {relevantData.map(({ label, value, unit }, index) => (
          <div key={index} className="flex justify-between">
            <span className="text-gray-600">{label}:</span>
            <span className="ml-2 font-medium text-gray-900">
              {formatValue(value, unit)}
            </span>
          </div>
        ))}
      </div>
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

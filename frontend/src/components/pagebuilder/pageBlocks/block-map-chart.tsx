'use client';

import * as React from 'react';
import Map, {
  Layer,
  Source,
  MapLayerMouseEvent,
  NavigationControl,
} from '@vis.gl/react-maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { MapChart, GeoJSONLayer } from '@/services/supabase/types';
import { VizColors } from '@/lib/utils';
import { MapErrorBoundary } from './MapErrorBoundary';
import { DocumentationAccordion } from '@/components/chart';
import { shouldShowPlaceholder } from './chart-utils';
import { PlaceholderChart } from './placeholder-chart';
import { NoDataPlaceholder } from './no-data-placeholder';
import { useCategoryDataContext } from './CategoryDataContext';
import { MapCSVDownloadButton } from '@/components/chart';

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
      // Special formatting for different types of values
      let formatted: string;

      // For animal counts (dyr), use whole numbers with Danish thousand separators
      if (
        unit === 'dyr' ||
        (typeof unit === 'string' && unit.includes('dyr'))
      ) {
        formatted = Math.round(value).toLocaleString('da-DK');
      }
      // For areas, show appropriate precision
      else if (unit === 'ha' || unit === 'hektar') {
        formatted = value.toLocaleString('da-DK', {
          minimumFractionDigits: value < 1 ? 2 : 1,
          maximumFractionDigits: value < 1 ? 2 : 1,
        });
      }
      // For percentages, show 1 decimal place
      else if (
        unit === '%' ||
        (typeof unit === 'string' && unit.includes('%'))
      ) {
        formatted = value.toLocaleString('da-DK', {
          minimumFractionDigits: 1,
          maximumFractionDigits: 1,
        });
      }
      // For large numbers (likely counts), use whole numbers
      else if (value >= 1000) {
        formatted = Math.round(value).toLocaleString('da-DK');
      }
      // For small numbers, use appropriate precision
      else {
        formatted = value.toLocaleString('da-DK', {
          maximumFractionDigits: value < 1 ? 2 : 1,
        });
      }

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

    // Animal data
    chr: 'CHR-nummer',
    chr_number: 'CHR-nummer',
    capacity: 'Kapacitet',
    animal_count: 'Antal dyr',
    herd_size: 'Besætningsstørrelse',
    animal_type: 'Dyretype',
    species: 'Art',

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
          publicServices: 'Skole og daginstitutioner',
          commercial: 'Erhverv',
          industrial: 'Industri',
        };
        const categoryLabel = categoryLabels[String(value)] || String(value);
        data.push({ label, value: categoryLabel });
      } else if (key === 'capacity' && typeof value === 'number') {
        // Format animal capacity with proper unit
        data.push({ label, value, unit: unit || 'dyr' });
      } else if (key === 'animal_count' && typeof value === 'number') {
        // Format animal count with proper unit
        data.push({ label, value, unit: unit || 'dyr' });
      } else if (key === 'herd_size' && typeof value === 'number') {
        // Format herd size with proper unit
        data.push({ label, value, unit: unit || 'dyr' });
      } else {
        data.push({ label, value, unit });
      }
    }

    return data;
  };

  const relevantData = getRelevantData();

  // Don't show tooltip if no relevant data
  if (relevantData.length === 0) return null;

  const tooltipStyle = {
    left: x,
    top: y,
    transform: 'translate(-50%, -100%)',
    marginTop: -12,
  };

  return (
    <div
      className="border-border bg-background absolute z-50 max-w-sm rounded-xl border shadow-xl backdrop-blur-sm"
      style={tooltipStyle}
    >
      {/* Header with better typography */}
      <div className="border-border border-b px-4 py-3">
        <h3 className="text-foreground text-base leading-tight font-semibold">
          {layerName}
        </h3>
        {/* Show site name prominently if available */}
        {properties.site_name && (
          <p className="text-muted-foreground mt-1 text-sm font-medium">
            {properties.site_name}
          </p>
        )}
      </div>

      {/* Content with improved spacing and hierarchy */}
      <div className="px-4 py-3">
        <div className="space-y-2.5">
          {relevantData.map(({ label, value, unit }, index) => (
            <div
              key={index}
              className="flex items-baseline justify-between gap-3"
            >
              <span className="text-muted-foreground text-sm leading-tight font-medium">
                {label}:
              </span>
              <span className="text-foreground text-right text-sm leading-tight font-semibold">
                {formatValue(value, unit)}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

const mapContainerStyle = { width: '100%', height: 600 };

// https://geojson.io
function BlockMapChartInner({ chart }: { chart: MapChart }) {
  const { mapStyle } = useMapTheme();
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
      return { isValid: false, error: 'No map data available' };
    }

    const { center, zoom, layers } = chart.data;

    if (!center || center.length !== 2 || !Array.isArray(layers)) {
      return { isValid: false, error: 'Invalid map configuration' };
    }

    // Filter out layers with invalid data or no features
    const validLayers = layers.filter((layer) => {
      if (!layer?.data) {
        return false;
      }

      // Basic GeoJSON validation
      const geojsonData = layer.data as GeoJSON.FeatureCollection;
      if (
        !geojsonData.type ||
        geojsonData.type !== 'FeatureCollection' ||
        !Array.isArray(geojsonData.features)
      ) {
        return false;
      }

      // Check if layer has any features (data points)
      if (geojsonData.features.length === 0) {
        return false;
      }

      return true;
    });

    if (validLayers.length === 0) {
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
      // If we're in a category with data, render nothing instead of individual message
      return null;
    }
  }

  return (
    <div className="space-y-4">
      {/* Header with download button */}
      <div className="flex items-center justify-end">
        <MapCSVDownloadButton
          mapChart={chart}
          chartTitle={chart.title}
          chartKey={chart._key}
        />
      </div>

      <div className="relative overflow-hidden rounded">
        <Map
          initialViewState={{
            longitude: chartData.center[0],
            latitude: chartData.center[1],
            zoom: chartData.zoom,
          }}
          style={mapContainerStyle}
          mapStyle={mapStyle}
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
            const dotStyle = {
              backgroundColor:
                style.fillColor || style.strokeColor || style.circleColor,
            };
            return (
              <button
                key={`${layer.name}-${index}`}
                data-testid={`map-legend-${layer.name}-button`}
                className="hover:bg-muted flex items-center gap-2 rounded-md transition-colors"
              >
                <div className="size-4 rounded-full" style={dotStyle} />
                <span className="text-xs font-medium">{layer.name}</span>
              </button>
            );
          })}
        </div>
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
    <div>
      <MapErrorBoundary>
        <BlockMapChartInner chart={chart} />
      </MapErrorBoundary>

      {/* Documentation accordion */}
      {chart.documentation && (
        <DocumentationAccordion documentation={chart.documentation} />
      )}
    </div>
  );
}

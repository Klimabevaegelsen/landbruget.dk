'use client';

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { AlertTriangle, Info } from 'lucide-react';
import Map, {
  MapLayerMouseEvent,
  NavigationControl,
  ViewState,
} from 'react-map-gl/maplibre';
import 'maplibre-gl/dist/maplibre-gl.css';
import { useMapTheme } from '@/hooks/useMapTheme';
import { 
  FertilizerData, 
  FertilizerMapFilters, 
  MapLayerVisibility,
  FertilizerTooltipData 
} from '../livestock-analysis/types';
import { FertilizerMapTooltip } from './FertilizerMapTooltip';
import { FertilizerColorLegend } from './FertilizerColorLegend';

// Type for MapLibre map instance
interface MapInstance {
  getSource: (id: string) => unknown;
  getLayer: (id: string) => unknown;
  addLayer: (layer: unknown) => void;
  removeLayer: (id: string) => void;
  removeSource: (id: string) => void;
  setLayoutProperty: (id: string, prop: string, value: string) => void;
  setPaintProperty: (id: string, prop: string, value: unknown) => void;
  addSource: (id: string, source: unknown) => void;
  setFilter: (id: string, filter: unknown) => void;
  queryRenderedFeatures: (point?: [number, number], options?: unknown) => unknown[];
  getCanvas: () => HTMLCanvasElement;
}

interface FertilizerMapProps {
  data: FertilizerData[];
  filters: FertilizerMapFilters;
  onCompanySelect?: (company: FertilizerData) => void;
  onMapReady?: () => void;
  viewState?: Partial<ViewState>;
  onViewStateChange?: (viewState: ViewState) => void;
  className?: string;
}

// Color schemes for different fertilizer visualizations
const COLOR_SCHEMES = {
  nitrogen_production: {
    name: 'Kvælstofproduktion (kg N)',
    colors: ['#f7fcf0', '#e0f3db', '#ccebc5', '#a8ddb5', '#7bccc4', '#4eb3d3', '#2b8cbe', '#08519c'],
    property: 'total_nitrogen_production'
  },
  phosphorus_production: {
    name: 'Fosforproduktion (kg P)', 
    colors: ['#fff7ec', '#fee8c8', '#fdd49e', '#fdbb84', '#fc8d59', '#ef6548', '#d7301f', '#990000'],
    property: 'total_phosphorus_production'
  },
  commercial_fertilizer: {
    name: 'Handelsgødning (kg N)',
    colors: ['#f7fcfd', '#e5f5f9', '#ccece6', '#99d8c9', '#66c2a4', '#41ae76', '#238b45', '#005824'],
    property: 'commercial_fertilizer_usage'
  },
  biogas: {
    name: 'Biogasproduktion (kg)',
    colors: ['#fcfbfd', '#efedf5', '#dadaeb', '#bcbddc', '#9e9ac8', '#807dba', '#6a51a3', '#4a1486'],
    property: 'biogas_production'
  },
  manure_types: {
    name: 'Gødningstyper Mix',
    colors: ['#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c', '#fc4e2a', '#e31a1c', '#b10026'],
    property: 'manure_diversity'
  },
  nutrient_balance: {
    name: 'Næringsstofbalance',
    colors: ['#d73027', '#f46d43', '#fdae61', '#fee08b', '#ffffbf', '#d9ef8b', '#a6d96a', '#66bd63'],
    property: 'nutrient_balance'
  }
};

export default function FertilizerMap({
  data,
  filters,
  onCompanySelect,
  onMapReady,
  viewState: externalViewState,
  onViewStateChange,
  className = '',
}: FertilizerMapProps) {
  const mapRef = useRef<any>(null);
  const [isMapLoaded, setIsMapLoaded] = useState(false);
  const [hoveredFeature, setHoveredFeature] = useState<FertilizerTooltipData | null>(null);
  const [mousePosition, setMousePosition] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const { mapStyle } = useMapTheme();
  
  // Layer visibility state
  const [layerVisibility, setLayerVisibility] = useState<MapLayerVisibility>({
    companies: true,
    density: false,
    municipalities: true,
  });

  // Default view state centered on Denmark
  const [viewState, setViewState] = useState<Partial<ViewState>>({
    longitude: 9.501785,
    latitude: 56.26392,
    zoom: 7,
    ...externalViewState,
  });

  // Process data for mapping with fertilizer-specific calculations
  const processedData = React.useMemo(() => {
    return data
      .filter(company => 
        company.address_latitude && 
        company.address_longitude &&
        company.address_latitude !== 0 &&
        company.address_longitude !== 0
      )
      .map(company => {
        // Calculate nitrogen production from various sources
        const totalNitrogenProduction = (
          (company.f_601_2_svinegylle_kg_n || 0) +
          (company.f_602_2_kvaeggylle_kg_n || 0) +
          (company.f_613_2_minkgylle_og_gylle_fra_oevrige_koedaedende_pelsdyr_kg_n || 0) +
          (company.f_614_2_fjerkregylle_kg_n || 0) +
          (company.f_604_2_fast_goedning_kg_n || 0) +
          (company.f_303_1_normproduktion_kg_n_ghi_beregnet || 0)
        );

        // Calculate phosphorus production
        const totalPhosphorusProduction = (
          (company.f_303_3_normproduktion_kg_p_ghi_beregnet || 0) +
          (company.c_2021_normproduktion_fosfor_ghi_beregnet || 0)
        );

        // Commercial fertilizer usage
        const commercialFertilizerUsage = (
          (company.f_703_1_indkoebt_kunstgoedning_fratrukket_solgt_kunstgoedning_kg_n || 0) +
          (company.f_706_1_samlet_forbrug_af_handelsgoedning_kg_n || 0)
        );

        // Biogas production (estimated from manure types)
        const biogasProduction = (
          // Using existing fields to estimate biogas production
          ((company.f_601_2_svinegylle_kg_n || 0) + 
           (company.f_602_2_kvaeggylle_kg_n || 0)) * 0.1 // Estimate 10% goes to biogas
        );

        // Manure diversity score
        const manureTypes = [
          company.f_601_2_svinegylle_kg_n,
          company.f_602_2_kvaeggylle_kg_n,
          company.f_613_2_minkgylle_og_gylle_fra_oevrige_koedaedende_pelsdyr_kg_n,
          company.f_614_2_fjerkregylle_kg_n,
          company.f_604_2_fast_goedning_kg_n,
        ].filter(val => val && val > 0);
        const manureDiversity = manureTypes.length * (totalNitrogenProduction / manureTypes.length || 0);

        // Nutrient balance (simplified)
        const nutrientBalance = (
          (company.f_902_kvaelstofkvote_minus_forbrug_af_kvaelstof || 0) +
          (company.f_244_harmoniareal_minus_fosforarealkrav_ha || 0) * 100 // Convert ha to kg equivalent
        );

        return {
          ...company,
          total_nitrogen_production: totalNitrogenProduction,
          total_phosphorus_production: totalPhosphorusProduction,
          commercial_fertilizer_usage: commercialFertilizerUsage,
          biogas_production: biogasProduction,
          manure_diversity: manureDiversity,
          nutrient_balance: nutrientBalance,
        };
      });
  }, [data]);

  // Calculate data ranges for color scaling
  const dataRanges = React.useMemo(() => {
    if (processedData.length === 0) return {};
    
    const ranges: Record<string, [number, number]> = {};
    Object.values(COLOR_SCHEMES).forEach(scheme => {
      const values = processedData
        .map(d => (d as any)[scheme.property])
        .filter(v => v != null && !isNaN(v) && v > 0);
      
      if (values.length > 0) {
        ranges[scheme.property] = [Math.min(...values), Math.max(...values)];
      } else {
        ranges[scheme.property] = [0, 1];
      }
    });
    
    return ranges;
  }, [processedData]);

  // Get color for a value based on current visualization mode
  const getColorForValue = useCallback((value: number, property: string): string => {
    const range = dataRanges[property];
    if (!range || value <= 0) return '#f0f0f0';
    
    const scheme = COLOR_SCHEMES[filters.visualizationMode as keyof typeof COLOR_SCHEMES];
    const [min, max] = range;
    const normalized = Math.max(0, Math.min(1, (value - min) / (max - min)));
    const colorIndex = Math.floor(normalized * (scheme.colors.length - 1));
    return scheme.colors[colorIndex];
  }, [dataRanges, filters.visualizationMode]);

  // Get circle size based on value
  const getSizeForValue = useCallback((value: number, property: string): number => {
    const range = dataRanges[property];
    if (!range || value <= 0) return 4;
    
    const [min, max] = range;
    const normalized = Math.max(0, Math.min(1, (value - min) / (max - min)));
    return 4 + (normalized * 12); // Size range from 4 to 16
  }, [dataRanges]);

  // Create GeoJSON data
  const geoJsonData = React.useMemo(() => {
    const currentScheme = COLOR_SCHEMES[filters.visualizationMode as keyof typeof COLOR_SCHEMES];
    
    return {
      type: 'FeatureCollection' as const,
      features: processedData.map(company => {
        const value = (company as any)[currentScheme.property] || 0;
        return {
          type: 'Feature' as const,
          properties: {
            cvr_number: company.cvr_number,
            company_name: company.company_name,
            municipality: company.municipality,
            total_nitrogen_production: company.total_nitrogen_production,
            total_phosphorus_production: company.total_phosphorus_production,
            commercial_fertilizer_usage: company.commercial_fertilizer_usage,
            biogas_production: company.biogas_production,
            manure_diversity: company.manure_diversity,
            nutrient_balance: company.nutrient_balance,
            color: getColorForValue(value, currentScheme.property),
            size: getSizeForValue(value, currentScheme.property),
            value: value,
          },
          geometry: {
            type: 'Point' as const,
            coordinates: [company.address_longitude!, company.address_latitude!],
          },
        };
      }),
    };
  }, [processedData, getColorForValue, getSizeForValue, filters.visualizationMode]);

  // Handle map load
  const handleMapLoad = useCallback(() => {
    const map = mapRef.current?.getMap() as MapInstance;
    if (!map) return;

    // Add fertilizer companies source and layer
    map.addSource('fertilizer-companies', {
      type: 'geojson',
      data: geoJsonData,
    });

    // Add circles layer with dynamic sizing
    map.addLayer({
      id: 'fertilizer-companies-circles',
      type: 'circle',
      source: 'fertilizer-companies',
      paint: {
        'circle-radius': ['get', 'size'],
        'circle-color': ['get', 'color'],
        'circle-opacity': 0.8,
        'circle-stroke-width': 1,
        'circle-stroke-color': '#ffffff',
      },
    });

    // Add labels layer for larger producers
    map.addLayer({
      id: 'fertilizer-companies-labels',
      type: 'symbol',
      source: 'fertilizer-companies',
      filter: ['>', ['get', 'value'], 1000], // Only show labels for significant producers
      layout: {
        'text-field': ['get', 'company_name'],
        'text-font': ['Open Sans Regular'],
        'text-size': 10,
        'text-offset': [0, -2],
        'text-anchor': 'bottom',
      },
      paint: {
        'text-color': '#333333',
        'text-halo-color': '#ffffff',
        'text-halo-width': 1,
      },
    });

    setIsMapLoaded(true);
    onMapReady?.();
  }, [geoJsonData, onMapReady]);

  // Update data when it changes
  useEffect(() => {
    if (!isMapLoaded) return;
    
    const map = mapRef.current?.getMap() as MapInstance;
    if (!map || !map.getSource('fertilizer-companies')) return;

    (map.getSource('fertilizer-companies') as any).setData(geoJsonData);
  }, [geoJsonData, isMapLoaded]);

  // Handle mouse events
  const handleMouseMove = useCallback((e: MapLayerMouseEvent) => {
    const map = mapRef.current?.getMap() as MapInstance;
    if (!map || !isMapLoaded) return;

    // Check if layer exists before querying
    if (!map.getLayer('fertilizer-companies-circles')) return;

    setMousePosition({ x: e.point.x, y: e.point.y });

    try {
      const features = map.queryRenderedFeatures([e.point.x, e.point.y], {
        layers: ['fertilizer-companies-circles'],
      });

      if (features.length > 0) {
        const feature = features[0] as any;
        const props = feature.properties;
        
        setHoveredFeature({
          cvr_number: props.cvr_number,
          company_name: props.company_name,
          municipality: props.municipality,
          total_nitrogen_production: props.total_nitrogen_production,
          total_phosphorus_production: props.total_phosphorus_production,
          commercial_fertilizer_usage: props.commercial_fertilizer_usage,
          biogas_production: props.biogas_production,
          dominant_fertilizer_type: 'Blandet', // Could be calculated
          coordinate: feature.geometry.coordinates,
        });
        
        // Change cursor to pointer
        map.getCanvas().style.cursor = 'pointer';
      } else {
        setHoveredFeature(null);
        map.getCanvas().style.cursor = '';
      }
    } catch (error) {
      setHoveredFeature(null);
      map.getCanvas().style.cursor = '';
    }
  }, []);

  const handleMouseLeave = useCallback(() => {
    setHoveredFeature(null);
    const map = mapRef.current?.getMap() as MapInstance;
    if (map) {
      map.getCanvas().style.cursor = '';
    }
  }, []);

  const handleClick = useCallback((e: MapLayerMouseEvent) => {
    const map = mapRef.current?.getMap() as MapInstance;
    if (!map || !isMapLoaded) return;

    // Check if layer exists before querying
    const layer = map.getLayer('fertilizer-companies-circles');
    if (!layer) return;

    try {
      const features = map.queryRenderedFeatures([e.point.x, e.point.y], {
        layers: ['fertilizer-companies-circles'],
      });

      if (features.length > 0 && onCompanySelect) {
        const feature = features[0] as any;
        const cvr = feature.properties.cvr_number;
        const company = data.find(c => c.cvr_number === cvr);
        if (company) {
          onCompanySelect(company);
        }
      }
    } catch (error) {
      console.error('Error querying features:', error);
    }
  }, [data, onCompanySelect, isMapLoaded]);

  // Handle view state changes
  const handleViewStateChange = useCallback((evt: any) => {
    const newViewState = evt.viewState;
    setViewState(newViewState);
    onViewStateChange?.(newViewState);
  }, [onViewStateChange]);

  if (processedData.length === 0) {
    return (
      <div className={`flex h-96 items-center justify-center rounded-lg border bg-muted/30 ${className}`}>
        <div className="text-center">
          <AlertTriangle className="mx-auto h-8 w-8 text-muted-foreground" />
          <p className="mt-2 text-sm text-muted-foreground">
            Ingen gødnings- og næringsstofdata tilgængelig med geografiske koordinater
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className={`relative overflow-hidden rounded-lg border ${className}`}>
      <Map
        ref={mapRef}
        {...viewState}
        onMove={handleViewStateChange}
        onLoad={handleMapLoad}
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        onClick={handleClick}
        mapStyle={mapStyle}
        style={{ width: '100%', height: '100%' }}
        interactiveLayerIds={['fertilizer-companies-circles']}
      >
        <NavigationControl position="top-right" />
        
        {hoveredFeature && (
          <FertilizerMapTooltip
            data={hoveredFeature}
            position={mousePosition}
            visualizationMode={filters.visualizationMode}
          />
        )}
      </Map>

      {/* Legend */}
      <div className="absolute bottom-4 left-4 z-10">
        <FertilizerColorLegend
          scheme={COLOR_SCHEMES[filters.visualizationMode as keyof typeof COLOR_SCHEMES]}
          range={dataRanges[COLOR_SCHEMES[filters.visualizationMode as keyof typeof COLOR_SCHEMES].property]}
        />
      </div>

      {/* Data info */}
      <div className="absolute top-4 left-4 z-10">
        <div className="bg-background/90 rounded-lg p-2 shadow-sm">
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Info className="h-3 w-3" />
            <span>{processedData.length} virksomheder med gødningsdata</span>
          </div>
        </div>
      </div>
    </div>
  );
}

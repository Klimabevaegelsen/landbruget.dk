import { NextRequest, NextResponse } from 'next/server';
import { DataManager } from '@/lib/data-processing';
import { DataVirtualizer } from '@/lib/data-virtualization';
import type { BBRDataFilter, BBRBuildingType } from '@/types/bbr-data';

// Initialize data manager and virtualizer
const dataManager = new DataManager();
const dataVirtualizer = new DataVirtualizer();

/**
 * GET /api/bbr-data
 * Fetch BBR buildings data with filtering
 * 
 * Query parameters:
 * - buildingTypes: string (optional, comma-separated list)
 * - minConstructionYear: number (optional)
 * - maxConstructionYear: number (optional)
 * - minFloorArea: number (optional)
 * - maxFloorArea: number (optional)
 * - bbox: string (optional, format: "minLon,minLat,maxLon,maxLat")
 * - viewport: string (optional, format: "lat,lon,zoom")
 * - proximity: string (optional, format: "lat,lon,radiusKm")
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    
    // Build filter object
    const filter: BBRDataFilter = {};

    // Parse building types filter
    const buildingTypesParam = searchParams.get('buildingTypes');
    if (buildingTypesParam) {
      const buildingTypes = buildingTypesParam.split(',').map(type => type.trim()) as BBRBuildingType[];
      // Validate building types
      const validBuildingTypes: BBRBuildingType[] = ['Residential', 'Agricultural', 'Industrial', 'Commercial', 'Public', 'Other'];
      filter.buildingTypes = buildingTypes.filter(type => validBuildingTypes.includes(type));
    }

    // Parse numeric filters
    const minConstructionYear = searchParams.get('minConstructionYear');
    if (minConstructionYear) {
      const parsed = parseInt(minConstructionYear);
      if (!isNaN(parsed) && parsed >= 1800 && parsed <= new Date().getFullYear()) {
        filter.minConstructionYear = parsed;
      }
    }
    
    const maxConstructionYear = searchParams.get('maxConstructionYear');
    if (maxConstructionYear) {
      const parsed = parseInt(maxConstructionYear);
      if (!isNaN(parsed) && parsed >= 1800 && parsed <= new Date().getFullYear()) {
        filter.maxConstructionYear = parsed;
      }
    }

    const minFloorArea = searchParams.get('minFloorArea');
    if (minFloorArea) {
      const parsed = parseFloat(minFloorArea);
      if (!isNaN(parsed) && parsed >= 0) {
        filter.minFloorArea = parsed;
      }
    }
    
    const maxFloorArea = searchParams.get('maxFloorArea');
    if (maxFloorArea) {
      const parsed = parseFloat(maxFloorArea);
      if (!isNaN(parsed) && parsed >= 0) {
        filter.maxFloorArea = parsed;
      }
    }

    // Parse bounding box filter
    const bboxParam = searchParams.get('bbox');
    if (bboxParam) {
      const bboxParts = bboxParam.split(',').map(parseFloat);
      if (bboxParts.length === 4 && bboxParts.every(n => !isNaN(n))) {
        filter.bbox = {
          minLon: bboxParts[0],
          minLat: bboxParts[1],
          maxLon: bboxParts[2],
          maxLat: bboxParts[3]
        };
      }
    }

    // Parse proximity filter
    const proximityParam = searchParams.get('proximity');
    if (proximityParam) {
      const proximityParts = proximityParam.split(',').map(parseFloat);
      if (proximityParts.length === 3 && proximityParts.every(n => !isNaN(n))) {
        filter.proximityFilter = {
          centerLat: proximityParts[0],
          centerLon: proximityParts[1],
          radiusKm: proximityParts[2]
        };
      }
    }

    // Fetch data from database
    console.log('Fetching BBR data with filters:', filter);
    const startTime = Date.now();
    
    const bbrData = await dataManager.fetchBBRData(filter);
    
    const fetchDuration = Date.now() - startTime;
    console.log(`BBR data fetched: ${bbrData.length} records in ${fetchDuration}ms`);

    // Apply viewport-based filtering if viewport is provided
    const viewportParam = searchParams.get('viewport');
    let filteredData = bbrData;
    
    if (viewportParam) {
      const viewportParts = viewportParam.split(',').map(parseFloat);
      if (viewportParts.length === 3 && viewportParts.every(n => !isNaN(n))) {
        const viewport = {
          latitude: viewportParts[0],
          longitude: viewportParts[1],
          zoom: viewportParts[2]
        };
        
        filteredData = dataVirtualizer.filterBBRData(bbrData, viewport);
        console.log(`Viewport filtering applied: ${filteredData.length} records after filtering`);
      }
    }

    // Calculate statistics
    const statistics = calculateBBRStatistics(filteredData);

    // Return response
    const response = {
      data: filteredData,
      statistics,
      metadata: {
        totalRecords: filteredData.length,
        fetchDuration,
        filters: filter,
        timestamp: new Date().toISOString()
      }
    };

    // Set cache headers for performance
    const headers = new Headers({
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=3600, stale-while-revalidate=600', // 1 hour cache
      'X-Total-Count': filteredData.length.toString(),
      'X-Fetch-Duration': fetchDuration.toString()
    });

    return new NextResponse(JSON.stringify(response), { 
      headers,
      status: 200 
    });

  } catch (error) {
    console.error('BBR data API error:', error);
    
    const errorMessage = error instanceof Error ? error.message : 'Internal server error';
    
    return NextResponse.json(
      { 
        error: errorMessage,
        timestamp: new Date().toISOString()
      },
      { status: 500 }
    );
  }
}

/**
 * Calculate BBR statistics for the data panel
 */
function calculateBBRStatistics(data: any[]) {
  if (data.length === 0) {
    return {
      totalBuildings: 0,
      typeBreakdown: {},
      constructionYearRange: { min: 0, max: 0 },
      totalFloorArea: 0,
      averageFloorArea: 0,
      buildingDensity: 0
    };
  }

  const totalFloorArea = data.reduce((sum, building) => sum + (building.floor_area || 0), 0);
  const typeBreakdown: Record<string, any> = {};
  const constructionYears = data
    .map(building => building.construction_year)
    .filter(year => year !== null && year !== undefined);
  
  // Calculate breakdown by building type
  data.forEach(building => {
    const type = building.building_type || 'Other';
    if (!typeBreakdown[type]) {
      typeBreakdown[type] = {
        count: 0,
        percentage: 0,
        avgFloorArea: 0,
        avgConstructionYear: 0
      };
    }
    typeBreakdown[type].count += 1;
  });

  // Calculate percentages and averages for each type
  Object.keys(typeBreakdown).forEach(type => {
    const typeBuildings = data.filter(building => (building.building_type || 'Other') === type);
    
    typeBreakdown[type].percentage = (typeBreakdown[type].count / data.length) * 100;
    
    const typeFloorAreas = typeBuildings
      .map(building => building.floor_area)
      .filter(area => area !== null && area !== undefined);
    typeBreakdown[type].avgFloorArea = typeFloorAreas.length > 0 
      ? typeFloorAreas.reduce((sum, area) => sum + area, 0) / typeFloorAreas.length 
      : 0;
    
    const typeConstructionYears = typeBuildings
      .map(building => building.construction_year)
      .filter(year => year !== null && year !== undefined);
    typeBreakdown[type].avgConstructionYear = typeConstructionYears.length > 0 
      ? typeConstructionYears.reduce((sum, year) => sum + year, 0) / typeConstructionYears.length 
      : 0;
  });

  // Calculate construction year range
  const constructionYearRange = constructionYears.length > 0 
    ? {
        min: Math.min(...constructionYears),
        max: Math.max(...constructionYears)
      }
    : { min: 0, max: 0 };

  // Rough building density calculation (buildings per km²)
  // This is a simplified calculation - in production would use actual geographic area
  const approximateAreaKm2 = 43094; // Denmark's area in km²
  const buildingDensity = data.length / approximateAreaKm2;

  return {
    totalBuildings: data.length,
    typeBreakdown,
    constructionYearRange,
    totalFloorArea,
    averageFloorArea: totalFloorArea / data.length,
    buildingDensity
  };
} 
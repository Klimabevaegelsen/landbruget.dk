import { NextRequest, NextResponse } from 'next/server';
import { DataManager } from '@/lib/data-processing';
import { DataVirtualizer } from '@/lib/data-virtualization';
import type { BNBODataFilter, BNBOStatusCode } from '@/types/bnbo-data';

// Initialize data manager and virtualizer
const dataManager = new DataManager();
const dataVirtualizer = new DataVirtualizer();

/**
 * GET /api/bnbo-data
 * Fetch BNBO status areas data with filtering
 * 
 * Query parameters:
 * - statusCodes: string (optional, comma-separated list)
 * - minAreaHa: number (optional)
 * - maxAreaHa: number (optional)
 * - year: number (optional)
 * - bbox: string (optional, format: "minLon,minLat,maxLon,maxLat")
 * - viewport: string (optional, format: "lat,lon,zoom")
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    
    // Build filter object
    const filter: BNBODataFilter = {};

    // Parse status codes filter
    const statusCodesParam = searchParams.get('statusCodes');
    if (statusCodesParam) {
      const statusCodes = statusCodesParam.split(',').map(code => code.trim()) as BNBOStatusCode[];
      // Validate status codes
      const validStatusCodes: BNBOStatusCode[] = ['protected', 'buffer', 'agricultural', 'transition', 'unprotected'];
      filter.statusCodes = statusCodes.filter(code => validStatusCodes.includes(code));
    }

    // Parse numeric filters
    const minAreaHa = searchParams.get('minAreaHa');
    if (minAreaHa) {
      const parsed = parseFloat(minAreaHa);
      if (!isNaN(parsed) && parsed >= 0) {
        filter.minAreaHa = parsed;
      }
    }
    
    const maxAreaHa = searchParams.get('maxAreaHa');
    if (maxAreaHa) {
      const parsed = parseFloat(maxAreaHa);
      if (!isNaN(parsed) && parsed >= 0) {
        filter.maxAreaHa = parsed;
      }
    }

    // Parse year filter
    const yearParam = searchParams.get('year');
    if (yearParam) {
      const year = parseInt(yearParam);
      if (!isNaN(year) && year >= 2020 && year <= 2025) {
        filter.year = year;
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

    // Fetch data from database
    console.log('Fetching BNBO data with filters:', filter);
    const startTime = Date.now();
    
    const bnboData = await dataManager.fetchBNBOData(filter);
    
    const fetchDuration = Date.now() - startTime;
    console.log(`BNBO data fetched: ${bnboData.length} records in ${fetchDuration}ms`);

    // Apply viewport-based filtering if viewport is provided
    const viewportParam = searchParams.get('viewport');
    let filteredData = bnboData;
    
    if (viewportParam) {
      const viewportParts = viewportParam.split(',').map(parseFloat);
      if (viewportParts.length === 3 && viewportParts.every(n => !isNaN(n))) {
        const viewport = {
          latitude: viewportParts[0],
          longitude: viewportParts[1],
          zoom: viewportParts[2]
        };
        
        filteredData = dataVirtualizer.filterBNBOData(bnboData, viewport);
        console.log(`Viewport filtering applied: ${filteredData.length} records after filtering`);
      }
    }

    // Calculate statistics
    const statistics = calculateBNBOStatistics(filteredData);

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
      'Cache-Control': 'public, max-age=1800, stale-while-revalidate=300', // 30 minutes cache
      'X-Total-Count': filteredData.length.toString(),
      'X-Fetch-Duration': fetchDuration.toString()
    });

    return new NextResponse(JSON.stringify(response), { 
      headers,
      status: 200 
    });

  } catch (error) {
    console.error('BNBO data API error:', error);
    
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
 * Calculate BNBO statistics for the data panel
 */
function calculateBNBOStatistics(data: any[]) {
  if (data.length === 0) {
    return {
      totalAreas: 0,
      totalAreaHa: 0,
      statusBreakdown: {},
      averageAreaHa: 0,
      largestAreaHa: 0,
      protectionCoverage: 0
    };
  }

  const totalAreaHa = data.reduce((sum, area) => sum + (area.area_ha || 0), 0);
  const statusBreakdown: Record<string, any> = {};
  
  // Calculate breakdown by status
  data.forEach(area => {
    const status = area.status_code;
    if (!statusBreakdown[status]) {
      statusBreakdown[status] = {
        count: 0,
        totalAreaHa: 0,
        percentage: 0
      };
    }
    statusBreakdown[status].count += 1;
    statusBreakdown[status].totalAreaHa += area.area_ha || 0;
  });

  // Calculate percentages
  Object.keys(statusBreakdown).forEach(status => {
    statusBreakdown[status].percentage = totalAreaHa > 0 
      ? (statusBreakdown[status].totalAreaHa / totalAreaHa) * 100 
      : 0;
  });

  // Calculate protection coverage (protected + buffer zones)
  const protectedArea = (statusBreakdown['protected']?.totalAreaHa || 0) + 
                       (statusBreakdown['buffer']?.totalAreaHa || 0);
  const protectionCoverage = totalAreaHa > 0 ? (protectedArea / totalAreaHa) * 100 : 0;

  return {
    totalAreas: data.length,
    totalAreaHa,
    statusBreakdown,
    averageAreaHa: totalAreaHa / data.length,
    largestAreaHa: Math.max(...data.map(area => area.area_ha || 0)),
    protectionCoverage
  };
} 
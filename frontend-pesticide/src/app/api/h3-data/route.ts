import { NextRequest, NextResponse } from 'next/server';
import { DataManager } from '@/lib/data-processing';
import { DataVirtualizer } from '@/lib/data-virtualization';
import type { H3DataFilter, H3ProcessingConfig } from '@/types/h3-data';

// Initialize data manager and virtualizer
const dataManager = new DataManager();
const dataVirtualizer = new DataVirtualizer();

/**
 * GET /api/h3-data
 * Fetch H3 PFAS exposure data with filtering and performance optimization
 * 
 * Query parameters:
 * - year: number (required)
 * - cumulative: boolean (optional, default false)
 * - minPesticideLoad: number (optional)
 * - maxPesticideLoad: number (optional)
 * - minPfasGrams: number (optional)
 * - maxPfasGrams: number (optional)
 * - bbox: string (optional, format: "minLon,minLat,maxLon,maxLat")
 * - viewport: string (optional, format: "lat,lon,zoom")
 * - stream: boolean (optional, default false)
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    
    // Parse required parameters
    const yearParam = searchParams.get('year');
    if (!yearParam) {
      return NextResponse.json(
        { error: 'Year parameter is required' },
        { status: 400 }
      );
    }
    
    const year = parseInt(yearParam);
    if (isNaN(year) || year < 2020 || year > 2025) {
      return NextResponse.json(
        { error: 'Invalid year. Must be between 2020 and 2025' },
        { status: 400 }
      );
    }

    // Parse optional parameters
    const cumulativeMode = searchParams.get('cumulative') === 'true';
    const useStreaming = searchParams.get('stream') === 'true';
    
    // Build filter object
    const filter: H3DataFilter = {
      cumulativeMode
    };

    // Parse numeric filters
    const minPesticideLoad = searchParams.get('minPesticideLoad');
    if (minPesticideLoad) {
      filter.minPesticideLoad = parseFloat(minPesticideLoad);
    }
    
    const maxPesticideLoad = searchParams.get('maxPesticideLoad');
    if (maxPesticideLoad) {
      filter.maxPesticideLoad = parseFloat(maxPesticideLoad);
    }
    
    const minPfasGrams = searchParams.get('minPfasGrams');
    if (minPfasGrams) {
      filter.minPfasGrams = parseFloat(minPfasGrams);
    }
    
    const maxPfasGrams = searchParams.get('maxPfasGrams');
    if (maxPfasGrams) {
      filter.maxPfasGrams = parseFloat(maxPfasGrams);
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

    // Processing configuration
    const config: H3ProcessingConfig = {
      aggregationMethod: cumulativeMode ? 'sum' : 'average',
      includeIntensityCalculations: true,
      geometryFormat: 'geojson',
      coordinateSystem: 'EPSG:4326'
    };

    // Fetch data from database
    console.log(`Fetching H3 data for year ${year}, cumulative: ${cumulativeMode}`);
    const startTime = Date.now();
    
    const h3Data = await dataManager.fetchH3Data(year, cumulativeMode, filter, config);
    
    const fetchDuration = Date.now() - startTime;
    console.log(`H3 data fetched: ${h3Data.length} records in ${fetchDuration}ms`);

    // Apply viewport-based filtering if viewport is provided
    const viewportParam = searchParams.get('viewport');
    let filteredData = h3Data;
    
    if (viewportParam) {
      const viewportParts = viewportParam.split(',').map(parseFloat);
      if (viewportParts.length === 3 && viewportParts.every(n => !isNaN(n))) {
        const viewport = {
          latitude: viewportParts[0],
          longitude: viewportParts[1],
          zoom: viewportParts[2]
        };
        
        filteredData = dataVirtualizer.filterH3Data(h3Data, viewport);
        console.log(`Viewport filtering applied: ${filteredData.length} records after filtering`);
      }
    }

    // Return streaming response for large datasets
    if (useStreaming && filteredData.length > 1000) {
      return createStreamingResponse(filteredData, {
        year,
        cumulative: cumulativeMode,
        totalRecords: filteredData.length,
        fetchDuration
      });
    }

    // Return standard JSON response
    const response = {
      data: filteredData,
      metadata: {
        year,
        cumulative: cumulativeMode,
        totalRecords: filteredData.length,
        fetchDuration,
        filters: filter,
        timestamp: new Date().toISOString()
      }
    };

    // Set cache headers for performance
    const headers = new Headers({
      'Content-Type': 'application/json',
      'Cache-Control': 'public, max-age=300, stale-while-revalidate=60', // 5 minutes cache
      'X-Total-Count': filteredData.length.toString(),
      'X-Fetch-Duration': fetchDuration.toString()
    });

    return new NextResponse(JSON.stringify(response), { 
      headers,
      status: 200 
    });

  } catch (error) {
    console.error('H3 data API error:', error);
    
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
 * Create streaming response for large datasets
 */
function createStreamingResponse(data: any[], metadata: any) {
  const encoder = new TextEncoder();
  
  const stream = new ReadableStream({
    start(controller) {
      // Send metadata first
      const metadataChunk = JSON.stringify({ 
        type: 'metadata', 
        ...metadata 
      }) + '\n';
      controller.enqueue(encoder.encode(metadataChunk));
    },
    
    pull(controller) {
      // Send data in chunks
      const chunkSize = 100;
      let index = 0;
      
      const sendChunk = () => {
        if (index >= data.length) {
          // Send completion signal
          const completionChunk = JSON.stringify({ 
            type: 'complete',
            totalSent: data.length 
          }) + '\n';
          controller.enqueue(encoder.encode(completionChunk));
          controller.close();
          return;
        }
        
        const chunk = data.slice(index, index + chunkSize);
        const dataChunk = JSON.stringify({ 
          type: 'data', 
          data: chunk,
          index,
          remaining: data.length - index - chunk.length
        }) + '\n';
        
        controller.enqueue(encoder.encode(dataChunk));
        index += chunkSize;
        
        // Continue with next chunk
        setTimeout(sendChunk, 10); // Small delay to prevent blocking
      };
      
      sendChunk();
    },
    
    cancel() {
      console.log('Streaming cancelled by client');
    }
  });

  return new NextResponse(stream, {
    headers: {
      'Content-Type': 'application/x-ndjson',
      'Cache-Control': 'no-cache',
      'Connection': 'keep-alive',
      'X-Content-Type-Options': 'nosniff'
    }
  });
}

/**
 * POST /api/h3-data
 * Bulk data operations (for data syncing)
 */
export async function POST(request: NextRequest) {
  try {
    // This endpoint would be used by the data syncer
    // For now, return method not allowed for security
    return NextResponse.json(
      { error: 'Bulk operations not implemented in this version' },
      { status: 501 }
    );
  } catch (error) {
    console.error('H3 data POST error:', error);
    
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
} 
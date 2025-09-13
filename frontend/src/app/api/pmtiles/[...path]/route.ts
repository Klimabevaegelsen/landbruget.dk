import { NextRequest, NextResponse } from 'next/server';

const PMTILES_BASE_URL = 'https://data.pesticidkortet.dk';
const CACHE_DURATION = 31536000; // 1 year in seconds (365 days)

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  const resolvedParams = await params;
  const pmtilesPath = resolvedParams.path.join('/');
  const pmtilesUrl = `${PMTILES_BASE_URL}/${pmtilesPath}`;

  try {
    // Check if it's a range request (PMTiles uses these for efficient tile loading)
    const range = request.headers.get('range');
    const requestHeaders: Record<string, string> = {};

    if (range) {
      requestHeaders.Range = range;
    }

    // Add cache-friendly headers
    requestHeaders['Cache-Control'] = 'public, max-age=31536000, immutable';

    console.log(
      `🗺️ Proxying PMTiles request: ${pmtilesPath}${range ? ` (range: ${range})` : ''}`
    );

    const response = await fetch(pmtilesUrl, {
      headers: requestHeaders,
    });

    if (!response.ok) {
      console.error(`❌ PMTiles not found: ${pmtilesUrl} - ${response.status}`);
      return NextResponse.json(
        { error: 'PMTiles file not found', path: pmtilesPath },
        { status: 404 }
      );
    }

    // Create response headers
    const responseHeaders = new Headers();

    // Copy essential headers for PMTiles functionality
    const essentialHeaders = [
      'content-type',
      'content-length',
      'content-range',
      'accept-ranges',
      'etag',
      'last-modified',
    ];

    essentialHeaders.forEach((header) => {
      const value = response.headers.get(header);
      if (value) {
        responseHeaders.set(header, value);
      }
    });

    // Add aggressive caching headers for immutable PMTiles
    responseHeaders.set(
      'Cache-Control',
      `public, max-age=${CACHE_DURATION}, immutable`
    );
    responseHeaders.set(
      'CDN-Cache-Control',
      `public, max-age=${CACHE_DURATION}`
    );
    responseHeaders.set(
      'Vercel-CDN-Cache-Control',
      `public, max-age=${CACHE_DURATION}`
    );

    // Add CORS headers for cross-origin requests
    responseHeaders.set('Access-Control-Allow-Origin', '*');
    responseHeaders.set('Access-Control-Allow-Methods', 'GET, HEAD, OPTIONS');
    responseHeaders.set('Access-Control-Allow-Headers', 'Range, Cache-Control');

    // Add performance headers
    responseHeaders.set('X-PMTiles-Cached', 'true');
    responseHeaders.set('X-Cache-Status', 'MISS'); // Will be HIT on subsequent requests

    console.log(
      `✅ PMTiles served: ${pmtilesPath} (${response.headers.get('content-length')} bytes)`
    );

    return new NextResponse(response.body, {
      status: response.status,
      headers: responseHeaders,
    });
  } catch (error) {
    console.error('❌ PMTiles proxy error:', error);
    return NextResponse.json(
      {
        error: 'Failed to fetch PMTiles',
        path: pmtilesPath,
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

// Handle preflight requests for CORS
export async function OPTIONS() {
  return new NextResponse(null, {
    status: 200,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, HEAD, OPTIONS',
      'Access-Control-Allow-Headers': 'Range, Cache-Control',
      'Access-Control-Max-Age': '86400', // 24 hours
    },
  });
}

// Support HEAD requests for PMTiles metadata checks
export async function HEAD(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  const resolvedParams = await params;
  const pmtilesPath = resolvedParams.path.join('/');
  const pmtilesUrl = `${PMTILES_BASE_URL}/${pmtilesPath}`;

  try {
    const response = await fetch(pmtilesUrl, { method: 'HEAD' });

    if (!response.ok) {
      return new NextResponse(null, { status: 404 });
    }

    const responseHeaders = new Headers();

    // Copy essential headers
    ['content-type', 'content-length', 'etag', 'last-modified'].forEach(
      (header) => {
        const value = response.headers.get(header);
        if (value) {
          responseHeaders.set(header, value);
        }
      }
    );

    // Add caching headers
    responseHeaders.set(
      'Cache-Control',
      `public, max-age=${CACHE_DURATION}, immutable`
    );
    responseHeaders.set('Access-Control-Allow-Origin', '*');

    return new NextResponse(null, {
      status: 200,
      headers: responseHeaders,
    });
  } catch (error) {
    console.error('PMTiles HEAD request error:', error);
    return new NextResponse(null, { status: 500 });
  }
}

import { NextRequest, NextResponse } from 'next/server';
import {
  getCacheStatusPayload,
  getRequestedTags,
  revalidateTags,
} from './cache-revalidation';

/**
 * API route to manually invalidate server-side caches
 * Call this endpoint after updating data on Tuesdays
 *
 * Usage:
 * POST /api/revalidate-cache
 * POST /api/revalidate-cache?tags=homepage-stats,homepage-rankings
 */
export async function POST(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const tags = getRequestedTags(searchParams.get('tags'));

    console.log('🔄 Manual cache invalidation requested for tags:', tags);
    revalidateTags(tags);

    return NextResponse.json({
      success: true,
      message: `Successfully revalidated ${tags.length} cache tag(s)`,
      tags,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    console.error('❌ Cache revalidation failed:', error);

    return NextResponse.json(
      {
        success: false,
        error: 'Failed to revalidate cache',
        message: error instanceof Error ? error.message : 'Unknown error',
        timestamp: new Date().toISOString(),
      },
      { status: 500 }
    );
  }
}

/**
 * GET endpoint to check cache status
 */
export async function GET() {
  return NextResponse.json(getCacheStatusPayload());
}

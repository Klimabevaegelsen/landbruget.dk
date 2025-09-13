import { NextRequest, NextResponse } from 'next/server';
import { revalidateTag } from 'next/cache';

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
    const tagsParam = searchParams.get('tags');

    // Default to all cache tags if none specified
    const tags = tagsParam
      ? tagsParam.split(',')
      : ['homepage-stats', 'homepage-rankings', 'municipality-rankings'];

    console.log('🔄 Manual cache invalidation requested for tags:', tags);

    // Revalidate each specified cache tag
    for (const tag of tags) {
      revalidateTag(tag.trim());
      console.log(`✅ Revalidated cache tag: ${tag.trim()}`);
    }

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
  return NextResponse.json({
    message: 'Cache revalidation endpoint',
    usage: {
      invalidate_all: 'POST /api/revalidate-cache',
      invalidate_specific:
        'POST /api/revalidate-cache?tags=homepage-stats,homepage-rankings',
    },
    available_tags: [
      'homepage-stats',
      'homepage-rankings',
      'municipality-rankings',
    ],
    next_tuesday: new Date(
      Date.now() + ((7 - new Date().getDay() + 2) % 7) * 24 * 60 * 60 * 1000
    ).toISOString(),
  });
}

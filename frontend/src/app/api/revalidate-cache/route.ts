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
  // Calculate next Tuesday in Copenhagen time (CET/CEST)
  const now = new Date();
  const copenhagenTime = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Copenhagen',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(now);

  // Find next Tuesday in Copenhagen
  const copenhagenNow = new Date(copenhagenTime.replace(' ', 'T'));
  const daysUntilTuesday = (2 - copenhagenNow.getDay() + 7) % 7;
  const nextTuesday = new Date(copenhagenNow);
  nextTuesday.setDate(copenhagenNow.getDate() + (daysUntilTuesday || 7));
  nextTuesday.setHours(9, 0, 0, 0); // 9:00 AM Copenhagen time

  return NextResponse.json({
    message: 'Cache revalidation endpoint for Tuesday data updates',
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
    cache_strategy: '7-day server cache + manual Tuesday invalidation',
    next_tuesday_copenhagen: nextTuesday.toLocaleString('da-DK', {
      timeZone: 'Europe/Copenhagen',
      weekday: 'long',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    }),
    current_copenhagen_time: copenhagenNow.toLocaleString('da-DK', {
      timeZone: 'Europe/Copenhagen',
      weekday: 'long',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    }),
  });
}

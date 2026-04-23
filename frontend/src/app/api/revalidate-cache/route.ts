import { NextRequest, NextResponse } from 'next/server';
import { revalidatePath, revalidateTag } from 'next/cache';

const TAG_TO_PATHS: Record<string, string[]> = {
  'homepage-stats': ['/api/homepage-statistics'],
  'homepage-rankings': ['/api/data/homepage-rankings'],
  'municipality-rankings': [
    '/api/data/kommuner',
    '/api/data/municipality-details',
  ],
  'pesticide-analysis': ['/api/data/pesticide-analysis'],
  'pesticide-company-details': ['/api/data/pesticide-company-details'],
  'burden-histogram': ['/api/burden-histogram'],
};

const copenhagenPartsFormatter = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Europe/Copenhagen',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

const copenhagenDisplayFormatter = new Intl.DateTimeFormat('da-DK', {
  timeZone: 'UTC',
  weekday: 'long',
  year: 'numeric',
  month: 'long',
  day: 'numeric',
  hour: '2-digit',
  minute: '2-digit',
});

function getCopenhagenWallClockDate(date: Date = new Date()) {
  const parts = Object.fromEntries(
    copenhagenPartsFormatter
      .formatToParts(date)
      .filter((part) => part.type !== 'literal')
      .map((part) => [part.type, part.value])
  );

  return new Date(
    Date.UTC(
      Number(parts.year),
      Number(parts.month) - 1,
      Number(parts.day),
      Number(parts.hour),
      Number(parts.minute),
      Number(parts.second)
    )
  );
}

function formatCopenhagenWallClockDate(date: Date) {
  return copenhagenDisplayFormatter.format(date);
}

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
      : [
          'homepage-stats',
          'homepage-rankings',
          'municipality-rankings',
          'pesticide-analysis',
          'pesticide-company-details',
          'burden-histogram',
        ];

    console.log('🔄 Manual cache invalidation requested for tags:', tags);

    // Revalidate each specified cache tag
    for (const tag of tags) {
      const normalizedTag = tag.trim();
      revalidateTag(normalizedTag, 'max');
      console.log(`✅ Revalidated cache tag: ${normalizedTag}`);

      for (const path of TAG_TO_PATHS[normalizedTag] ?? []) {
        revalidatePath(path);
        console.log(`✅ Revalidated cache path: ${path}`);
      }
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
  const copenhagenNow = getCopenhagenWallClockDate();
  const daysUntilTuesday = (2 - copenhagenNow.getUTCDay() + 7) % 7;
  const nextTuesday = new Date(copenhagenNow);
  nextTuesday.setUTCDate(copenhagenNow.getUTCDate() + (daysUntilTuesday || 7));
  nextTuesday.setUTCHours(9, 0, 0, 0); // 9:00 AM Copenhagen time

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
      'pesticide-analysis',
      'pesticide-company-details',
      'burden-histogram',
    ],
    cache_strategy: '7-day server cache + manual Tuesday invalidation',
    next_tuesday_copenhagen: formatCopenhagenWallClockDate(nextTuesday),
    current_copenhagen_time: formatCopenhagenWallClockDate(copenhagenNow),
  });
}

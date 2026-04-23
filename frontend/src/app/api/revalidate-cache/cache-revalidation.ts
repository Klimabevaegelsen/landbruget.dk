import { revalidatePath, revalidateTag } from 'next/cache';

export const AVAILABLE_TAGS = [
  'homepage-stats',
  'homepage-rankings',
  'municipality-rankings',
  'pesticide-analysis',
  'pesticide-company-details',
  'burden-histogram',
] as const;

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

export function getRequestedTags(tagsParam: string | null) {
  return tagsParam ? tagsParam.split(',') : [...AVAILABLE_TAGS];
}

export function revalidateTags(tags: string[]) {
  for (const tag of tags) {
    const normalizedTag = tag.trim();
    revalidateTag(normalizedTag, 'max');

    for (const path of TAG_TO_PATHS[normalizedTag] ?? []) {
      revalidatePath(path);
    }
  }
}

export function getCacheStatusPayload() {
  const copenhagenNow = getCopenhagenWallClockDate();
  const daysUntilTuesday = (2 - copenhagenNow.getUTCDay() + 7) % 7;
  const nextTuesday = new Date(copenhagenNow);
  nextTuesday.setUTCDate(copenhagenNow.getUTCDate() + (daysUntilTuesday || 7));
  nextTuesday.setUTCHours(9, 0, 0, 0);

  return {
    message: 'Cache revalidation endpoint for Tuesday data updates',
    usage: {
      invalidate_all: 'POST /api/revalidate-cache',
      invalidate_specific:
        'POST /api/revalidate-cache?tags=homepage-stats,homepage-rankings',
    },
    available_tags: AVAILABLE_TAGS,
    cache_strategy: '7-day server cache + manual Tuesday invalidation',
    next_tuesday_copenhagen: formatCopenhagenWallClockDate(nextTuesday),
    current_copenhagen_time: formatCopenhagenWallClockDate(copenhagenNow),
  };
}

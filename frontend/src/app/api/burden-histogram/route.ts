import { NextRequest, NextResponse } from 'next/server';
import { getCachedBurdenHistogram } from '@/lib/server-cache';

export const dynamic = 'force-dynamic';
export const revalidate = 604800;

const CACHE_HEADERS = {
  'Cache-Control': 'public, max-age=604800, stale-while-revalidate=604800',
  'CDN-Cache-Control': 'public, max-age=604800',
  'Vercel-CDN-Cache-Control': 'public, max-age=604800',
};

export async function GET(request: NextRequest) {
  const year = Number(new URL(request.url).searchParams.get('year') ?? '2024');

  const data = await getCachedBurdenHistogram(year);

  return NextResponse.json(data, { headers: CACHE_HEADERS });
}

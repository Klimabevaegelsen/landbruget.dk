import { ImageResponse } from 'next/og';
import type { NextRequest } from 'next/server';
import { PesticideOgImage } from './PesticideOgImage';

export const runtime = 'edge';

export function GET(request: NextRequest) {
  const { searchParams } = request.nextUrl;

  return new ImageResponse(
    <PesticideOgImage
      grade={searchParams.get('grade')}
      addr={searchParams.get('addr') ?? 'Din adresse'}
      fields={searchParams.get('fields') ?? '0'}
      pfas={searchParams.get('pfas') ?? '0'}
      dist={searchParams.get('dist') ?? '0'}
    />,
    { width: 1200, height: 630 }
  );
}

import { NextRequest, NextResponse } from 'next/server';

interface PurgeRequest {
  files: string[];
}

export async function POST(request: NextRequest) {
  const { CLOUDFLARE_ZONE_ID, CLOUDFLARE_API_TOKEN } = process.env;

  if (!CLOUDFLARE_ZONE_ID || !CLOUDFLARE_API_TOKEN) {
    return NextResponse.json(
      { success: false, error: 'Cloudflare credentials not configured' },
      { status: 500 }
    );
  }

  try {
    const body: PurgeRequest = await request.json();
    const filesToPurge = body.files.map(
      (file) => `https://data.pesticidkortet.dk/pmtiles/${file}`
    );

    const response = await fetch(
      `https://api.cloudflare.com/client/v4/zones/${CLOUDFLARE_ZONE_ID}/purge_cache`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${CLOUDFLARE_API_TOKEN}`,
        },
        body: JSON.stringify({ files: filesToPurge }),
      }
    );

    const data = await response.json();

    if (!response.ok) {
      return NextResponse.json(
        { success: false, error: 'Failed to purge cache', details: data },
        { status: response.status }
      );
    }

    return NextResponse.json({ success: true, data });
  } catch (error) {
    return NextResponse.json(
      { success: false, error: 'Internal server error' },
      { status: 500 }
    );
  }
}

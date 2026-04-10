import { NextRequest, NextResponse } from 'next/server';

const PMTILES_BASE_URL = 'https://data.pesticidkortet.dk';

// PMTiles files to proactively warm (in priority order)
const WARMUP_FILES = [
  // Current year files (highest priority)
  'field_analysis_2024.pmtiles',

  // Background layers (used by all users)
  'bnbo_areas.pmtiles', // BNBO areas from environmental generator
  'buildings_proximity.pmtiles', // Buildings proximity (year-independent)
  'wetlands_all.pmtiles', // Wetlands (year-independent)
  'water_projects.pmtiles', // Water projects (year-independent)

  // Historical years (medium priority)
  'field_analysis_2023.pmtiles',
  'field_analysis_2022.pmtiles',
  'field_analysis_2021.pmtiles',
  'field_analysis_2020.pmtiles',
  'field_analysis_2019.pmtiles',
  'field_analysis_2018.pmtiles',
  'field_analysis_2017.pmtiles',
  'field_analysis_2016.pmtiles',
  'field_analysis_2015.pmtiles',
];

interface WarmupResult {
  filename: string;
  status: 'success' | 'error' | 'already_cached';
  size?: number;
  duration?: number;
  error?: string;
}

export async function POST(request: NextRequest) {
  try {
    // Verify authorization (add your own auth logic)
    const authHeader = request.headers.get('authorization');
    const expectedToken = process.env.PMTILES_WARMUP_TOKEN;

    if (!expectedToken || authHeader !== `Bearer ${expectedToken}`) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    console.log('🔥 Starting PMTiles cache warmup...');

    const results: WarmupResult[] = [];
    const startTime = Date.now();

    // Process files in batches to avoid overwhelming the server
    const batchSize = 3;
    for (let i = 0; i < WARMUP_FILES.length; i += batchSize) {
      const batch = WARMUP_FILES.slice(i, i + batchSize);

      const batchPromises = batch.map(
        async (filename): Promise<WarmupResult> => {
          const fileStartTime = Date.now();

          try {
            // Use our own proxy API to warm the cache
            const proxyUrl = `${request.nextUrl.origin}/api/pmtiles/${filename}`;

            console.log(`🔥 Warming: ${filename}`);

            // Make HEAD request first to check if file exists
            const headResponse = await fetch(
              `${PMTILES_BASE_URL}/pmtiles/${filename}`,
              {
                method: 'HEAD',
                headers: {
                  'User-Agent': 'PMTiles-Warmup/1.0',
                },
              }
            );

            if (!headResponse.ok) {
              return {
                filename,
                status: 'error',
                error: `File not found: ${headResponse.status}`,
                duration: Date.now() - fileStartTime,
              };
            }

            const fileSize = headResponse.headers.get('content-length');

            // Now warm our proxy cache by making a range request for the header
            // PMTiles header is typically in the first 16KB
            const warmResponse = await fetch(proxyUrl, {
              headers: {
                Range: 'bytes=0-16383', // First 16KB
                'User-Agent': 'PMTiles-Warmup/1.0',
                'X-Cache-Warming': 'true',
              },
            });

            if (warmResponse.ok) {
              console.log(`✅ Warmed: ${filename} (${fileSize} bytes)`);
              return {
                filename,
                status: 'success',
                size: fileSize ? parseInt(fileSize) : undefined,
                duration: Date.now() - fileStartTime,
              };
            } else {
              return {
                filename,
                status: 'error',
                error: `Warmup failed: ${warmResponse.status}`,
                duration: Date.now() - fileStartTime,
              };
            }
          } catch (error) {
            console.error(`❌ Error warming ${filename}:`, error);
            return {
              filename,
              status: 'error',
              error: error instanceof Error ? error.message : 'Unknown error',
              duration: Date.now() - fileStartTime,
            };
          }
        }
      );

      const batchResults = await Promise.allSettled(batchPromises);

      // Add results from this batch
      batchResults.forEach((result) => {
        if (result.status === 'fulfilled') {
          results.push(result.value);
        } else {
          console.error('Batch promise rejected:', result.reason);
        }
      });

      // Small delay between batches to be nice to the server
      if (i + batchSize < WARMUP_FILES.length) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }
    }

    const totalDuration = Date.now() - startTime;
    const successCount = results.filter((r) => r.status === 'success').length;
    const errorCount = results.filter((r) => r.status === 'error').length;

    console.log(
      `🎯 PMTiles warmup completed: ${successCount} success, ${errorCount} errors in ${totalDuration}ms`
    );

    return NextResponse.json({
      success: true,
      summary: {
        total: results.length,
        success: successCount,
        errors: errorCount,
        duration: totalDuration,
      },
      results,
    });
  } catch (error) {
    console.error('❌ PMTiles warmup failed:', error);
    return NextResponse.json(
      {
        error: 'Warmup failed',
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      { status: 500 }
    );
  }
}

// GET endpoint to check warmup status
export async function GET() {
  try {
    const results = await Promise.all(
      WARMUP_FILES.slice(0, 5).map(async (filename) => {
        try {
          const response = await fetch(
            `${PMTILES_BASE_URL}/pmtiles/${filename}`,
            {
              method: 'HEAD',
            }
          );

          return {
            filename,
            available: response.ok,
            size: response.headers.get('content-length'),
            lastModified: response.headers.get('last-modified'),
          };
        } catch (error) {
          return {
            filename,
            available: false,
            error: error instanceof Error ? error.message : 'Unknown error',
          };
        }
      })
    );

    return NextResponse.json({
      warmupFiles: WARMUP_FILES,
      sampleStatus: results,
      instructions: {
        warmup: 'POST /api/pmtiles-warmup with Bearer token',
        token: 'Set PMTILES_WARMUP_TOKEN environment variable',
      },
    });
  } catch {
    return NextResponse.json({ error: 'Status check failed' }, { status: 500 });
  }
}

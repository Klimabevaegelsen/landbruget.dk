import { NextResponse } from 'next/server'

interface PMTilesFile {
  year: number
  resolution: number
  timestamp: string
  size: number
  url: string
  lastModified: string
}

interface BNBOFile {
  filename: string
  size: number
  url: string
  lastModified: string
  type: string
}

interface KommuneFile {
  filename: string
  year: number
  timestamp: string
  size: number
  url: string
  lastModified: string
  type: string
}

interface PMTilesMetadata {
  files: PMTilesFile[]
  years: number[]
  resolutions: number[]
  totalFiles: number
  lastUpdated: string
  bnbo?: {
    files: BNBOFile[]
    available: boolean
    status_codes: string[]
    status_colors: Record<string, string>
  }
  kommune?: {
    files: KommuneFile[]
    available: boolean
    years: number[]
    zoom_levels: {
      min: number
      max: number
      base: number
    }
  }
}

export async function GET() {
  try {
    // Create static metadata based on known file structure
    // Years: 2015-2023, Resolutions: 7-10
    const years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    const resolutions = [7, 8, 9, 10]
    
    const pmtilesFiles: PMTilesFile[] = []
    
    // Generate file entries for all year/resolution combinations
    for (const year of years) {
      for (const resolution of resolutions) {
        pmtilesFiles.push({
          year,
          resolution,
          timestamp: '20250705_181521', // Use a representative timestamp
          size: 0, // Size not available without GCS API
          url: `/api/pmtiles/h3_pfas_${year}_res${resolution}.pmtiles`,
          lastModified: new Date().toISOString(),
        })
      }
    }
    
    // Add BNBO PMTiles information
    const bnboFiles: BNBOFile[] = [{
      filename: 'bnbo_areas.pmtiles',
      size: 4685085, // 4.47 MB
      url: 'https://storage.googleapis.com/landbrugsdata-raw-data/pmtiles/bnbo_areas.pmtiles',
      lastModified: new Date().toISOString(),
      type: 'bnbo_areas'
    }]

    // Add Kommune PMTiles information
    const kommuneFiles: KommuneFile[] = []
    for (const year of years) {
      kommuneFiles.push({
        filename: `kommune_pfas_${year}.pmtiles`,
        year,
        timestamp: '20250705_204103', // Use the timestamp we found in GCS
        size: 0, // Size not available without GCS API
        url: `/api/pmtiles/kommune_pfas_${year}.pmtiles`,
        lastModified: new Date().toISOString(),
        type: 'kommune_pfas'
      })
    }
    
    const metadata: PMTilesMetadata = {
      files: pmtilesFiles,
      years,
      resolutions,
      totalFiles: pmtilesFiles.length,
      lastUpdated: new Date().toISOString(),
      bnbo: {
        files: bnboFiles,
        available: true,
        status_codes: ['Action Required', 'Completed', 'Unknown'],
        status_colors: {
          'Action Required': '#ff6b6b',
          'Completed': '#51cf66',
          'Unknown': '#868e96'
        }
      },
      kommune: {
        files: kommuneFiles,
        available: true,
        years,
        zoom_levels: {
          min: 3,
          max: 12,
          base: 6
        }
      }
    }
    
    return NextResponse.json(metadata, {
      headers: {
        'Cache-Control': 'public, max-age=300', // Cache for 5 minutes
        'Content-Type': 'application/json',
      },
    })
  } catch (error) {
    console.error('Error generating PMTiles metadata:', error)
    return NextResponse.json(
      { error: 'Failed to generate metadata' },
      { status: 500 }
    )
  }
} 
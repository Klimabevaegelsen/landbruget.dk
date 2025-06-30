import { createClient, type SupabaseClient } from '@supabase/supabase-js';
import { cache } from 'react';
import type { Database } from '@/types/database';

// React 19 cache function for automatic deduplication
const getCachedSupabaseClient = cache(() => {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseAnonKey) {
    throw new Error('Missing Supabase environment variables');
  }

  return createClient<Database>(supabaseUrl, supabaseAnonKey, {
    auth: {
      persistSession: false, // Optimize for server components
    },
    global: {
      headers: {
        'Cache-Control': 'max-age=300, stale-while-revalidate=60',
      },
    },
    db: {
      schema: 'public',
    },
    realtime: {
      enabled: false, // Disable realtime for better performance
    },
  });
});

// Export the cached client
export const supabase: SupabaseClient<Database> = getCachedSupabaseClient();

// Utility function for error handling
export function handleSupabaseError(error: any): never {
  console.error('Supabase error:', error);
  
  if (error?.code === 'PGRST116') {
    throw new Error('No data found for the specified criteria');
  }
  
  if (error?.code === 'PGRST301') {
    throw new Error('Database connection error');
  }
  
  if (error?.message) {
    throw new Error(`Database error: ${error.message}`);
  }
  
  throw new Error('An unexpected database error occurred');
}

// Utility function for geometry transformation
export function transformWKTToGeoJSON(wkt: string): GeoJSON.Geometry {
  try {
    // Simple WKT parser for POINT, POLYGON, and MULTIPOLYGON
    if (wkt.startsWith('POINT')) {
      const coords = wkt.match(/POINT\(([^)]+)\)/)?.[1];
      if (coords) {
        const [lon, lat] = coords.split(' ').map(Number);
        return {
          type: 'Point',
          coordinates: [lon, lat]
        };
      }
    }
    
    if (wkt.startsWith('POLYGON')) {
      const coords = wkt.match(/POLYGON\(\(([^)]+)\)\)/)?.[1];
      if (coords) {
        const points = coords.split(',').map(point => {
          const [lon, lat] = point.trim().split(' ').map(Number);
          return [lon, lat];
        });
        return {
          type: 'Polygon',
          coordinates: [points]
        };
      }
    }
    
    if (wkt.startsWith('MULTIPOLYGON')) {
      // Simplified MULTIPOLYGON parsing - would need more robust implementation for production
      const coords = wkt.match(/MULTIPOLYGON\(\(\(([^)]+)\)\)\)/)?.[1];
      if (coords) {
        const points = coords.split(',').map(point => {
          const [lon, lat] = point.trim().split(' ').map(Number);
          return [lon, lat];
        });
        return {
          type: 'MultiPolygon',
          coordinates: [[points]]
        };
      }
    }
    
    throw new Error(`Unsupported WKT format: ${wkt.substring(0, 20)}...`);
  } catch (error) {
    console.error('WKT transformation error:', error);
    throw new Error('Failed to transform geometry data');
  }
}

// Utility function for bbox queries
export function buildBboxQuery(bbox: {
  minLon: number;
  maxLon: number;
  minLat: number;
  maxLat: number;
}) {
  return `ST_Intersects(geometry, ST_MakeEnvelope(${bbox.minLon}, ${bbox.minLat}, ${bbox.maxLon}, ${bbox.maxLat}, 4326))`;
}

// Connection health check
export async function checkSupabaseConnection(): Promise<boolean> {
  try {
    const { data, error } = await supabase
      .from('h3_pfas_exposure')
      .select('id')
      .limit(1);
    
    return !error;
  } catch {
    return false;
  }
} 
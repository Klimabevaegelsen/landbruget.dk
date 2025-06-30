import { supabase, handleSupabaseError } from './supabase';
import type { 
  H3RawData, 
  H3DataQuality 
} from '@/types/h3-data';
import type { BNBORawData } from '@/types/bnbo-data';
import type { BBRRawData } from '@/types/bbr-data';
import { GCS_CONFIG } from './shared-constants';

// Data sync configuration
interface DataSyncConfig {
  gcs_bucket: string;
  supabase_url: string;
  supabase_key: string;
  sync_schedule: string; // cron expression
  batch_size: number;
  max_retries: number;
}

// Sync status tracking
interface SyncStatus {
  table: string;
  year?: number;
  status: 'pending' | 'running' | 'completed' | 'failed';
  records_processed: number;
  records_total: number;
  started_at?: string;
  completed_at?: string;
  error_message?: string;
}

// Sync result
interface SyncResult {
  success: boolean;
  records_synced: number;
  errors: string[];
  duration_ms: number;
}

/**
 * H3DataSyncer class - Handles data synchronization from GCS to Supabase
 * Coordinates with the H3 PFAS pipeline output and maintains data consistency
 */
export class H3DataSyncer {
  private config: DataSyncConfig;
  private syncStatus: Map<string, SyncStatus> = new Map();

  constructor(config?: Partial<DataSyncConfig>) {
    this.config = {
      gcs_bucket: GCS_CONFIG.BUCKET,
      supabase_url: process.env.NEXT_PUBLIC_SUPABASE_URL || '',
      supabase_key: process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '',
      sync_schedule: '0 2 * * *', // Daily at 2 AM
      batch_size: 1000,
      max_retries: 3,
      ...config
    };
  }

  /**
   * Sync H3 PFAS exposure data for a specific year
   */
  async syncH3Data(year: number): Promise<SyncResult> {
    const syncKey = `h3_${year}`;
    const startTime = Date.now();
    
    this.updateSyncStatus(syncKey, {
      table: 'h3_pfas_exposure',
      year,
      status: 'running',
      records_processed: 0,
      records_total: 0,
      started_at: new Date().toISOString()
    });

    try {
      // 1. Fetch data from GCS (simulated - in production would use actual GCS client)
      const gcsPath = `gs://${this.config.gcs_bucket}/${GCS_CONFIG.H3_DATA_PATH}/${year}/data.parquet`;
      console.log(`Fetching H3 data from: ${gcsPath}`);
      
      const h3RawData = await this.fetchH3DataFromGCS(gcsPath);
      
      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        records_total: h3RawData.length
      });

      // 2. Transform data for Supabase PostGIS
      const transformedData = this.transformH3DataForSupabase(h3RawData);

      // 3. Sync data in batches
      const syncResult = await this.syncDataInBatches(
        'h3_pfas_exposure',
        transformedData,
        ['h3_id', 'year'], // Conflict columns
        syncKey
      );

      // 4. Update sync status
      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        status: syncResult.success ? 'completed' : 'failed',
        records_processed: syncResult.records_synced,
        completed_at: new Date().toISOString(),
        error_message: syncResult.errors.join('; ')
      });

      const duration = Date.now() - startTime;
      console.log(`H3 data sync completed for year ${year}: ${syncResult.records_synced} records in ${duration}ms`);

      return {
        ...syncResult,
        duration_ms: duration
      };

    } catch (error) {
      console.error(`H3 data sync failed for year ${year}:`, error);
      
      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        status: 'failed',
        completed_at: new Date().toISOString(),
        error_message: error instanceof Error ? error.message : 'Unknown error'
      });

      return {
        success: false,
        records_synced: 0,
        errors: [error instanceof Error ? error.message : 'Unknown error'],
        duration_ms: Date.now() - startTime
      };
    }
  }

  /**
   * Sync BNBO status areas data
   */
  async syncBNBOData(): Promise<SyncResult> {
    const syncKey = 'bnbo_areas';
    const startTime = Date.now();
    
    this.updateSyncStatus(syncKey, {
      table: 'bnbo_status_areas',
      status: 'running',
      records_processed: 0,
      records_total: 0,
      started_at: new Date().toISOString()
    });

    try {
      const gcsPath = `gs://${this.config.gcs_bucket}/${GCS_CONFIG.BNBO_DATA_PATH}/data.parquet`;
      console.log(`Fetching BNBO data from: ${gcsPath}`);
      
      const bnboRawData = await this.fetchBNBODataFromGCS(gcsPath);
      
      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        records_total: bnboRawData.length
      });

      const transformedData = this.transformBNBODataForSupabase(bnboRawData);

      const syncResult = await this.syncDataInBatches(
        'bnbo_status_areas',
        transformedData,
        ['bnbo_id'], // Conflict columns
        syncKey
      );

      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        status: syncResult.success ? 'completed' : 'failed',
        records_processed: syncResult.records_synced,
        completed_at: new Date().toISOString(),
        error_message: syncResult.errors.join('; ')
      });

      return {
        ...syncResult,
        duration_ms: Date.now() - startTime
      };

    } catch (error) {
      console.error('BNBO data sync failed:', error);
      
      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        status: 'failed',
        completed_at: new Date().toISOString(),
        error_message: error instanceof Error ? error.message : 'Unknown error'
      });

      return {
        success: false,
        records_synced: 0,
        errors: [error instanceof Error ? error.message : 'Unknown error'],
        duration_ms: Date.now() - startTime
      };
    }
  }

  /**
   * Sync BBR buildings data
   */
  async syncBBRData(): Promise<SyncResult> {
    const syncKey = 'bbr_buildings';
    const startTime = Date.now();
    
    this.updateSyncStatus(syncKey, {
      table: 'bbr_buildings',
      status: 'running',
      records_processed: 0,
      records_total: 0,
      started_at: new Date().toISOString()
    });

    try {
      const gcsPath = `gs://${this.config.gcs_bucket}/${GCS_CONFIG.BBR_DATA_PATH}/data.parquet`;
      console.log(`Fetching BBR data from: ${gcsPath}`);
      
      const bbrRawData = await this.fetchBBRDataFromGCS(gcsPath);
      
      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        records_total: bbrRawData.length
      });

      const transformedData = this.transformBBRDataForSupabase(bbrRawData);

      const syncResult = await this.syncDataInBatches(
        'bbr_buildings',
        transformedData,
        ['bbr_id'], // Conflict columns
        syncKey
      );

      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        status: syncResult.success ? 'completed' : 'failed',
        records_processed: syncResult.records_synced,
        completed_at: new Date().toISOString(),
        error_message: syncResult.errors.join('; ')
      });

      return {
        ...syncResult,
        duration_ms: Date.now() - startTime
      };

    } catch (error) {
      console.error('BBR data sync failed:', error);
      
      this.updateSyncStatus(syncKey, {
        ...this.syncStatus.get(syncKey)!,
        status: 'failed',
        completed_at: new Date().toISOString(),
        error_message: error instanceof Error ? error.message : 'Unknown error'
      });

      return {
        success: false,
        records_synced: 0,
        errors: [error instanceof Error ? error.message : 'Unknown error'],
        duration_ms: Date.now() - startTime
      };
    }
  }

  /**
   * Sync all data types for all available years
   */
  async syncAllData(): Promise<{ h3: SyncResult[]; bnbo: SyncResult; bbr: SyncResult }> {
    console.log('Starting full data synchronization...');
    
    // Sync H3 data for all years
    const h3Results: SyncResult[] = [];
    const years = [2020, 2021, 2022, 2023, 2024, 2025];
    
    for (const year of years) {
      const result = await this.syncH3Data(year);
      h3Results.push(result);
      
      // Add delay between years to avoid overwhelming the database
      await new Promise(resolve => setTimeout(resolve, 1000));
    }

    // Sync BNBO and BBR data
    const bnboResult = await this.syncBNBOData();
    const bbrResult = await this.syncBBRData();

    console.log('Full data synchronization completed');
    
    return {
      h3: h3Results,
      bnbo: bnboResult,
      bbr: bbrResult
    };
  }

  /**
   * Get current sync status for all operations
   */
  getSyncStatus(): SyncStatus[] {
    return Array.from(this.syncStatus.values());
  }

  /**
   * Get sync status for specific operation
   */
  getSyncStatusFor(key: string): SyncStatus | null {
    return this.syncStatus.get(key) || null;
  }

  /**
   * Validate data integrity after sync
   */
  async validateDataIntegrity(year: number): Promise<H3DataQuality> {
    try {
      const { data, error } = await supabase
        .from('h3_pfas_exposure')
        .select('*')
        .eq('year', year);

      if (error) {
        handleSupabaseError(error);
      }

      if (!data || data.length === 0) {
        throw new Error(`No H3 data found for year ${year}`);
      }

      // Calculate quality metrics
      const quality: H3DataQuality = {
        totalRecords: data.length,
        recordsWithGeometry: data.filter(row => row.geometry).length,
        recordsWithPesticideData: data.filter(row => row.total_pesticide_load !== null && row.total_pesticide_load > 0).length,
        recordsWithPfasData: data.filter(row => row.total_pfas_grams !== null && row.total_pfas_grams > 0).length,
        yearRange: { min: year, max: year },
        spatialExtent: {
          minLon: 8.0, // Denmark approximate bounds
          maxLon: 15.0,
          minLat: 54.5,
          maxLat: 57.8
        },
        dataCompleteness: (data.filter(row => 
          row.total_pesticide_load !== null && 
          row.total_pfas_grams !== null && 
          row.geometry
        ).length / data.length) * 100,
        lastUpdated: new Date().toISOString()
      };

      console.log(`Data integrity validation for year ${year}:`, quality);
      return quality;

    } catch (error) {
      console.error(`Data integrity validation failed for year ${year}:`, error);
      throw error;
    }
  }

  /**
   * Simulate fetching H3 data from GCS
   * In production, this would use the actual Google Cloud Storage client
   */
  private async fetchH3DataFromGCS(gcsPath: string): Promise<H3RawData[]> {
    // DEBUG: Simulated data - remove when implementing actual GCS integration
    console.log(`DEBUG: Simulating fetch from ${gcsPath}`);
    
    // Return mock data for development
    return [
      {
        h3_id: 622203529111552000,
        year: 2023,
        total_pesticide_load: 12.5,
        total_pfas_grams: 0.8,
        pesticide_application_count: 3,
        field_count: 5,
        agricultural_area_ha: 15.2,
        avg_field_coverage: 0.85,
        geometry_wkt: 'POLYGON((9.5 56.2, 9.6 56.2, 9.6 56.3, 9.5 56.3, 9.5 56.2))',
        h3_centroid_lat: 56.25,
        h3_centroid_lon: 9.55,
        h3_resolution: 10
      }
    ];
  }

  /**
   * Simulate fetching BNBO data from GCS
   */
  private async fetchBNBODataFromGCS(gcsPath: string): Promise<BNBORawData[]> {
    console.log(`DEBUG: Simulating BNBO fetch from ${gcsPath}`);
    
    return [
      {
        bnbo_id: 'BNBO_001',
        status_code: 'protected',
        status_description: 'Fully Protected Area',
        area_ha: 125.5,
        geometry_wkt: 'POLYGON((9.4 56.1, 9.7 56.1, 9.7 56.4, 9.4 56.4, 9.4 56.1))',
        year: 2023
      }
    ];
  }

  /**
   * Simulate fetching BBR data from GCS
   */
  private async fetchBBRDataFromGCS(gcsPath: string): Promise<BBRRawData[]> {
    console.log(`DEBUG: Simulating BBR fetch from ${gcsPath}`);
    
    return [
      {
        bbr_id: 'BBR_001',
        building_code: '110',
        building_type: 'Residential',
        construction_year: 1995,
        floor_area: 150.0,
        geometry_wkt: 'POINT(9.55 56.25)',
        address: 'Test Address 1, 8000 Aarhus'
      }
    ];
  }

  /**
   * Transform H3 raw data for Supabase insertion
   */
  private transformH3DataForSupabase(rawData: H3RawData[]): any[] {
    return rawData.map(row => ({
      h3_id: row.h3_id,
      year: row.year,
      total_pesticide_load: row.total_pesticide_load,
      total_pfas_grams: row.total_pfas_grams,
      pesticide_application_count: row.pesticide_application_count,
      field_count: row.field_count,
      agricultural_area_ha: row.agricultural_area_ha,
      avg_field_coverage: row.avg_field_coverage,
      h3_resolution: row.h3_resolution || 10,
      // Transform geometry from WKT to PostGIS
      geometry: `ST_GeomFromText('${row.geometry_wkt}', 4326)`,
      h3_centroid: `ST_Point(${row.h3_centroid_lon}, ${row.h3_centroid_lat})`
    }));
  }

  /**
   * Transform BNBO raw data for Supabase insertion
   */
  private transformBNBODataForSupabase(rawData: BNBORawData[]): any[] {
    return rawData.map(row => ({
      bnbo_id: row.bnbo_id,
      status_code: row.status_code,
      status_description: row.status_description,
      area_ha: row.area_ha,
      year: row.year,
      geometry: `ST_GeomFromText('${row.geometry_wkt}', 4326)`
    }));
  }

  /**
   * Transform BBR raw data for Supabase insertion
   */
  private transformBBRDataForSupabase(rawData: BBRRawData[]): any[] {
    return rawData.map(row => ({
      bbr_id: row.bbr_id,
      building_code: row.building_code,
      building_type: row.building_type,
      construction_year: row.construction_year,
      floor_area: row.floor_area,
      address: row.address,
      geometry: `ST_GeomFromText('${row.geometry_wkt}', 4326)`
    }));
  }

  /**
   * Sync data in batches with proper conflict resolution
   */
  private async syncDataInBatches(
    tableName: string,
    data: any[],
    conflictColumns: string[],
    syncKey: string
  ): Promise<SyncResult> {
    const errors: string[] = [];
    let recordsSynced = 0;
    
    for (let i = 0; i < data.length; i += this.config.batch_size) {
      const batch = data.slice(i, i + this.config.batch_size);
      
      try {
        const { error } = await supabase
          .from(tableName)
          .upsert(batch, { 
            onConflict: conflictColumns.join(','),
            ignoreDuplicates: false 
          });

        if (error) {
          console.error(`Batch sync error for ${tableName}:`, error);
          errors.push(`Batch ${Math.floor(i / this.config.batch_size) + 1}: ${error.message}`);
        } else {
          recordsSynced += batch.length;
        }

        // Update progress
        this.updateSyncStatus(syncKey, {
          ...this.syncStatus.get(syncKey)!,
          records_processed: recordsSynced
        });

      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        console.error(`Batch sync exception for ${tableName}:`, error);
        errors.push(`Batch ${Math.floor(i / this.config.batch_size) + 1}: ${errorMessage}`);
      }
    }

    return {
      success: errors.length === 0,
      records_synced: recordsSynced,
      errors,
      duration_ms: 0 // Duration will be calculated by the calling method
    };
  }

  /**
   * Update sync status tracking
   */
  private updateSyncStatus(key: string, status: SyncStatus): void {
    this.syncStatus.set(key, status);
  }
} 
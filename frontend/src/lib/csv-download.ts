/**
 * Utility functions for downloading chart data as CSV files
 */

import {
  ChartData,
  MapChart,
  GeoJSONLayer,
  Timeline,
  BaseDataGrid,
} from '@/services/supabase/types';

/**
 * Converts chart data to CSV format
 */
export function chartDataToCSV(
  chartData: ChartData,
  chartTitle?: string
): string {
  // chartTitle parameter is available for future use
  void chartTitle;
  const { xAxis, yAxis, series } = chartData;

  if (!series || series.length === 0) {
    return '';
  }

  // Create CSV headers
  const headers: string[] = [];

  // Add category column header (either from xAxis or yAxis for horizontal charts)
  if (xAxis?.values) {
    headers.push('Kategori');
  } else if (yAxis?.values) {
    headers.push('Kategori');
  }

  // Add series names as column headers
  series.forEach((s) => {
    headers.push(s.name);
  });

  // Create CSV rows
  const rows: string[] = [];
  rows.push(headers.join(','));

  // Get categories (x-axis values or y-axis values for horizontal charts)
  const categories = xAxis?.values || yAxis?.values || [];

  // Add data rows
  categories.forEach((category, index) => {
    const row: string[] = [];

    // Add category value (escape commas and quotes)
    row.push(escapeCSVField(String(category)));

    // Add series values for this category
    series.forEach((s) => {
      const value = s.data[index];
      row.push(String(value ?? ''));
    });

    rows.push(row.join(','));
  });

  return rows.join('\n');
}

/**
 * Escapes CSV field values (handles commas, quotes, newlines)
 */
function escapeCSVField(field: string): string {
  if (field.includes(',') || field.includes('"') || field.includes('\n')) {
    return `"${field.replace(/"/g, '""')}"`;
  }
  return field;
}

/**
 * Downloads CSV data as a file
 */
export function downloadCSV(csvData: string, filename: string): void {
  if (!csvData.trim()) {
    console.warn('No data to download');
    return;
  }

  const blob = new Blob([csvData], { type: 'text/csv;charset=utf-8;' });
  const link = document.createElement('a');

  if (link.download !== undefined) {
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }
}

/**
 * Generates a filename for the CSV download based on chart title and current date
 */
export function generateCSVFilename(
  chartTitle?: string,
  chartKey?: string
): string {
  const date = new Date().toISOString().split('T')[0]; // YYYY-MM-DD format
  const baseTitle = chartTitle || chartKey || 'chart-data';

  // Clean the title for use in filename
  const cleanTitle = baseTitle
    .toLowerCase()
    .replace(/[^a-z0-9æøå\s-]/g, '') // Keep Danish characters
    .replace(/\s+/g, '-')
    .replace(/-+/g, '-')
    .trim();

  return `${cleanTitle}-${date}.csv`;
}

/**
 * Converts map chart data (GeoJSON) to CSV format
 */
export function mapDataToCSV(mapChart: MapChart): string {
  const { layers } = mapChart.data;

  if (!layers || layers.length === 0) {
    return '';
  }

  // Collect all properties from all features in all layers
  const allRows: Array<Record<string, unknown>> = [];
  const allHeaders = new Set<string>();

  // Fields to exclude from CSV export (technical/internal fields)
  const excludedFields = new Set([
    'id',
    'uuid',
    'geom',
    'geometry',
    'wkt',
    'created_at',
    'updated_at',
    'version',
    'source',
    'processed_at',
    'import_id',
    'raw_data',
    'metadata',
    'internal_id',
    'fid',
    'ogc_fid',
    'gml_id',
    'objectid',
  ]);

  layers.forEach((layer: GeoJSONLayer) => {
    if (layer.data && layer.data.features) {
      layer.data.features.forEach((feature) => {
        if (feature.properties && typeof feature.properties === 'object') {
          const row: Record<string, unknown> = {};

          // Add layer name as a column
          row['Layer'] = layer.name;
          allHeaders.add('Layer');

          // Add all properties, excluding technical fields
          Object.keys(feature.properties as Record<string, unknown>).forEach(
            (key) => {
              if (!excludedFields.has(key.toLowerCase())) {
                const value = (feature.properties as Record<string, unknown>)[
                  key
                ];
                // Skip null, undefined, or empty string values
                if (value !== null && value !== undefined && value !== '') {
                  row[key] = value;
                  allHeaders.add(key);
                }
              }
            }
          );

          allRows.push(row);
        }
      });
    }
  });

  if (allRows.length === 0) {
    return '';
  }

  // Create CSV headers - put Layer first, then sort the rest
  const headers = [
    'Layer',
    ...Array.from(allHeaders)
      .filter((h) => h !== 'Layer')
      .sort(),
  ];

  // Create CSV rows
  const csvRows: string[] = [];
  csvRows.push(headers.map((h) => escapeCSVField(h)).join(','));

  allRows.forEach((row) => {
    const csvRow = headers.map((header) => {
      const value = row[header];
      return escapeCSVField(String(value ?? ''));
    });
    csvRows.push(csvRow.join(','));
  });

  return csvRows.join('\n');
}

/**
 * Converts timeline data to CSV format
 */
export function timelineDataToCSV(timeline: Timeline): string {
  const { events } = timeline;

  if (!events || events.length === 0) {
    return '';
  }

  // Define headers
  const headers = ['Dato', 'Begivenhedstype', 'Beskrivelse'];

  // Create CSV rows
  const csvRows: string[] = [];
  csvRows.push(headers.join(','));

  events.forEach((event) => {
    if (event && typeof event === 'object') {
      const row = [
        escapeCSVField(String(event.date || '')),
        escapeCSVField(String(event.event_type || '')),
        escapeCSVField(String(event.description || '')),
      ];
      csvRows.push(row.join(','));
    }
  });

  return csvRows.join('\n');
}

/**
 * Converts table data to CSV format
 */
export function tableDataToCSV(table: BaseDataGrid): string {
  const { columns, rows } = table;

  if (!columns || columns.length === 0 || !rows || rows.length === 0) {
    return '';
  }

  // Create CSV headers using column labels
  const headers = columns.map((col) => escapeCSVField(col.label));

  // Create CSV rows
  const csvRows: string[] = [];
  csvRows.push(headers.join(','));

  rows.forEach((row) => {
    const csvRow = columns.map((col) => {
      const value = row[col.key];
      return escapeCSVField(String(value ?? ''));
    });
    csvRows.push(csvRow.join(','));
  });

  return csvRows.join('\n');
}

/**
 * Main function to handle CSV download for chart data
 */
export function downloadChartAsCSV(
  chartData: ChartData,
  chartTitle?: string,
  chartKey?: string
): void {
  const csvData = chartDataToCSV(chartData, chartTitle);
  const filename = generateCSVFilename(chartTitle, chartKey);
  downloadCSV(csvData, filename);
}

/**
 * Main function to handle CSV download for map chart data
 */
export function downloadMapAsCSV(
  mapChart: MapChart,
  chartTitle?: string,
  chartKey?: string
): void {
  const csvData = mapDataToCSV(mapChart);
  const filename = generateCSVFilename(chartTitle, chartKey);
  downloadCSV(csvData, filename);
}

/**
 * Main function to handle CSV download for timeline data
 */
export function downloadTimelineAsCSV(
  timeline: Timeline,
  chartTitle?: string,
  chartKey?: string
): void {
  const csvData = timelineDataToCSV(timeline);
  const filename = generateCSVFilename(chartTitle, chartKey);
  downloadCSV(csvData, filename);
}

/**
 * Main function to handle CSV download for table data
 */
export function downloadTableAsCSV(
  table: BaseDataGrid,
  chartTitle?: string,
  chartKey?: string
): void {
  const csvData = tableDataToCSV(table);
  const filename = generateCSVFilename(chartTitle, chartKey);
  downloadCSV(csvData, filename);
}

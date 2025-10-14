import { FieldAnalysisData } from '@/components/field-analysis/types';

/**
 * Convert field analysis data to CSV format
 */
export function convertToCSV(fields: FieldAnalysisData[]): string {
  if (fields.length === 0) {
    return '';
  }

  // Define CSV headers in Danish
  const headers = [
    'Mark UUID',
    'Kommune',
    'CVR Nummer',
    'Areal (ha)',
    'Afgrøde',
    'Økologisk',
    'Total Pesticidbelastning',
    'Total Pesticidanvendelser',
    'PFAS Aktivstof (kg)',
    'PFAS Belastning',
    'PFAS Anvendelser',
    'Total Dosage (kg)',
    'Total Dosage (liter)',
    'Total Dosage (gram)',
    'Total Dosage (ml)',
    'Total Dosage (tabletter)',
    'Diquat Belastning',
    'Diquat Anvendelser',
    'Glyphosate Aktivstof (kg)',
    'Glyphosate Belastning',
    'Glyphosate Anvendelser',
    'BNBO Areal (ha)',
    'Vådområde Areal (ha)',
    'BNBO Handlingskrævende (ha)',
    'BNBO Gennemført (ha)',
    'BNBO Status Kategorier',
    'Boligbygninger Nærhed',
    'Uddannelsesinstitutioner Nærhed',
    'Vandafstand Nærhed',
    'Unikke Pesticidprodukter',
    'Delvis Dækning',
    'Pesticide Detaljer (kg)',
    'Pesticide Detaljer (liter)',
    'Pesticide Detaljer (gram)',
    'Pesticide Detaljer (ml)',
    'Pesticide Detaljer (tons)',
    'Koordinater (lat)',
    'Koordinater (lng)',
  ];

  // Convert data rows
  const rows = fields.map((field) => [
    field.field_uuid || '',
    field.kommune || '',
    field.cvr_number || '',
    field.area_hectares?.toString() || '',
    field.crop_name || '',
    field.is_organic ? 'Ja' : 'Nej',
    field.total_pesticide_belastning?.toString() || '',
    field.total_pesticide_applications?.toString() || '',
    field.total_pfas_active_ingredient_kg?.toString() || '',
    field.total_pfas_belastning?.toString() || '',
    field.pfas_applications?.toString() || '',
    field.total_dosage_kg?.toString() || '',
    field.total_dosage_liters?.toString() || '',
    field.total_dosage_grams?.toString() || '',
    field.total_dosage_ml?.toString() || '',
    field.total_dosage_tablets?.toString() || '',
    field.total_diquat_belastning?.toString() || '',
    field.diquat_applications?.toString() || '',
    field.total_glyphosate_active_ingredient_kg?.toString() || '',
    field.total_glyphosate_belastning?.toString() || '',
    field.glyphosate_applications?.toString() || '',
    (field.bnbo_area_hectares ?? 0).toString(),
    (field.wetland_area_hectares ?? 0).toString(),
    field.bnbo_action_required_hectares?.toString() || '',
    field.bnbo_completed_hectares?.toString() || '',
    field.bnbo_status_categories || '',
    field.residential_buildings_proximity || '',
    field.educational_facilities_proximity || '',
    field.water_distance_proximity || '',
    field.unique_pesticide_products?.toString() || '',
    field.is_partial_coverage ? 'Ja' : 'Nej',
    field.pesticides_kg_detail || '',
    field.pesticides_liters_detail || '',
    field.pesticides_grams_detail || '',
    field.pesticides_ml_detail || '',
    field.pesticides_tablets_detail || '',
    field.click_coordinates?.lat?.toString() || '',
    field.click_coordinates?.lng?.toString() || '',
  ]);

  // Escape CSV values that contain commas, quotes, or newlines
  const escapeCSVValue = (value: string): string => {
    if (value.includes(',') || value.includes('"') || value.includes('\n')) {
      return `"${value.replace(/"/g, '""')}"`;
    }
    return value;
  };

  // Build CSV string
  const csvContent = [
    headers.map(escapeCSVValue).join(','),
    ...rows.map((row) => row.map(escapeCSVValue).join(',')),
  ].join('\n');

  return csvContent;
}

/**
 * Download CSV file
 */
export function downloadCSV(csvContent: string, filename: string): void {
  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
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
 * Generate filename for CSV download
 */
export function generateCSVFilename(
  year: number,
  bounds?: { north: number; south: number; east: number; west: number }
): string {
  const timestamp = new Date()
    .toISOString()
    .slice(0, 19)
    .replace(/[:\-]/g, '')
    .replace('T', '_');

  if (bounds) {
    const boundsStr = `${bounds.north.toFixed(3)}_${bounds.south.toFixed(3)}_${bounds.east.toFixed(3)}_${bounds.west.toFixed(3)}`;
    return `markanalyse_${year}_${boundsStr}_${timestamp}.csv`;
  }

  return `markanalyse_${year}_${timestamp}.csv`;
}

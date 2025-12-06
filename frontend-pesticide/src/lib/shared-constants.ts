// Shared constants for H3 PFAS Visualization - Used by all developers
export const YEARS = [2020, 2021, 2022, 2023, 2024, 2025];

export const H3_RESOLUTION = 10;

export const DEFAULT_VIEWPORT = {
  latitude: 56.26392,  // Denmark center
  longitude: 9.501785,
  zoom: 7
};

export const API_ENDPOINTS = {
  H3_DATA: '/api/h3-data',
  BNBO_DATA: '/api/bnbo-data',
  BBR_DATA: '/api/bbr-data'
};

// Data processing constants
export const GCS_CONFIG = {
  BUCKET: 'landbrugsdata-raw-data',
  H3_DATA_PATH: 'gold/h3_pfas_exposure',
  BNBO_DATA_PATH: 'gold/bnbo_status_areas',
  BBR_DATA_PATH: 'gold/bbr_buildings'
};

// Performance constants
export const CACHE_SETTINGS = {
  H3_DATA_TTL: 300, // 5 minutes
  BNBO_DATA_TTL: 3600, // 1 hour
  BBR_DATA_TTL: 3600, // 1 hour
  MAX_CACHE_SIZE: 100 // Maximum cached datasets
};

// Visualization constants
export const VISUALIZATION_LIMITS = {
  MAX_H3_HEXAGONS: 10000,
  MAX_BNBO_POLYGONS: 5000,
  MAX_BBR_POINTS: 50000,
  VIEWPORT_BUFFER: 0.1 // Degrees
}; 
// Color schemes and visual design for H3 PFAS visualization

export const COLOR_SCHEMES = {
  pesticide_load: {
    name: 'Pesticide Load',
    colors: ['#f7fbff', '#deebf7', '#c6dbef', '#9ecae1', '#6baed6', '#4292c6', '#2171b5', '#084594'],
    domain: [0, 1000], // kg/ha equivalent (standardized from pipeline)
    unit: 'kg/ha',
    type: 'sequential'
  },
  pfas_grams: {
    name: 'PFAS Mass',
    colors: ['#fff5f0', '#fee0d2', '#fcbba1', '#fc9272', '#fb6a4a', '#ef3b2c', '#cb181d', '#99000d'],
    domain: [0, 50], // grams (actual PFAS active ingredient mass)
    unit: 'g',
    type: 'sequential'
  }
};

export const BNBO_COLORS = {
  'protected': '#2d8659',      // Dark green - fully protected
  'buffer': '#52c878',         // Light green - buffer zone
  'agricultural': '#ffd23f',   // Yellow - agricultural buffer
  'transition': '#ff8c42',     // Orange - transition zone
  'unprotected': '#e5e5e5'     // Gray - no protection
};

export const BBR_COLORS = {
  'Residential': '#4a90e2',    // Blue
  'Agricultural': '#7ed321',   // Green
  'Industrial': '#f5a623',     // Orange
  'Commercial': '#d0021b',     // Red
  'Public': '#9013fe',         // Purple
  'Other': '#50e3c2'          // Teal
};

// Generate color scale configuration for Kepler.gl
export function generateColorScale(field: string) {
  const config = COLOR_SCHEMES[field as keyof typeof COLOR_SCHEMES];
  
  if (!config) {
    console.warn(`Color scheme not found for field: ${field}`);
    return {
      type: 'quantile',
      field: field,
      domain: [0, 100],
      range: ['#f7fbff', '#084594'] // Default blue scale
    };
  }
  
  return {
    type: 'quantile',
    field: field,
    domain: config.domain,
    range: config.colors,
    scale: config.type
  };
}

// Get color for BNBO status
export function getBNBOStatusColor(statusCode: string): string {
  return BNBO_COLORS[statusCode as keyof typeof BNBO_COLORS] || BNBO_COLORS.unprotected;
}

// Get color for BBR building type
export function getBBRTypeColor(buildingType: string): string {
  return BBR_COLORS[buildingType as keyof typeof BBR_COLORS] || BBR_COLORS.Other;
}

// Color utility functions
export function hexToRgb(hex: string): [number, number, number] {
  const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
  return result ? [
    parseInt(result[1], 16),
    parseInt(result[2], 16),
    parseInt(result[3], 16)
  ] : [0, 0, 0];
}

export function rgbToHex(r: number, g: number, b: number): string {
  return "#" + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
}

// Generate interpolated colors for smooth gradients
export function interpolateColor(color1: string, color2: string, factor: number): string {
  const rgb1 = hexToRgb(color1);
  const rgb2 = hexToRgb(color2);
  
  const r = Math.round(rgb1[0] + factor * (rgb2[0] - rgb1[0]));
  const g = Math.round(rgb1[1] + factor * (rgb2[1] - rgb1[1]));
  const b = Math.round(rgb1[2] + factor * (rgb2[2] - rgb1[2]));
  
  return rgbToHex(r, g, b);
}

// Generate color palette for data visualization
export function generateColorPalette(baseColor: string, steps: number): string[] {
  const palette: string[] = [];
  const rgb = hexToRgb(baseColor);
  
  for (let i = 0; i < steps; i++) {
    const factor = i / (steps - 1);
    const lightness = 0.9 - (factor * 0.7); // From light to dark
    
    const r = Math.round(rgb[0] * lightness);
    const g = Math.round(rgb[1] * lightness);
    const b = Math.round(rgb[2] * lightness);
    
    palette.push(rgbToHex(r, g, b));
  }
  
  return palette;
}

// Color accessibility utilities
export function getContrastRatio(color1: string, color2: string): number {
  const rgb1 = hexToRgb(color1);
  const rgb2 = hexToRgb(color2);
  
  const luminance1 = getLuminance(rgb1);
  const luminance2 = getLuminance(rgb2);
  
  const lighter = Math.max(luminance1, luminance2);
  const darker = Math.min(luminance1, luminance2);
  
  return (lighter + 0.05) / (darker + 0.05);
}

function getLuminance(rgb: [number, number, number]): number {
  const [r, g, b] = rgb.map(c => {
    c = c / 255;
    return c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4);
  });
  
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

// Ensure text is readable on colored backgrounds
export function getReadableTextColor(backgroundColor: string): string {
  const contrastWithWhite = getContrastRatio(backgroundColor, '#ffffff');
  const contrastWithBlack = getContrastRatio(backgroundColor, '#000000');
  
  return contrastWithWhite > contrastWithBlack ? '#ffffff' : '#000000';
}

// Color scheme validation
export function validateColorScheme(colors: string[]): boolean {
  return colors.every(color => /^#[0-9A-F]{6}$/i.test(color));
}

// Export color constants for easy access
export const COLORS = {
  PRIMARY: '#4a90e2',
  SECONDARY: '#7ed321',
  SUCCESS: '#52c878',
  WARNING: '#ffd23f',
  ERROR: '#d0021b',
  INFO: '#50e3c2',
  LIGHT: '#f8f9fa',
  DARK: '#343a40',
  WHITE: '#ffffff',
  BLACK: '#000000'
};

// Tailwind CSS class mappings for colors
export const COLOR_CLASSES = {
  BNBO: {
    protected: 'bg-green-700 text-white',
    buffer: 'bg-green-500 text-white',
    agricultural: 'bg-yellow-400 text-black',
    transition: 'bg-orange-500 text-white',
    unprotected: 'bg-gray-300 text-black'
  },
  BBR: {
    Residential: 'bg-blue-500 text-white',
    Agricultural: 'bg-green-500 text-white',
    Industrial: 'bg-orange-500 text-white',
    Commercial: 'bg-red-600 text-white',
    Public: 'bg-purple-600 text-white',
    Other: 'bg-teal-500 text-white'
  }
}; 
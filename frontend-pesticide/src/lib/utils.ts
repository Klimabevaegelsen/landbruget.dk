import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatNumber(value: number, decimals: number = 2): string {
  if (value === 0) return '0';
  if (value < 0.01) return '<0.01';
  return value.toLocaleString('en-US', {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals,
  });
}

export function formatPercentage(value: number, decimals: number = 1): string {
  return `${(value * 100).toFixed(decimals)}%`;
}

export const YEARS = [2020, 2021, 2022, 2023, 2024, 2025];
export const H3_RESOLUTION = 10;
export const DEFAULT_VIEWPORT = {
  latitude: 56.26392,
  longitude: 9.501785,
  zoom: 7
};

export const API_ENDPOINTS = {
  H3_DATA: '/api/h3-data',
  BNBO_DATA: '/api/bnbo-data',
  BBR_DATA: '/api/bbr-data'
}; 
'use client';

import { Button } from '@/components/ui/button';
import { Download } from 'lucide-react';
import { MapChart } from '@/services/data/types';
import { downloadMapAsCSV } from '@/lib/csv-download';

interface MapCSVDownloadButtonProps {
  mapChart: MapChart;
  chartTitle?: string;
  chartKey?: string;
  className?: string;
  variant?: 'default' | 'outline' | 'secondary' | 'ghost';
  size?: 'default' | 'sm' | 'lg' | 'icon';
  disabled?: boolean;
}

export function MapCSVDownloadButton({
  mapChart,
  chartTitle,
  chartKey,
  className,
  variant = 'outline',
  size = 'sm',
  disabled = false,
}: MapCSVDownloadButtonProps) {
  const handleDownload = () => {
    try {
      downloadMapAsCSV(mapChart, chartTitle, chartKey);
    } catch (error) {
      console.error('Failed to download CSV:', error);
      // You could add a toast notification here if you have a toast system
    }
  };

  // Check if there's data to download
  const hasData =
    mapChart?.data?.layers?.length > 0 &&
    mapChart.data.layers.some(
      (layer) => layer.data?.features && layer.data.features.length > 0
    );

  return (
    <Button
      onClick={handleDownload}
      disabled={disabled || !hasData}
      variant={variant}
      size={size}
      className={`touch-manipulation ${className}`}
      title="Download kortdata som CSV"
    >
      <Download className="h-4 w-4" />
      <span className="hidden sm:inline">CSV</span>
      <span className="sm:hidden">Hent</span>
    </Button>
  );
}

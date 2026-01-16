'use client';

import { Button } from '@/components/ui/button';
import { Download } from 'lucide-react';
import { ChartData } from '@/services/supabase/types';
import { downloadChartAsCSV } from '@/lib/csv-download';

interface CSVDownloadButtonProps {
  chartData: ChartData;
  chartTitle?: string;
  chartKey?: string;
  className?: string;
  variant?: 'default' | 'outline' | 'secondary' | 'ghost';
  size?: 'default' | 'sm' | 'lg' | 'icon';
  disabled?: boolean;
}

export function CSVDownloadButton({
  chartData,
  chartTitle,
  chartKey,
  className,
  variant = 'outline',
  size = 'sm',
  disabled = false,
}: CSVDownloadButtonProps) {
  const handleDownload = () => {
    try {
      downloadChartAsCSV(chartData, chartTitle, chartKey);
    } catch (error) {
      console.error('Failed to download CSV:', error);
      // You could add a toast notification here if you have a toast system
    }
  };

  // Check if there's data to download
  const hasData =
    chartData?.series?.length > 0 &&
    chartData.series.some((series) => series.data.length > 0);

  return (
    <Button
      onClick={handleDownload}
      disabled={disabled || !hasData}
      variant={variant}
      size={size}
      className={`touch-manipulation ${className}`}
      title="Download som CSV"
    >
      <Download className="h-4 w-4" />
      <span className="hidden sm:inline">CSV</span>
      <span className="sm:hidden">Hent</span>
    </Button>
  );
}

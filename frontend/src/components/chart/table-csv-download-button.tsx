'use client';

import { Button } from '@/components/ui/button';
import { Download } from 'lucide-react';
import { BaseDataGrid } from '@/services/supabase/types';
import { downloadTableAsCSV } from '@/lib/csv-download';

interface TableCSVDownloadButtonProps {
  table: BaseDataGrid;
  chartTitle?: string;
  chartKey?: string;
  className?: string;
  variant?: 'default' | 'outline' | 'secondary' | 'ghost';
  size?: 'default' | 'sm' | 'lg' | 'icon';
  disabled?: boolean;
}

export function TableCSVDownloadButton({
  table,
  chartTitle,
  chartKey,
  className,
  variant = 'outline',
  size = 'sm',
  disabled = false,
}: TableCSVDownloadButtonProps) {
  const handleDownload = () => {
    try {
      downloadTableAsCSV(table, chartTitle, chartKey);
    } catch (error) {
      console.error('Failed to download CSV:', error);
      // You could add a toast notification here if you have a toast system
    }
  };

  // Check if there's data to download
  const hasData = table?.rows?.length > 0 && table?.columns?.length > 0;

  return (
    <Button
      onClick={handleDownload}
      disabled={disabled || !hasData}
      variant={variant}
      size={size}
      className={className}
      title="Download tabel som CSV"
    >
      <Download className="h-4 w-4" />
      CSV
    </Button>
  );
}

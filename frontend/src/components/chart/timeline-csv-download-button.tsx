'use client';

import { Button } from '@/components/ui/button';
import { Download } from 'lucide-react';
import { Timeline } from '@/services/supabase/types';
import { downloadTimelineAsCSV } from '@/lib/csv-download';

interface TimelineCSVDownloadButtonProps {
  timeline: Timeline;
  chartTitle?: string;
  chartKey?: string;
  className?: string;
  variant?: 'default' | 'outline' | 'secondary' | 'ghost';
  size?: 'default' | 'sm' | 'lg' | 'icon';
  disabled?: boolean;
}

export function TimelineCSVDownloadButton({
  timeline,
  chartTitle,
  chartKey,
  className,
  variant = 'outline',
  size = 'sm',
  disabled = false,
}: TimelineCSVDownloadButtonProps) {
  const handleDownload = () => {
    try {
      downloadTimelineAsCSV(timeline, chartTitle, chartKey);
    } catch (error) {
      console.error('Failed to download CSV:', error);
      // You could add a toast notification here if you have a toast system
    }
  };

  // Check if there's data to download
  const hasData = timeline?.data?.events?.length > 0;

  return (
    <Button
      onClick={handleDownload}
      disabled={disabled || !hasData}
      variant={variant}
      size={size}
      className={className}
      title="Download tidslinje som CSV"
    >
      <Download className="h-4 w-4" />
      CSV
    </Button>
  );
}

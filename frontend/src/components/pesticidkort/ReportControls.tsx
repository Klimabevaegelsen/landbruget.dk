'use client';

import { ModeToggle } from '@/components/pesticidkort/ModeToggle';
import { YearTimeline } from '@/components/pesticidkort/YearTimeline';

interface ReportControlsProps {
  mode: 'citizen' | 'expert';
  onModeChange: (m: 'citizen' | 'expert') => void;
  year: number;
  onYearChange: (year: number) => void;
}

export function ReportControls({
  mode,
  onModeChange,
  year,
  onYearChange,
}: ReportControlsProps) {
  return (
    <div className="bg-background/95 border-border border-t px-5 py-3 backdrop-blur-sm">
      <ModeToggle mode={mode} onChange={onModeChange} />
      <YearTimeline year={year} onChange={onYearChange} compact />
    </div>
  );
}

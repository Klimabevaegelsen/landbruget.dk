'use client';

import { useState, useCallback } from 'react';
import dynamic from 'next/dynamic';
import { X } from 'lucide-react';
import { StoryChapter } from '@/components/pesticidkort/StoryChapter';
import {
  STORY_CHAPTERS,
  interpolateChapter,
} from '@/components/pesticidkort/story-chapters';
import type { PesticideReport } from '@/components/pesticidkort/types';

const StoryMap = dynamic(
  () =>
    import('@/components/pesticidkort/StoryMapInner').then(
      (m) => m.StoryMapInner
    ),
  { ssr: false }
);

interface StoryModeProps {
  report: PesticideReport;
  onClose: () => void;
}

export function StoryMode({ report, onClose }: StoryModeProps) {
  const [activeChapter, setActiveChapter] = useState(0);

  const data: Record<string, string | number> = {
    year: report.year,
    fields_count: report.fields_count,
    pfas_count: report.pfas_fields_count,
    distance: Math.round(report.nearest_field_m),
    grade: report.grade?.grade ?? 'UNKNOWN',
    total_burden: report.fields
      .reduce((sum, f) => sum + f.total_pesticide_belastning, 0)
      .toFixed(1),
  };

  const handleChapterEnter = useCallback((index: number) => {
    setActiveChapter(index);
  }, []);

  return (
    <div
      data-testid="story-mode"
      className="bg-background fixed inset-0 z-50 flex"
    >
      <button
        onClick={onClose}
        data-testid="story-close-button"
        className="bg-background/80 text-foreground absolute top-4 right-4 z-50 flex h-10 w-10 items-center justify-center rounded-full backdrop-blur-sm"
      >
        <X className="h-5 w-5" />
      </button>

      <div className="w-full overflow-y-auto md:w-2/5">
        {STORY_CHAPTERS.map((ch, i) => (
          <StoryChapter
            key={ch.id}
            index={i}
            title={ch.title}
            body={interpolateChapter(ch.body, data)}
            chapter={ch}
            data={data}
            onEnterView={() => handleChapterEnter(i)}
          />
        ))}
      </div>

      <div className="sticky top-0 hidden h-screen md:block md:w-3/5">
        <StoryMap
          lat={report.lat}
          lng={report.lng}
          radiusM={report.radius_m}
          year={report.year}
          chapter={STORY_CHAPTERS[activeChapter]}
        />
      </div>
    </div>
  );
}

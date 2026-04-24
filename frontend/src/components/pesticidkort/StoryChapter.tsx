'use client';

import { useRef } from 'react';
import { motion, useInView } from 'motion/react';
import { cn } from '@/lib/utils';
import type { StoryChapter as ChapterType } from '@/components/pesticidkort/story-chapters';

interface StoryChapterProps {
  index: number;
  title: string;
  body: string;
  chapter: ChapterType;
  data: Record<string, string | number>;
  onEnterView: () => void;
}

export function StoryChapter({
  index,
  title,
  body,
  chapter,
  data,
  onEnterView,
}: StoryChapterProps) {
  const ref = useRef<HTMLDivElement>(null);
  const isInView = useInView(ref, { amount: 0.5, once: false });

  if (isInView) {
    onEnterView();
  }

  const statValue = chapter.statKey ? data[chapter.statKey] : null;

  return (
    <div
      ref={ref}
      data-testid={`story-chapter-${index}`}
      className="flex min-h-[70vh] items-center md:min-h-screen"
    >
      <motion.div
        initial={{ opacity: 0, y: 30 }}
        animate={isInView ? { opacity: 1, y: 0 } : { opacity: 0.2, y: 0 }}
        transition={{ duration: 0.5, ease: [0.25, 0.1, 0.25, 1] }}
        className={cn(
          'max-w-sm px-6 py-16 md:px-10',
          chapter.visual === 'warning-accent' &&
            'border-l-[3px] border-l-warning bg-warning/[0.04] rounded-r-lg'
        )}
      >
        <p className="text-muted-foreground mb-2 text-xs font-medium tracking-widest uppercase">
          {index + 1} / 5
        </p>

        {chapter.visual === 'stat-callout' && statValue != null && (
          <p className="font-display text-primary mb-3 text-4xl font-bold tabular-nums md:text-5xl">
            {statValue}
            {chapter.statSuffix && (
              <span className="text-muted-foreground ml-1 text-lg font-normal">
                {chapter.statSuffix}
              </span>
            )}
          </p>
        )}

        {chapter.visual === 'grade-display' && statValue != null && (
          <p className="font-display text-primary mb-3 text-6xl font-bold md:text-7xl">
            {statValue}
          </p>
        )}

        <h2 className="text-foreground text-2xl leading-tight font-bold tracking-tight md:text-3xl">
          {title}
        </h2>
        <p className="text-muted-foreground mt-4 text-base leading-relaxed">
          {body}
        </p>
      </motion.div>
    </div>
  );
}
